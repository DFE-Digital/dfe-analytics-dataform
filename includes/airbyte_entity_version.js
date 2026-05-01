/* Generates {entity}_version_{source}{suffix} tables from Airbyte Change Data Capture (CDC) data. 

  Source table format (from Airbyte CDC, partitioned by _airbyte_extracted_at):
   - One row per change event (insert/update/delete) captured by Change Data Capture
   - Each row contains the full entity state at the time of the change
   - Airbyte sync mode is incremental + append, so it only adds new entries when there are changes
   - In full syncs, the same data re-appended with a new _airbyte_extracted_at and _ab_cdc_updated_at
   - Key Airbyte metadata columns:
       _airbyte_raw_id        - Unique ID assigned by Airbyte to each raw record
       _airbyte_extracted_at  - Timestamp when Airbyte extracted the record (PARTITION column)
       _airbyte_meta          - Airbyte internal metadata (JSON)
       _airbyte_generation_id - Airbyte sync generation identifier
       _ab_cdc_updated_at     - CDC timestamp of the change event (string, e.g. '2026-02-11T10:30:45.123456Z')
       _ab_cdc_deleted_at     - Non-null only for deletion events
       _ab_cdc_lsn            - CDC log sequence number
   - other columns vary per entity (defined in dataSchema keys)
   
*/

const data_functions = require("./data_functions");
const parameterFunctions = require("./parameter_functions");

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.tableSuffix || '_airbyte';

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';

        const fieldAssertionDependencies = params.airbyteEnableAssertions ?
            params.dataSchema.map(schema => schema.entityTableName + "_airbyte_fields_not_in_schema_" + params.eventSourceName) : [];

        return publish(tableName, {
                type: "incremental",
                protected: false,
                dependencies: fieldAssertionDependencies,
                uniqueKey: [primaryKey, "valid_from"],
                description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${entitySchema.description || ''}`,
                columns: Object.assign({
                        [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity.`,
                        valid_from: "Timestamp from which this version was valid (updated_at from the source).",
                        valid_to: "Timestamp until which this version was valid. NULL = current.",
                        is_current: "TRUE if this is the current version.",
                        is_deleted: "TRUE if this entity is deleted.",
                        version_number: "Sequential version number (1 = oldest).",
                        created_at: "Timestamp this entity was first saved in the database.",
                        updated_at: "Timestamp this entity was last updated in the database.",
                        cdc_updated_at: "Timestamp of the CDC event captured by Airbyte.",
                        deleted_at: "Timestamp of the CDC event at which the entity was deleted in the source database. NULL if not deleted.",
                        _airbyte_raw_id: "Unique identifier assigned by Airbyte to each raw record ingested."
                    },
                    ...(entitySchema.keys ? parameterFunctions.getKeyColumns(entitySchema.keys) : [])
                ),
                bigquery: {
                    /* - Partition 0: current versions (valid_to IS NULL)  — touched on every run
                       - Partition 1: historical versions (valid_to IS NOT NULL) — written once, never touched again */
                    partitionBy: "RANGE_BUCKET(valid_to_partition_number, GENERATE_ARRAY(0, 2, 1))",
                    /* updatePartitionFilter ensures the MERGE only scans/rewrites current versions */
                    updatePartitionFilter: "valid_to_partition_number = 0",
                    clusterBy: [primaryKey, "is_current"],
                    labels: {
                        eventsource: params.eventSourceName.toLowerCase(),
                        sourcedataset: params.bqDatasetName.toLowerCase(),
                        sourcetype: 'airbyte',
                        entitytype: 'version'
                    }
                },
                tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'version'],
                assertions: {
                    uniqueKey: [
                        [primaryKey, "valid_from"]
                    ],
                    nonNull: [primaryKey, "valid_from"],
                    rowConditions: ['valid_from <= valid_to OR valid_to IS NULL']
                }
            })
            .preOps(ctx => `DECLARE extracted_at_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), `SELECT MAX(_airbyte_extracted_at) FROM ${ctx.self()} WHERE valid_to_partition_number = 0`, `SELECT TIMESTAMP("2026-01-01")`)}
        )`)
            .query(ctx => `
        
WITH
  source_data AS (
  /* Read new rows from the Airbyte source */
    SELECT
      CAST(${primaryKey} AS STRING) AS ${primaryKey},
      * EXCEPT (
          ${primaryKey},
          created_at,
          updated_at,
          _airbyte_raw_id,
          _airbyte_meta,
          _airbyte_generation_id,
          _ab_cdc_lsn,
          _ab_cdc_updated_at,
          _ab_cdc_deleted_at),
      TIMESTAMP(LEFT(_ab_cdc_updated_at, 26)) AS cdc_updated_at,
      CAST(created_at AS TIMESTAMP) AS created_at,
      CAST(updated_at AS TIMESTAMP) AS updated_at,
      CAST(_ab_cdc_deleted_at AS TIMESTAMP) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      AND _airbyte_extracted_at > extracted_at_checkpoint
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(${primaryKey} AS STRING), TIMESTAMP(updated_at)
    ORDER BY _airbyte_extracted_at DESC) = 1

    ${ctx.incremental() ? ` 
    /* Re-read current versions */
    UNION ALL

    SELECT * EXCEPT (valid_from, valid_to, is_deleted, is_current, version_number, valid_to_partition_number)
    FROM ${ctx.self()}
    WHERE
      valid_to_partition_number = 0

    `: ``}

  ),

/* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE. */
deletions AS (
    SELECT
      ${primaryKey},
      MAX(deleted_at) AS deleted_at
    FROM source_data
    WHERE deleted_at IS NOT NULL
    GROUP BY ${primaryKey}
  ),
live_records AS (
    SELECT *
    FROM source_data
    WHERE deleted_at IS NULL
  ),

versioned AS (
SELECT
  live_records.*,
  updated_at AS valid_from,
  /* valid_to is either the next version's updated_at,
      or if no next version exists, the deletion timestamp (if deleted) */
  COALESCE(
    LEAD(updated_at)
      OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY updated_at ASC, cdc_updated_at ASC
      ),
      deletions.deleted_at
    ) AS valid_to,
  deletions.deleted_at IS NOT NULL
    AND LEAD(updated_at)
      OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY updated_at ASC, cdc_updated_at ASC
      )
      IS NULL AS is_deleted,
  ROW_NUMBER()
    OVER (
      PARTITION BY live_records.${primaryKey}
      ORDER BY updated_at DESC, cdc_updated_at DESC
    ) = 1
    AND deletions.deleted_at IS NULL AS is_current,
  ROW_NUMBER()
    OVER (
      PARTITION BY live_records.${primaryKey}
      ORDER BY updated_at ASC, cdc_updated_at ASC
    ) AS version_number
FROM live_records
LEFT JOIN deletions USING (${primaryKey})
)

SELECT
  *,
  IF(valid_to IS NULL, 0, 1) AS valid_to_partition_number
FROM versioned

`)
            .postOps(ctx => `${data_functions.setKeyConstraints(ctx, dataform, {
      primaryKey: primaryKey + ", valid_from" 
      })}
      ${params.expirationDays ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${params.expirationDays};` : ``}
      ${entitySchema.expirationDays ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${entitySchema.expirationDays};` : ``}
    `)
    });
};
