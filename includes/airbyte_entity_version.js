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
                        _airbyte_raw_id: "Unique identifier assigned by Airbyte to each raw record ingested.",
                        _airbyte_extracted_at: "Timestamp of the Airbyte extraction"
                    },
                    ...(entitySchema.keys ? parameterFunctions.getKeyColumns(entitySchema.keys) : [])
                ),
                bigquery: {
                    partitionBy: "DATE(valid_to)",
                    /* updatePartitionFilter ensures the MERGE only scans/rewrites current versions */
                    updatePartitionFilter: "valid_to IS NULL",
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
        ${ctx.when(ctx.incremental(), `SELECT MAX(_airbyte_extracted_at) FROM ${ctx.self()} WHERE valid_to IS NULL`, `SELECT TIMESTAMP("2026-01-01")`)}
        )`)
            .query(ctx => `
        
WITH
  source_data AS (
  /* Read new rows from the Airbyte source, filtered to only partitions after the checkpoint.
     QUALIFY collapses same-(entity, updated_at) duplicates within this batch (e.g. if a full
     sync and a CDC event for the same entity both land in the same incremental run). */
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
          _ab_cdc_deleted_at
      ),
      TIMESTAMP(LEFT(_ab_cdc_updated_at, 26)) AS cdc_updated_at,
      TIMESTAMP(created_at) AS created_at,
      TIMESTAMP(updated_at) AS updated_at,
      TIMESTAMP(_ab_cdc_deleted_at) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      AND _airbyte_extracted_at > extracted_at_checkpoint
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CAST(${primaryKey} AS STRING), TIMESTAMP(updated_at)
      ORDER BY _airbyte_extracted_at DESC
    ) = 1
  ),

${ctx.incremental() ? `
  combined_with_current_versions AS (
  /* Re-read current versions from self() (partition 0 only).
     NOT EXISTS excludes any row whose (primaryKey, updated_at) already exists in source_data.
     This handles full syncs that arrive days later: source_data's copy of the row wins, and the stale self() copy is dropped before any window functions run. 
     Without this,the QUALIFY in source_data would not help because it only deduplicates within the new batch, it cannot see rows already sitting in self(). */
    SELECT * FROM source_data
    
    UNION ALL

    SELECT * EXCEPT (valid_from, valid_to, is_deleted, is_current, version_number)
    FROM ${ctx.self()}
    WHERE
      valid_to IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM source_data s
        WHERE s.${primaryKey} = ${ctx.self()}.${primaryKey}
          AND s.updated_at = ${ctx.self()}.updated_at
      )
  ),
` : ``}

  deletions AS (
  /* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE. */
    SELECT
      ${primaryKey},
      MAX(deleted_at) AS deleted_at
    FROM ${ctx.incremental() ? `combined_with_current_versions` : `source_data`}
    WHERE deleted_at IS NOT NULL
    GROUP BY ${primaryKey}
  ),

  live_records AS (
    SELECT *
    FROM ${ctx.incremental() ? `combined_with_current_versions` : `source_data`}
    WHERE deleted_at IS NULL
  )
    SELECT
      live_records.*,
      updated_at AS valid_from,
      /* valid_to is either the next version's updated_at, or if no next version exists, the deletion timestamp (if deleted) */
      COALESCE(
        LEAD(updated_at) OVER (
          PARTITION BY live_records.${primaryKey}
          ORDER BY updated_at ASC, cdc_updated_at ASC
        ),
        IF(deletions.deleted_at > cdc_updated_at, deletions.deleted_at, NULL)
      ) AS valid_to,
      deletions.deleted_at IS NOT NULL
        AND deletions.deleted_at > updated_at
        AND LEAD(updated_at) OVER (
          PARTITION BY live_records.${primaryKey}
          ORDER BY updated_at ASC, cdc_updated_at ASC
        ) IS NULL AS is_deleted,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY updated_at DESC, cdc_updated_at DESC
      ) = 1
        AND (deletions.deleted_at IS NULL OR deletions.deleted_at <= cdc_updated_at) AS is_current,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY updated_at ASC, cdc_updated_at ASC
      ) AS version_number
    FROM live_records
    LEFT JOIN deletions USING (${primaryKey})


`)
            .postOps(ctx => `${data_functions.setKeyConstraints(ctx, dataform, {
      primaryKey: primaryKey + ", valid_from" 
      })}
      ${params.expirationDays ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${params.expirationDays};` : ``}
      ${entitySchema.expirationDays ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${entitySchema.expirationDays};` : ``}
    `)
    });
};
