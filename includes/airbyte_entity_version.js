/* Generates {entity}_version_{source}{suffix} tables from Airbyte CDC data. */

const data_functions = require("./data_functions");

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;
    if (!params.enableAirbyteSource || !params.airbyteEnableVersioning) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';

    const getKeys = (keys) => {
    return keys.filter(k => !k.historic).map(key => ({
        [key.alias || key.keyName]: {
          description: key.description,
          bigqueryPolicyTags: key.hidden && key.hiddenPolicyTagLocation ? [key.hiddenPolicyTagLocation] : []
        }
    }))
    };

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';
        
        const fieldAssertionDependencies = params.airbyteEnableAssertions
          ? params.dataSchema.map(schema => schema.entityTableName + "_airbyte_fields_not_in_schema_" + params.eventSourceName)
          : [];

        return publish(tableName, {
            type: "incremental",
            protected: false,
            dependencies: fieldAssertionDependencies, 
            uniqueKey: [primaryKey, "valid_from", "cdc_updated_at"],
            description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${entitySchema.description || ''}`,
            columns: Object.assign(
                {
                    [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity.`,
                    valid_from: "Timestamp from which this version was valid (updated_at from the source).",
                    valid_to: "Timestamp until which this version was valid. NULL = current.",
                    is_current: "TRUE if this is the current version.",
                    is_deleted: "TRUE if this entity is deleted.",
                    version_number: "Sequential version number (1 = latest).",
                    created_at: "Timestamp this entity was first saved in the database.",
                    updated_at: "Timestamp this entity was last updated in the database.",
                    cdc_updated_at: "Timestamp of the CDC event captured by Airbyte.",
                    deleted_at: "Timestamp of the CDC event at which the entity was deleted in the source database. NULL if not deleted.",
                    _airbyte_raw_id: "Unique identifier assigned by Airbyte to each raw record ingested."
                },
                ...(entitySchema.keys ? getKeys(entitySchema.keys) : [])
            ),
            bigquery: {
                partitionBy: "DATE(cdc_updated_at)",
                clusterBy: [primaryKey],
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase(),
                    sourcetype: 'airbyte',
                    entitytype: 'version'
                }
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte'],
            assertions: {
                uniqueKey: [
                    /* Airbyte creates duplicates on schema changes; added cdc_updated_at to the unique key assertion. */
                    [primaryKey, "valid_from", "cdc_updated_at"]
                ],
                nonNull: [primaryKey, "valid_from"],
                rowConditions: ['valid_from <= valid_to OR valid_to IS NULL']
            }
        }).query(ctx => `
        
WITH
  source_data AS (
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
      CAST(SUBSTR(_ab_cdc_updated_at, 1, 23) AS TIMESTAMP) AS cdc_updated_at,
      CAST(created_at AS TIMESTAMP) AS created_at,
      CAST(updated_at AS TIMESTAMP) AS updated_at,
      CAST(_ab_cdc_deleted_at AS TIMESTAMP) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      ${ctx.incremental() ? `
      AND TIMESTAMP(SUBSTR(_ab_cdc_updated_at, 1, 23)) > (
        SELECT COALESCE(MAX(cdc_updated_at), TIMESTAMP('2026-01-01'))
        FROM ${ctx.self()}
      ) ` : ''}
  ),

/* Deduplicate rows that are identical in all columns.
     This handles:
     - Full syncs: same data re-appended with new cdc_updated_at
     - Schema changes where old rows are re-synced but values haven't changed
     
     It does NOT dedupe when a new field is added and has a different value
     (e.g. old row has NULL, new row has 'some_value') */
  source_data_deduplicate AS (
    SELECT * EXCEPT (dedup_rn)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY
            REGEXP_REPLACE(
              TO_JSON_STRING(source_data),
              r'"(_airbyte_raw_id|cdc_updated_at|deleted_at)":\s*"[^"]*"',
              ''
            )
          ORDER BY cdc_updated_at DESC
        ) AS dedup_rn
      FROM source_data
    )
    WHERE dedup_rn = 1
  ),

${ctx.incremental() ? ` 
/* Re-read current versions so the window can include them in next steps */
existing_open AS (
    SELECT * EXCEPT (valid_from, valid_to, is_deleted, is_current, version_number)
    FROM ${ctx.self()}
    WHERE is_current = TRUE
  ),
combined AS (
    SELECT * FROM source_data_deduplicate
    UNION ALL
    SELECT * FROM existing_open
  ),

/* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE. */
deletions AS (
    SELECT
      ${primaryKey},
      deleted_at
    FROM combined
    WHERE deleted_at IS NOT NULL
  ),
live_records AS (
    SELECT *
    FROM combined
    WHERE deleted_at IS NULL
  )
`: `
/* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE. */
deletions AS (
    SELECT
      ${primaryKey},
      deleted_at
    FROM source_data_deduplicate
    WHERE deleted_at IS NOT NULL
        ),
live_records AS (
    SELECT *
    FROM source_data_deduplicate
    WHERE deleted_at IS NULL
  )

`}
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
            ORDER BY updated_at ASC
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
          ORDER BY updated_at DESC, cdc_updated_at DESC
        ) AS version_number
    FROM live_records
    LEFT JOIN deletions
      ON live_records.${primaryKey} = deletions.${primaryKey}

`)
  .postOps(ctx => `${data_functions.setKeyConstraints(ctx, dataform, {
            primaryKey: primaryKey + ", valid_from" + ", cdc_updated_at"
            })}
            ${params.expirationDays && ctx.incremental() ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${params.expirationDays};` : ``}
            ${entitySchema.expirationDays && ctx.incremental() ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${entitySchema.expirationDays};` : ``}
        `)
    });
};