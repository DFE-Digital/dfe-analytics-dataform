/* Generates {entity}_version_{source}{suffix} tables from Airbyte CDC data. */

module.exports = (params) => {
    if (!params.enableAirbyteSource || !params.airbyteEnableVersioning) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';

        return publish(tableName, {
            type: "incremental",
            protected: false,
            uniqueKey: [primaryKey, "valid_from"],
            description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${entitySchema.description || ''}`,
            columns: {
                [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity.`,
                valid_from: "Timestamp from which this version was valid (updated_at from the source).",
                valid_to: "Timestamp until which this version was valid. NULL = current.",
                is_current: "TRUE if this is the current version.",
                version_number: "Sequential version number (1 = first).",
                created_at: "Timestamp this entity was first saved in the database.",
                updated_at: "Timestamp this entity was last updated in the database."
            },
            bigquery: {
                partitionBy: "DATE(valid_from)",
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
                    [primaryKey, "valid_from"]
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
          _ab_cdc_deleted_at,
          _ab_cdc_updated_at),
      CAST(created_at AS TIMESTAMP) AS created_at,
      CAST(updated_at AS TIMESTAMP) AS updated_at,
      CAST(_ab_cdc_deleted_at AS TIMESTAMP) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      ${ctx.incremental() ? `
      AND _airbyte_extracted_at > (
        SELECT COALESCE(MAX(_airbyte_extracted_at), TIMESTAMP('2026-01-01'))
        FROM ${ctx.self()}
      ) ` : ''}
  ),
${ctx.incremental() ? ` 
/* Re-read current versions so the window can include them in next steps */
existing_open AS (
    SELECT * EXCEPT (valid_from, valid_to, is_deleted, is_current, version_number)
    FROM ${ctx.self()}
    WHERE is_current = TRUE
  ),
combined AS (
    SELECT * FROM source_data
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
    FROM source_data
    WHERE deleted_at IS NOT NULL
        ),
live_records AS (
    SELECT *
    FROM source_data
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
            ORDER BY updated_at ASC
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
          ORDER BY updated_at DESC
        ) = 1
        AND deletions.deleted_at IS NULL AS is_current,
      ROW_NUMBER()
        OVER (
          PARTITION BY live_records.${primaryKey}
          ORDER BY updated_at ASC
        ) AS version_number
    FROM live_records
    LEFT JOIN deletions
      ON live_records.${primaryKey} = deletions.${primaryKey}

`)
 .postOps(ctx => `
            ${data_functions.setKeyConstraints(ctx, dataform, {
                primaryKey: primaryKey + ", valid_from"
            })}
             /* data retention: delete versions older than the entity-specific expirationDays */
            ${params.expirationDays && ctx.incremental() ? `
                DELETE FROM ${ctx.self()}
                WHERE DATE(valid_from) < CURRENT_DATE - ${params.expirationDays};
            ` : ``}
            ${entitySchema.expirationDays && ctx.incremental() ? `
                DELETE FROM ${ctx.self()}
                WHERE DATE(valid_from) < CURRENT_DATE - ${entitySchema.expirationDays};
            ` : ``}
        `);
    });
};