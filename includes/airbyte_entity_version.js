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
  
   Legacy merge:
   Optionally seeds pre-cutoff version history from the legacy {entityTableName}_version_{eventSourceName}

   model (from the event-stream / dfe-analytics pipeline via flattened_entity_version).

   column and injected BEFORE the window functions, so valid_to / is_current /
   version_number recompute across the cutoff seam. Applied on full-refresh only; on incremental
   runs the checkpoint is past the cutoff and legacy is never touched.
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
        const hasTimestamps = entitySchema.hasTimestamps;

        const fieldAssertionDependencies = params.airbyteEnableAssertions ?
            params.dataSchema.map(schema => schema.entityTableName + "_airbyte_fields_not_in_schema_" + params.eventSourceName) : [];
        
        const legacyEnabled = params.enabledAirbyteLegacyMerge === true;
        const legacyCutoff = params.airbyteLegacyMergeCutoff;
        const legacyModel = entitySchema.entityTableName + "_version_" + params.eventSourceName;
        if (legacyEnabled && !legacyCutoff) {
            throw new Error(`enabledAirbyteLegacyMerge is true but airbyteLegacyMergeCutoff was not provided (entity: ${entitySchema.entityTableName}).`);

        }

        /* Column that carries the version-ordering timestamp (matches the Airbyte model's). */
        const orderCol = hasTimestamps ? 'updated_at' : 'cdc_updated_at';

        /* Native entity columns == configured key names */
        const outName = k => k.alias || k.keyName;
        const mergeKeys = (entitySchema.keys || []).filter(k => outName(k) !== primaryKey);
        const keyList = mergeKeys.map(outName);

        /* Explicit, fixed column order used on BOTH sides of the UNION so alignment is positional-safe
           regardless of the Airbyte table's physical column order. */
        const versionCols = [
            primaryKey,
            ...keyList,
            '_airbyte_extracted_at',
            'cdc_updated_at',
            'created_at',
            ...(hasTimestamps ? ['updated_at'] : []),
            'deleted_at',
            '_airbyte_raw_id'
        ];
        const versionColsSql = versionCols.join(', ');

        /* Cast raw columns to match the data type in dataSchema.*/
        function airbyteKeyCast(key) {
            if ((key.isArray || key.dataType === 'integer_array') && legacyEnabled) {
                throw new Error(`airbyteKeyCast: array-typed key "${key.keyName}" (entity: ${entitySchema.entityTableName}) is not supported by the legacy merge yet.`);
            }
            if (key.historic) {
                const bqType = { boolean: 'BOOL', integer: 'INT64', float: 'FLOAT64', timestamp: 'TIMESTAMP', date: 'DATE', json: 'JSON' }[key.dataType] || 'STRING';
                return `CAST(NULL AS ${bqType})`;
            }
            const raw = '`' + key.keyName + '`';
            const s = `CAST(${raw} AS STRING)`;
            switch (key.dataType) {
                case 'boolean':   return `SAFE_CAST(${s} AS BOOL)`;
                case 'integer':   return `SAFE_CAST(${s} AS INT64)`;
                case 'float':     return `SAFE_CAST(${s} AS FLOAT64)`;
                case 'timestamp': return data_functions.stringToTimestamp(s);
                case 'date':      return data_functions.stringToDate(s);
                case 'json':      return `SAFE.PARSE_JSON(${s})`;
                default:          return s; // string / undefined
            }
        }

        const airbyteKeyCastList = mergeKeys.map(k =>
            `${airbyteKeyCast(k)} AS \`${outName(k)}\`,`
        ).join('\n        ');
        
        const legacyKeyProjection = mergeKeys.map(k =>
            `\`${outName(k)}\``
        ).join(',\n        ');
        
        return publish(tableName, {
                type: "incremental",
                protected: false,
                dependencies: fieldAssertionDependencies,
                uniqueKey: [primaryKey, "valid_from"],
                description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${entitySchema.description || ''}`,
                columns: Object.assign({
                        [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity.`,
                        valid_from: hasTimestamps ?
                            "Timestamp from which this version was valid (updated_at from the source database)." :
                            "Timestamp from which this version was valid (CDC event timestamp from Airbyte, used as a substitute because this entity does not have an updated_at column in the source database).",
                        valid_to: "Timestamp until which this version was valid. NULL if this is the current version.",
                        is_current: "TRUE if this is the most recent non-deleted version of the entity.",
                        is_deleted: "TRUE if this entity has been soft-deleted via a CDC deletion event or Airbyte full-refresh reconciliation.",
                        version_number: "Sequential version number for this entity, starting at 1 (oldest).",
                        ...(hasTimestamps ? {
                            created_at: "Timestamp this entity was first saved in the source database.",
                            updated_at: "Timestamp this entity was last updated in the source database. Also used as valid_from to derive version history.",
                        } : {
                            created_at: "Always NULL. This entity does not have a created_at column in the source database (non-Rails service).",
                        }),
                        cdc_updated_at: "Timestamp of the CDC change event captured by Airbyte. Derived from _ab_cdc_updated_at. For entities without updated_at, this is also used as valid_from.",
                        deleted_at: "Timestamp of the CDC deletion event at which this entity was deleted in the source database. NULL if the entity has not been deleted.",
                        _airbyte_raw_id: "Unique identifier assigned by Airbyte to each raw record ingested from the source.",
                        _airbyte_extracted_at: "Timestamp when Airbyte extracted this record from the source database.",
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
            .query(ctx => {

            /* Legacy is seeded only on the full historical build. 
               On incremental runs the checkpoint is past the cutoff, so legacy is neither scanned nor referenced. */
            const injectLegacy = !ctx.incremental() && legacyEnabled;

            /* What feeds the deletions / live_records split and the window functions. */
            const versionInput = ctx.incremental()
                ? `combined_with_current_versions`
                : (injectLegacy ? `merged_full_history` : `source_data`);

            return `
        
WITH
  source_data AS (
  /* Read new rows from the Airbyte source, filtered to only partitions after the checkpoint.
     QUALIFY collapses same-(entity, updated_at) duplicates within this batch (e.g. if a full
     sync and a CDC event for the same entity both land in the same incremental run). */
    SELECT
      CAST(${primaryKey} AS STRING) AS ${primaryKey},
      ${airbyteKeyCastList}
      _airbyte_extracted_at,
      TIMESTAMP(LEFT(_ab_cdc_updated_at, 26)) AS cdc_updated_at,
      ${hasTimestamps
        ? `TIMESTAMP(created_at) AS created_at, TIMESTAMP(updated_at) AS updated_at,`
        : `CAST(NULL AS TIMESTAMP) AS created_at,`
        /* updated_at omitted entirely; cdc_updated_at takes its role */
        }
      TIMESTAMP(_ab_cdc_deleted_at) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      AND _airbyte_extracted_at > extracted_at_checkpoint
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CAST(${primaryKey} AS STRING), ${hasTimestamps ? `TIMESTAMP(updated_at)` : `cdc_updated_at`}
      ORDER BY _airbyte_extracted_at DESC
    ) = 1
  ),

${injectLegacy ? `
    legacy_live AS (
    SELECT
        CAST(id AS STRING) AS ${primaryKey},
        ${legacyKeyProjection},
        CAST(NULL AS TIMESTAMP) AS _airbyte_extracted_at,
        valid_from AS cdc_updated_at,
        ${hasTimestamps
            ? `created_at,
        COALESCE(updated_at, valid_from) AS updated_at,`
            : `CAST(NULL AS TIMESTAMP) AS created_at,`}
        CAST(NULL AS TIMESTAMP) AS deleted_at,
        CAST(NULL AS STRING) AS _airbyte_raw_id
    FROM ${ctx.ref(legacyModel)}
    WHERE valid_from < TIMESTAMP("${legacyCutoff}")
    ),

    legacy_deletion_markers AS (
        SELECT * FROM (
            SELECT
                CAST(${primaryKey} AS STRING) AS ${primaryKey},
                ${legacyKeyProjection},
                CAST(NULL AS TIMESTAMP) AS _airbyte_extracted_at,
                valid_to AS cdc_updated_at,
                ${hasTimestamps
                    ? `created_at,
            valid_to AS updated_at,`
                    : `CAST(NULL AS TIMESTAMP) AS created_at,`}
                valid_to AS deleted_at,
                CAST(NULL AS STRING) AS _airbyte_raw_id
            FROM ${ctx.ref(legacyModel)}
            WHERE valid_from < TIMESTAMP("${legacyCutoff}")
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY valid_from DESC) = 1
        )
        WHERE deleted_at IS NOT NULL
            AND deleted_at < TIMESTAMP("${legacyCutoff}")
    ),

    merged_full_history AS (
        SELECT ${versionColsSql}
        FROM (
            SELECT ${versionColsSql}, 0 AS _merge_priority FROM source_data
            UNION ALL
            SELECT ${versionColsSql}, 1 AS _merge_priority FROM legacy_live
            UNION ALL
            SELECT ${versionColsSql}, 1 AS _merge_priority FROM legacy_deletion_markers
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ${primaryKey}, ${orderCol}
            ORDER BY _merge_priority
        ) = 1
    ),
    ` : ``}

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
          AND s.${hasTimestamps ? `updated_at` : `cdc_updated_at`} 
            = ${ctx.self()}.${hasTimestamps ? `updated_at` : `cdc_updated_at`}
      )
  ),
` : ``}

  deletions AS (
    /* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE. */
    SELECT
    ${primaryKey},
    MAX(deleted_at) AS deleted_at
    FROM ${versionInput}
    WHERE deleted_at IS NOT NULL
    GROUP BY ${primaryKey}
    ),

  live_records AS (
    SELECT *
    FROM ${versionInput}
    WHERE deleted_at IS NULL
  )
    SELECT
      live_records.*,
      ${hasTimestamps ? `updated_at` : `cdc_updated_at`} AS valid_from,
      /* valid_to is either the next version's updated_at, or if no next version exists, the deletion timestamp (if deleted) */
      COALESCE(
        LEAD(${hasTimestamps ? `updated_at` : `cdc_updated_at`}) OVER (
            PARTITION BY live_records.${primaryKey}
            ORDER BY ${hasTimestamps ? `updated_at ASC, ` : ``} cdc_updated_at ASC
        ),
        IF(deletions.deleted_at > cdc_updated_at, deletions.deleted_at, NULL)
      ) AS valid_to,
      deletions.deleted_at IS NOT NULL
        AND deletions.deleted_at > ${hasTimestamps ? `updated_at` : `cdc_updated_at`} 
        AND LEAD(${hasTimestamps ? `updated_at` : `cdc_updated_at`}) OVER (
          PARTITION BY live_records.${primaryKey}
          ORDER BY ${hasTimestamps ? `updated_at ASC, ` : ``}cdc_updated_at ASC
        ) IS NULL AS is_deleted,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY ${hasTimestamps ? `updated_at DESC, ` : ``}cdc_updated_at DESC
      ) = 1
        AND (deletions.deleted_at IS NULL OR deletions.deleted_at <= cdc_updated_at) AS is_current,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY ${hasTimestamps ? `updated_at ASC, ` : ``}cdc_updated_at ASC
      ) AS version_number
    FROM live_records
    LEFT JOIN deletions USING (${primaryKey})


`})
            .postOps(ctx => `
      ${data_functions.setKeyConstraints(ctx, dataform, {
        primaryKey: primaryKey + ", valid_from"
      })}

      ${params.expirationDays
        ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${params.expirationDays};`
        : ``}

      ${entitySchema.expirationDays
        ? `DELETE FROM ${ctx.self()} WHERE DATE(valid_from) < CURRENT_DATE - ${entitySchema.expirationDays};`
        : ``}

      ALTER TABLE ${ctx.self()}
      SET OPTIONS (
        partition_expiration_days = ${params.expirationDays || `NULL`}
      );
    `)
    });
};
