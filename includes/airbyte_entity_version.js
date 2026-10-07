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

   hasTimestamps:
   Resolved per entity, falling back to the source-level params.hasTimestamps. Controls only whether
   the source's created_at / updated_at columns are carried through. Version ordering is controlled
   separately by versionOrderBy (below).

   versionOrderBy:
   Which timestamp orders versions and becomes valid_from. Resolved per entity; the source-level
   params.versionOrderBy only applies to entities with hasTimestamps, so a source-level 'updated_at'
   can never be inherited by an entity that has no updated_at.
   - 'updated_at' (default when hasTimestamps): the source database's updated_at.
   - 'cdc' (default when not hasTimestamps): the CDC event timestamp. Use this with hasTimestamps
     when the source's updated_at is not bumped on every change to the row. For example, in Publish
     changed_at is bumped when related records change while the parent's updated_at stays the same,
     so ordering by updated_at would give two versions the same valid_from.
   When ordering by CDC time:
   - Post-seam (Airbyte side), cdc_updated_at is the CDC event timestamp.
   - Pre-seam (legacy side), the legacy model's own valid_from is projected into cdc_updated_at, so
     legacy versions keep their original valid_from verbatim. valid_from is derived from the event
     occurred_at and so is populated regardless of the source table's columns.
   valid_to / is_current / version_number are always recomputed rather than copied from legacy: the
   final legacy version is open (valid_to IS NULL) and must be closed by the first post-cutoff CDC
   event, otherwise the entity would carry two open versions across the seam. Because the legacy
   model is contiguous (each version's valid_to equals the next version's valid_from), recomputing
   over the preserved valid_from values reproduces the legacy intervals exactly.

   Array-typed keys (isArray / integer_array):
   Stored as a canonical ARRAY: element-typed, NULL elements removed, sorted ascending.
   - Sorted rather than source-ordered because element order is not preserved through the legacy
     event-stream flattening (data/hidden_data arrive as ARRAY<STRUCT<key, value ARRAY<STRING>>>
     and UNNEST does not guarantee ordering without WITH OFFSET). Sorting is therefore the only
     representation that is stable on both sides of the cutoff seam, and it makes
     TO_JSON_STRING() comparison meaningful for downstream change detection.
   - A NULL array cannot be stored in BigQuery (it reads back as []), so absent and empty both
     normalise to []. Set preserveArrayOrder on a key to keep source order; this is only
     supportable when the legacy merge is disabled.
*/


const data_functions = require("./data_functions");
const parameterFunctions = require("./parameter_functions");

/* dataSchema dataType -> BigQuery type, for scalar columns and array elements alike. */
const BQ_TYPES = {
    boolean: 'BOOL',
    integer: 'INT64',
    float: 'FLOAT64',
    timestamp: 'TIMESTAMP',
    date: 'DATE',
    json: 'JSON',
    string: 'STRING'
};

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.tableSuffix || '_airbyte';

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.defaultPrimaryKeyField || 'id';

        /* Entity-level override wins; otherwise inherit the source-level default. Explicitly
           undefined-checked so that a per-entity `hasTimestamps: false` is distinguishable from
           "not configured at this level". */
        const hasTimestamps = entitySchema.hasTimestamps === undefined ?
            params.hasTimestamps === true :
            entitySchema.hasTimestamps === true;

        /* Which column orders versions and becomes valid_from. Entity-level wins; the source-level
           default only applies where the entity has timestamps, so it can't break entities without them. */
        const versionOrderBy = entitySchema.versionOrderBy ||
            (hasTimestamps ? (params.versionOrderBy || 'updated_at') : 'cdc');
        // The 'updated_at'/'cdc' value check is mirrored in parameter_functions.js; keep them in sync.
        // The 'updated_at' requires hasTimestamps check lives here, as hasTimestamps is only resolved per entity at this point.
        if (!['updated_at', 'cdc'].includes(versionOrderBy)) {
            throw new Error(`versionOrderBy must be 'updated_at' or 'cdc', got "${versionOrderBy}" (entity: ${entitySchema.entityTableName}).`);
        }
        if (versionOrderBy === 'updated_at' && !hasTimestamps) {
            throw new Error(`versionOrderBy 'updated_at' requires hasTimestamps (entity: ${entitySchema.entityTableName}).`);
        }
        const orderByUpdatedAt = versionOrderBy === 'updated_at';

        const hasMappedKeys = (entitySchema.keys || []).some(k => k.valueMappings && !k.historic);
        const fieldAssertionDependencies = [entitySchema.entityTableName + "_airbyte_fields_not_in_schema_" + params.eventSourceName,
            ...(hasMappedKeys ? [entitySchema.entityTableName + "_airbyte_schema_fields_missing_from_source_" + params.eventSourceName] : [])
        ];

        /* Per-entity opt-out (dataSchema includeLegacyHistory: false) for entities with no legacy history,
        e.g. ones added after the dfe-analytics entity event feed was retired. When off, the legacy table
        is never referenced and the Airbyte source is read from the default checkpoint rather than the cutoff. */
        const legacyEnabled = parameterFunctions.airbyteLegacyMergeEnabledFor(params, entitySchema);
        const legacyCutoff = params.airbyteLegacyMergeCutoff;
        const legacyModel = entitySchema.entityTableName + "_version_" + params.eventSourceName;
        if (legacyEnabled && !legacyCutoff) {
            throw new Error(`enabledAirbyteLegacyMerge is true but airbyteLegacyMergeCutoff was not provided (entity: ${entitySchema.entityTableName}).`);

        }

        /* Column that carries the version-ordering timestamp (matches the Airbyte model's). */
        const orderCol = orderByUpdatedAt ? 'updated_at' : 'cdc_updated_at';

        /* Window orderings used throughout. cdc_updated_at is a tiebreaker when ordering by updated_at. */
        const orderAsc = orderByUpdatedAt ? 'updated_at ASC, cdc_updated_at ASC' : 'cdc_updated_at ASC';
        const orderDesc = orderByUpdatedAt ? 'updated_at DESC, cdc_updated_at DESC' : 'cdc_updated_at DESC';

        /* Native entity columns == configured key names */
        const outName = k => k.alias || k.keyName;
        const mergeKeys = (entitySchema.keys || []).filter(k => outName(k) !== primaryKey);
        const keyList = mergeKeys.map(outName);

        /* Explicit, fixed column order used on BOTH sides of the UNION so alignment is positional-safe
           regardless of the Airbyte table's physical column order.
           created_at is always present: source_data emits it unconditionally (as a typed NULL when the
           entity has no timestamps) and the columns metadata below documents it in both branches, so
           omitting it here would give the same entity a different shape depending on whether the
           legacy merge ran. */
        const versionCols = [
            primaryKey,
            ...keyList,
            '_airbyte_extracted_at',
            'cdc_updated_at',
            'created_at',
            ...(hasTimestamps ? ['updated_at'] : []),
            'deleted_at',
            '_airbyte_raw_id',
            ...(params.airbyteAudit && params.airbyteAudit.enabled ? ['request_uuid', 'audit_id'] : [])
        ];
        const versionColsSql = versionCols.map(c => '`' + c + '`').join(', ');

        /* ---------- Array key helpers ---------- */

        const isArrayKey = key => key.isArray === true || key.dataType === 'integer_array';

        /* Resolve and validate the BigQuery element type of an array key. */
        function arrayElementType(key) {
            const declared = key.elementDataType || (key.dataType === 'integer_array' ? 'integer' : 'string');
            const bqType = BQ_TYPES[declared];
            if (!bqType) {
                throw new Error(`arrayElementType: unknown elementDataType "${declared}" for key "${key.keyName}" (entity: ${entitySchema.entityTableName}).`);
            }
            if (bqType === 'JSON') {
                /* JSON is not orderable or groupable in BigQuery, so it cannot be canonicalised. */
                throw new Error(`arrayElementType: JSON elements are not supported in array-typed key "${key.keyName}" (entity: ${entitySchema.entityTableName}). Store the array as a single JSON column instead.`);
            }
            return bqType;
        }

        /* Cast every element of an array expression to the target element type. */
        const arrayElements = (rawSql, elemType) =>
            `ARRAY(SELECT SAFE_CAST(v AS ${elemType}) FROM UNNEST(${rawSql}) AS v)`;

        /* Canonical form: NULL elements dropped (BigQuery cannot store them), deterministically
           ordered so both sides of the legacy UNION and successive versions are comparable. */
        function canonicalArray(elementsSql, preserveOrder) {
            if (preserveOrder) {
                return `ARRAY(SELECT x FROM UNNEST(${elementsSql}) AS x WITH OFFSET o WHERE x IS NOT NULL ORDER BY o)`;
            }
            return `ARRAY(SELECT x FROM UNNEST(${elementsSql}) AS x WHERE x IS NOT NULL ORDER BY x)`;
        }

        /* Fail loudly on array configurations that cannot be honoured, rather than producing a
           table whose contents silently disagree either side of the cutoff. */
        mergeKeys.filter(isArrayKey).forEach(key => {
            if (key.preserveArrayOrder && legacyEnabled) {
                throw new Error(`preserveArrayOrder is not supportable across the legacy merge for key "${key.keyName}" (entity: ${entitySchema.entityTableName}): element order is not preserved through flattened_entity_version. Remove preserveArrayOrder or disable enabledAirbyteLegacyMerge.`);
            }
            if (key.valueMappings) {
                throw new Error(`valueMappings is not supported on array-typed key "${key.keyName}" (entity: ${entitySchema.entityTableName}).`);
            }
        });

        /* Cast raw columns to match the data type in dataSchema.*/
        function airbyteKeyCast(key) {
            /* Arrays are handled first: the scalar historic/valueMappings branches below would
               emit a non-array expression and break positional alignment in the UNION. */
            if (isArrayKey(key)) {
                const elemType = arrayElementType(key);
                if (key.historic) {
                    /* Typed empty array. NULL arrays are not storable in BigQuery. */
                    return `ARRAY<${elemType}>[]`;
                }
                const raw = '`' + key.keyName + '`';
                /* Airbyte lands arrays either as a native repeated column or as a JSON string,
                   depending on the source connector and destination normalisation. */
                const elements = key.arraySource === 'json' ?
                    arrayElements(`JSON_VALUE_ARRAY(SAFE.PARSE_JSON(TO_JSON_STRING(${raw})))`, elemType) :
                    arrayElements(raw, elemType);
                return canonicalArray(elements, key.preserveArrayOrder === true);
            }

            if (key.historic) {
                const bqType = BQ_TYPES[key.dataType] || 'STRING';
                return `CAST(NULL AS ${bqType})`;
            }
            const raw = '`' + key.keyName + '`';
            const s = `CAST(${raw} AS STRING)`;

            // If valueMappings is configured, apply a CASE expression.
            // Unknown values fall through to the raw string value. NULL in gives NULL out via the ELSE.
            // Type cast is skipped — valueMappings always produces STRING, enforced at config validation.
            if (key.valueMappings) {
                const whenClauses = Object.entries(key.valueMappings)
                    .map(([from, to]) => `WHEN ${s} = '${from.replace(/'/g, "\\'")}' THEN '${to.replace(/'/g, "\\'")}'`)
                    .join('\n                ');
                return `CASE\n ${whenClauses}\n ELSE ${s}\n END`;
            }

            switch (key.dataType) {
                case 'boolean':
                    return `SAFE_CAST(${s} AS BOOL)`;
                case 'integer':
                    return `SAFE_CAST(${s} AS INT64)`;
                case 'float':
                    return `SAFE_CAST(${s} AS FLOAT64)`;
                case 'timestamp':
                    /* Airbyte lands Postgres timestamp columns as DATETIME. CAST(DATETIME AS STRING)
                       gives 'YYYY-MM-DD HH:MM:SS.ffffff', which stringToTimestamp rejects, so every
                       value became NULL. Cast the raw column directly; string parsing is the fallback
                       for connectors that land these columns as STRING. */
                    return `COALESCE(SAFE_CAST(${raw} AS TIMESTAMP), ${data_functions.stringToTimestamp(s)})`;
                case 'date':
                    /* Raw DATE/DATETIME/TIMESTAMP columns cast directly. The TIMESTAMP step catches strings
                      with a time component (e.g. '2026-02-11T10:30:45Z'), which a direct cast to DATE rejects.
                      stringToDate is the final fallback for other string formats. */
                    return `COALESCE(
                      SAFE_CAST(${raw} AS DATE),
                      DATE(SAFE_CAST(${raw} AS TIMESTAMP)),
                      ${data_functions.stringToDate(s)}
                    )`;
                case 'json':
                    return `SAFE.PARSE_JSON(${s})`;
                default:
                    return s; // string / undefined
            }
        }

        const airbyteKeyCastList = mergeKeys.map(k =>
            `${airbyteKeyCast(k)} AS \`${outName(k)}\`,`
        ).join('\n        ');

        /* Array keys are re-cast and canonicalised on the legacy side too. The legacy element type
           is STRING at the event-stream layer whatever flattened_entity_version presents, so the
           element type is re-asserted here rather than inherited — otherwise the UNION ALL either
           fails on type mismatch or the two sides disagree on element ordering at the seam. */
        const legacyKeyProjection = mergeKeys.map(k => {
            if (!isArrayKey(k)) return `\`${outName(k)}\``;
            const elemType = arrayElementType(k);
            const elements = arrayElements('`' + outName(k) + '`', elemType);
            return `${canonicalArray(elements, k.preserveArrayOrder === true)} AS \`${outName(k)}\``;
        }).join(',\n        ');

        /* Legacy timestamp projection. merged_full_history selects by name, so order here is cosmetic.
           Ordering by updated_at: updated_at is the ordering column, and legacy valid_from is carried
           into updated_at so version boundaries survive the seam.
           Ordering by CDC time: legacy valid_from becomes cdc_updated_at, which is what the final SELECT
           reads back out as valid_from, preserving the legacy value exactly. With timestamps, the legacy
           updated_at is passed through untouched because it is data, not the ordering column.
           Without timestamps: legacy updated_at doesn't exist in versionCols; created_at is passed
           through as-is (NULL today) so it self-corrects if the legacy model ever populates it. */
        const legacyTimestampProjection = orderByUpdatedAt
            /* COALESCE for the same per-row nullable updated_at as on the Airbyte side. valid_from is
               the right substitute here (it is already what updated_at becomes on the line below);
               on the Airbyte side there is no valid_from yet, so CDC time stands in instead. */
            ?
            `COALESCE(updated_at, valid_from) AS cdc_updated_at,
        created_at,
        valid_from AS updated_at,` :
            hasTimestamps ?
            `valid_from AS cdc_updated_at,
        created_at,
        updated_at,` :
            `valid_from AS cdc_updated_at,
        created_at,`;

        /* Collapse consecutive identical payloads: full syncs re-append unchanged rows, and where
           updated_at is NULL (no timestamps, or rows predating the migration that added them) the
           source_data dedup can't catch them. Runs after the deletion split so CDC deletes aren't
           swallowed.

           Timestamp keys are compared at millisecond precision, because legacy stores milliseconds
           and Airbyte microseconds. Stored values keep full precision. */
        const timestampKeys = new Set(
            mergeKeys
            .filter(k => k.dataType === 'timestamp' && !k.valueMappings && !isArrayKey(k))
            .map(outName)
        );
        const payloadCols = keyList.map(k =>
            timestampKeys.has(k) ?
            `TIMESTAMP_TRUNC(\`${k}\`, MILLISECOND) AS \`${k}\`` :
            '`' + k + '`'
        ).join(', ');
        const contentDedup = keyList.length > 0;

        /* Column descriptions that depend on how versions are ordered. */
        const validFromDescription = orderByUpdatedAt ?
            "Timestamp from which this version was valid (updated_at from the source database)." :
            hasTimestamps ?
            "Timestamp from which this version was valid. For versions sourced from Airbyte this is the CDC event timestamp, used instead of updated_at because updated_at is not bumped on every change to this entity in the source database. For pre-cutoff versions seeded from the legacy event-stream model, this is that model's own valid_from, carried over unchanged." :
            "Timestamp from which this version was valid. For versions sourced from Airbyte this is the CDC event timestamp, used as a substitute because this entity does not have an updated_at column in the source database. For pre-cutoff versions seeded from the legacy event-stream model, this is that model's own valid_from, carried over unchanged.";

        const cdcUpdatedAtDescription = orderByUpdatedAt ?
            "Timestamp of the CDC change event captured by Airbyte. Derived from _ab_cdc_updated_at." :
            hasTimestamps ?
            "Version-ordering timestamp. For Airbyte-sourced versions, the CDC change event timestamp derived from _ab_cdc_updated_at. For pre-cutoff versions seeded from the legacy event-stream model, that model's valid_from. Used as valid_from because updated_at is not bumped on every change to this entity in the source database." :
            "Version-ordering timestamp. For Airbyte-sourced versions, the CDC change event timestamp derived from _ab_cdc_updated_at. For pre-cutoff versions seeded from the legacy event-stream model, that model's valid_from. Used as valid_from because this entity has no updated_at column in the source database.";

        const updatedAtDescription = orderByUpdatedAt ?
            "Timestamp this entity was last updated in the source database. Also used as valid_from to derive version history." :
            "Timestamp this entity was last updated in the source database. Not used for version ordering, because it is not bumped on every change to this entity; see valid_from.";

        return publish(tableName, {
                type: "incremental",
                protected: false,
                dependencies: fieldAssertionDependencies,
                uniqueKey: [primaryKey, "valid_from"],
                description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${entitySchema.description || ''}`,
                columns: Object.assign({
                        [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity.`,
                        valid_from: validFromDescription,
                        valid_to: "Timestamp until which this version was valid. NULL if this is the current version.",
                        is_current: "TRUE if this is the most recent non-deleted version of the entity.",
                        is_deleted: "TRUE if this entity has been soft-deleted via a CDC deletion event, Airbyte full-refresh reconciliation, or for pre-cutoff history (closure of its final version in the legacy event-stream model).",
                        version_number: "Sequential version number for this entity, starting at 1 (oldest).",
                        ...(hasTimestamps ? {
                            created_at: "Timestamp this entity was first saved in the source database.",
                            updated_at: updatedAtDescription,
                        } : {
                            created_at: "Always NULL. This entity does not have a populated created_at column in the source database.",
                        }),
                        cdc_updated_at: cdcUpdatedAtDescription,
                        deleted_at: "Timestamp of the CDC deletion event at which this entity was deleted in the source database. NULL if the entity has not been deleted.",
                        _airbyte_raw_id: "Unique identifier assigned by Airbyte to each raw record ingested from the source. NULL for pre-cutoff versions seeded from the legacy event-stream model.",
                        _airbyte_extracted_at: "Timestamp when Airbyte extracted this record from the source database. NULL for pre-cutoff versions seeded from the legacy event-stream model.",
                        ...(params.airbyteAudit && params.airbyteAudit.enabled ? {
                            request_uuid: "request_uuid of the web request linked to this version via the audits table. NULL until the audit-link apply operation runs, or if no matching audit was found.",
                            audit_id: "Primary key of the audits record linked to this version. NULL until linked."
                        } : {}),    
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
            /* Legacy-seeded rows carry a NULL _airbyte_extracted_at by construction, so an entity
               whose only open version came from legacy contributes nothing to the first subquery.
               Where every open version is legacy, MAX() over them is NULL, `> NULL` matches no
               partitions, and the table silently stops ingesting. Fall back to the table-wide
               maximum, then to the cutoff for the case where no Airbyte row has landed at all. */
            .preOps(ctx => `DECLARE extracted_at_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), 
          `SELECT COALESCE(
           (SELECT MAX(_airbyte_extracted_at) FROM ${ctx.self()} WHERE valid_to IS NULL),
           (SELECT MAX(_airbyte_extracted_at) FROM ${ctx.self()}),
           TIMESTAMP("${legacyEnabled && legacyCutoff ? legacyCutoff : '2026-01-01'}")
         )`,
          `SELECT TIMESTAMP("${legacyEnabled && legacyCutoff ? legacyCutoff : '2026-01-01'}")`)}
        )`)
            .query(ctx => {

                /* Legacy is seeded only on the full historical build. 
                   On incremental runs the checkpoint is past the cutoff, so legacy is neither scanned nor referenced. */
                const injectLegacy = !ctx.incremental() && legacyEnabled;

                /* What feeds the deletions / live_records split and the window functions. */
                const versionInput = ctx.incremental() ?
                    `combined_with_current_versions` :
                    (injectLegacy ? `merged_full_history` : `source_data`);

                return `
        
WITH
  source_data AS (
  /* Read new rows from the Airbyte source, filtered to only partitions after the checkpoint.
     QUALIFY collapses same-(entity, ordering timestamp) duplicates within this batch (e.g. if a full
     sync and a CDC event for the same entity both land in the same incremental run). */
    SELECT
      CAST(${primaryKey} AS STRING) AS \`${primaryKey}\`,
      ${airbyteKeyCastList}
      _airbyte_extracted_at,
      TIMESTAMP(LEFT(_ab_cdc_updated_at, 26)) AS cdc_updated_at,
      ${hasTimestamps
        ? (orderByUpdatedAt
            /* updated_at is nullable per row on some entities (rows predating the migration that added
               the column). As the ordering column it must not be NULL, so CDC time stands in.
               cdc_updated_at is inlined rather than referenced: BigQuery does not allow a SELECT list
               item to reference a sibling alias. */
            ? `TIMESTAMP(created_at) AS created_at, COALESCE(TIMESTAMP(updated_at), TIMESTAMP(LEFT(_ab_cdc_updated_at, 26))) AS updated_at,`
            /* Not the ordering column, so keep the source value as-is, NULLs included. */
            : `TIMESTAMP(created_at) AS created_at, TIMESTAMP(updated_at) AS updated_at,`)
        : `CAST(NULL AS TIMESTAMP) AS created_at,`
        /* updated_at omitted entirely; cdc_updated_at takes its role */
        }
      TIMESTAMP(_ab_cdc_deleted_at) AS deleted_at,
      CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id
      ${params.airbyteAudit && params.airbyteAudit.enabled ? `,
      CAST(NULL AS STRING) AS request_uuid,
      CAST(NULL AS STRING) AS audit_id` : ``}
    FROM ${sourceTable}
    WHERE
      ${primaryKey} IS NOT NULL
      AND _airbyte_extracted_at > extracted_at_checkpoint
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CAST(${primaryKey} AS STRING), ${orderByUpdatedAt ? `COALESCE(TIMESTAMP(updated_at), TIMESTAMP(LEFT(_ab_cdc_updated_at, 26)))` : `cdc_updated_at`}
      ORDER BY _airbyte_extracted_at DESC
    ) = 1
  ),

${injectLegacy ? `
    legacy_data AS (
    SELECT
        \`${primaryKey}\`,
        ${legacyKeyProjection},
        CAST(NULL AS TIMESTAMP)  AS _airbyte_extracted_at,
        ${legacyTimestampProjection}
        CAST(NULL AS TIMESTAMP) AS deleted_at,
        CAST(NULL AS STRING)     AS _airbyte_raw_id
        ${params.airbyteAudit && params.airbyteAudit.enabled ? `,
      CAST(NULL AS STRING) AS request_uuid,
      CAST(NULL AS STRING) AS audit_id` : ``}
    FROM ${ctx.ref(legacyModel)}
    WHERE valid_from <= TIMESTAMP("${legacyCutoff}")
    ),

    legacy_deletions AS (
    /* The legacy model has no deleted_at. 
       An entity deleted in the legacy has every version closed (no open version anywhere in the model), and MAX(valid_to) is the deletion timestamp. */
        SELECT
            \`${primaryKey}\`,
            MAX(valid_to) AS deleted_at
        FROM ${ctx.ref(legacyModel)}
        GROUP BY 1
        HAVING COUNTIF(valid_to IS NULL) = 0
           AND MAX(valid_to) <= TIMESTAMP("${legacyCutoff}")
    ),

    merged_full_history AS (
        SELECT ${versionColsSql}
        FROM (
          SELECT ${versionColsSql}, 0 AS _merge_priority FROM source_data 
          UNION ALL
          SELECT ${versionColsSql}, 1 AS _merge_priority FROM legacy_data
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY \`${primaryKey}\`, ${orderCol}
        ORDER BY _merge_priority
    ) = 1
    ),
    
    ` : ``}

${ctx.incremental() ? `
  combined_with_current_versions AS (
  /* Re-read current versions from self() (partition 0 only).
     NOT EXISTS excludes any row whose (primaryKey, ordering timestamp) already exists in source_data.
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
          AND s.${orderCol} 
            = ${ctx.self()}.${orderCol}
      )
  ),
` : ``}

  deletions AS (
    /* Filter out deletion rows, they signal that the previous version ended. Keep them in a separate CTE.
       On the full-refresh legacy build, legacy_deletions is unioned in here so pre-cutoff deletions use the same channel.*/
    SELECT
    ${primaryKey},
    MAX(deleted_at) AS deleted_at
    FROM (
        SELECT ${primaryKey}, deleted_at
        FROM ${versionInput}
        WHERE deleted_at IS NOT NULL
        ${injectLegacy ? `
        UNION ALL

        SELECT ${primaryKey}, deleted_at
        FROM legacy_deletions
        ` : ``}
        )
    GROUP BY ${primaryKey}
    ),

  live_records AS (${contentDedup ? `
    /* Consecutive identical payloads are full-sync artefacts rather than real versions. Keep the
       earliest occurrence of each distinct payload: that is the correct valid_from. On incremental
       runs LAG sees the re-read current version from self(), so a new row is compared against the
       version it would supersede; on the full-refresh build it spans the seam, collapsing a final
       legacy version and an identical first Airbyte row into one. */
    SELECT * EXCEPT (_payload, _prev_payload)
    FROM (
      SELECT
        *,
        LAG(_payload) OVER (
          PARTITION BY ${primaryKey}
          ORDER BY ${orderAsc}
        ) AS _prev_payload
      FROM (
        SELECT
          *,
          TO_JSON_STRING((SELECT AS STRUCT ${payloadCols})) AS _payload
        FROM ${versionInput}
        WHERE deleted_at IS NULL
      )
    )
    WHERE _payload IS DISTINCT FROM _prev_payload` : `
    SELECT *
    FROM ${versionInput}
    WHERE deleted_at IS NULL`}
  )
    SELECT
      live_records.*,
      ${orderCol} AS valid_from,
      /* valid_to is either the next version's ordering timestamp, or if no next version exists, the deletion timestamp (if deleted) */
      COALESCE(
        LEAD(${orderCol}) OVER (
            PARTITION BY live_records.${primaryKey}
            ORDER BY ${orderAsc}
        ),
        IF(deletions.deleted_at > cdc_updated_at, deletions.deleted_at, NULL)
      ) AS valid_to,
      deletions.deleted_at IS NOT NULL
        AND deletions.deleted_at > ${orderCol} 
        AND LEAD(${orderCol}) OVER (
          PARTITION BY live_records.${primaryKey}
          ORDER BY ${orderAsc}
        ) IS NULL AS is_deleted,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY ${orderDesc}
      ) = 1
        AND (deletions.deleted_at IS NULL OR deletions.deleted_at <= cdc_updated_at) AS is_current,
      ROW_NUMBER() OVER (
        PARTITION BY live_records.${primaryKey}
        ORDER BY ${orderAsc}
      ) AS version_number
    FROM live_records
    LEFT JOIN deletions USING (${primaryKey})


`
            })
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