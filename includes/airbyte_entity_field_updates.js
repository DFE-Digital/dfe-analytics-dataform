/* Generates {entity}_field_updates_{source}{suffix} tables from the Airbyte version tables.
   One row for each time a field was updated on an entity, giving the field name, its previous
   value and its new value. Derived from the {entity}_version_{source}{suffix} table by comparing
   each version to the version immediately before it (LAG over version_number).

   Differences vs the legacy {eventSourceName}_entity_field_updates model:
   - One output relation per entity (like the flattened field updates tables), keyed on the
     entity's primary key column rather than a generic entity_id/entity_table_name pair.
   - CDC data carries no web request context, so event_type, request_*, response_*, device/browser
     and anonymised_user_agent_and_ip columns cannot be reproduced.
   - Only fields configured in dataSchema keys are compared. updated_at is never in that list, and
     historic keys are excluded (they no longer exist as columns in the Airbyte source), so the
     legacy behaviour of ignoring updated_at churn is preserved automatically.
   - An entity's first version produces no rows (LAG yields NULL previous_data, and the inner
     JOIN on UNNEST drops it), matching the legacy behaviour where creations only appear once a
     prior version exists to diff against.
   - key_updated reports the *output* (aliased) field name, matching the column names in the
     version and flattened tables, not the raw source column name.
*/

const data_functions = require("./data_functions");
const airbyteReconciliation = require("./airbyte_reconciliation");

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.tableSuffix || '_airbyte';

    return params.dataSchema.map(tableSchema => {
        const tableName = `${tableSchema.entityTableName}_field_updates_${params.eventSourceName}${suffix}`;
        const versionTableName = `${tableSchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const primaryKey = tableSchema.primaryKey || params.airbyteConfig.defaultPrimaryKeyField || 'id';
        const hasTimestamps = tableSchema.hasTimestamps;
        const materialisation = tableSchema.materialisation || 'table';

        /* The version table publishes each key under its alias when one is configured, falling
           back to the raw source column name. Must match the version model's outName exactly. */
        const outName = k => k.alias || k.keyName;

        /* Fields to diff between versions: configured keys, excluding
           - historic keys (no longer present as columns in the Airbyte source / version table)
           - the primary key itself (it can never change between versions of the same entity,
             and for tables like sign_offs/disabilities it is only in keys to give it an alias).
             Compared on the output name, mirroring the version model's mergeKeys filter. */
        const trackedKeys = (tableSchema.keys || []).filter(k => !k.historic && outName(k) !== primaryKey);

        /* Nothing to diff for this entity - don't publish a field updates relation at all. */
        if (trackedKeys.length === 0) return null;

        /* Reference version table columns by their output (aliased) names - the raw keyName does
           not exist as a column in the version table when an alias is configured.
           JSON-typed columns can't be CAST to STRING directly.
           The struct's key literal also uses the output name so that key_updated matches the
           column names consumers see in the version and flattened tables. */
        const newDataStructSql = trackedKeys.map(k => {
            const col = outName(k);
            const valueSql = k.dataType === 'json'
                ? `TO_JSON_STRING(\`${col}\`)`
                : `CAST(\`${col}\` AS STRING)`;
            return `STRUCT('${col}' AS key, ${valueSql} AS value)`;
        }).join(',\n      ');

        return publish(tableName, {
                ...params.defaultConfig,
                type: materialisation,
                /* The schema assertions are already upstream of this table via the version table
                   (ctx.ref below), so they don't need repeating here. The reconciliation apply
                   operation is not in the ref graph, so it does. */
                dependencies: [
                    ...(params.airbyteReconciliation.enabled
                        ? [airbyteReconciliation.reconciliationNames(params, tableSchema).applyOperationName]
                        : [])
                ],
                ...(materialisation == 'table' ? {
                    assertions: {
                        uniqueKey: [
                            [primaryKey, "occurred_at", "key_updated"]
                        ],
                        nonNull: [primaryKey, "occurred_at", "key_updated"]
                    }
                } : {}),
                bigquery: {
                    ...(materialisation == 'table' ? {
                        partitionBy: "DATE(occurred_at)",
                        clusterBy: [primaryKey, "key_updated"]
                    } : {}),
                    labels: {
                        eventsource: params.eventSourceName.toLowerCase(),
                        sourcedataset: params.bqDatasetName.toLowerCase(),
                        sourcetype: 'airbyte',
                        entitytabletype: 'field_updates'
                    }
                },
                tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'field_updates'],
                description: `[AIRBYTE] One row for each time a field was updated on a ${tableSchema.entityTableName} entity, giving the field name, its previous value and its new value. Derived from ${versionTableName} by comparing each version to the one immediately before it. An entity's first version, and changes to created_at/updated_at/${primaryKey}, are not included. ${tableSchema.description || ''}`,
                columns: {
                    [primaryKey]: {
                        description: `Primary key of the ${tableSchema.entityTableName} entity that was updated.`,
                        bigqueryPolicyTags: tableSchema.hidePrimaryKey && params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                    },
                    occurred_at: "Timestamp of the entity version that this field update was part of (valid_from in the version table).",
                    update_id: `UID for the collection of field updates that took place to this entity at this time. One-way hash of ${primaryKey} and occurred_at. Useful for COUNT DISTINCTs.`,
                    key_updated: "The name of the field that was updated, as it appears in the version table (i.e. the configured alias where one exists).",
                    new_value: {
                        description: "The value of this field after it was updated, cast to a string. Hidden because it may contain values of some fields which are configured to be hidden.",
                        bigqueryPolicyTags: params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                    },
                    previous_value: {
                        description: "The value of this field before it was updated, cast to a string. Hidden because it may contain values of some fields which are configured to be hidden.",
                        bigqueryPolicyTags: params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                    },
                    previous_occurred_at: "Timestamp this entity was previously updated (valid_from of the previous version).",
                    seconds_since_previous_update: "The number of seconds between occurred_at and previous_occurred_at.",
                    ...(hasTimestamps ? {
                        seconds_since_created: "The number of seconds between occurred_at and created_at."
                    } : {})
                }
            })
            .query(ctx => `
WITH versions_with_new_data AS (
  SELECT
    ${primaryKey},
    version_number,
    valid_from AS occurred_at,
    ${hasTimestamps ? `created_at,` : ``}
    [${newDataStructSql}] AS new_data
  FROM
    ${ctx.ref(versionTableName)}
),
versions_with_field_arrays AS (
  SELECT
    ${primaryKey},
    occurred_at,
    ${hasTimestamps ? `created_at,` : ``}
    new_data,
    LAG(new_data) OVER versions_of_this_entity AS previous_data,
    LAG(occurred_at) OVER versions_of_this_entity AS previous_occurred_at
  FROM
    versions_with_new_data
  WINDOW versions_of_this_entity AS (
    PARTITION BY ${primaryKey}
    ORDER BY version_number ASC
  )
)
SELECT
  versions_with_field_arrays.${primaryKey},
  occurred_at,
  FARM_FINGERPRINT(${primaryKey} || CAST(occurred_at AS STRING)) AS update_id,
  new_data_combined.key AS key_updated,
  new_data_combined.value AS new_value,
  previous_data_combined.value AS previous_value,
  previous_occurred_at,
  TIMESTAMP_DIFF(occurred_at, previous_occurred_at, SECOND) AS seconds_since_previous_update
  ${hasTimestamps ? `, TIMESTAMP_DIFF(occurred_at, created_at, SECOND) AS seconds_since_created` : ``}
FROM
  versions_with_field_arrays
  CROSS JOIN UNNEST(new_data) AS new_data_combined
  JOIN UNNEST(previous_data) AS previous_data_combined ON new_data_combined.key = previous_data_combined.key
WHERE
  IFNULL(new_data_combined.value, "null") != IFNULL(previous_data_combined.value, "null")
`)
            .postOps(ctx => materialisation == 'table'
                /* Key constraints can only be set on tables - ALTER TABLE ... ADD PRIMARY KEY /
                   ADD CONSTRAINT fails with "ALTER TABLE only supports altering constraints for
                   tables" when the entity is materialised as a view. */
                ? `${data_functions.setKeyConstraints(ctx, dataform, {
                    primaryKey: `${primaryKey}, occurred_at, key_updated`,
                    foreignKeys: [
                        {keyInThisTable: `${primaryKey}, occurred_at`, foreignTable: versionTableName, keyInForeignTable: `${primaryKey}, valid_from`}
                    ]
                })}`
                : ``)
    }).filter(Boolean);
};