/* Generates {entity}_latest_{source}{suffix} tables from Airbyte raw data; Contains the most recent version of each entity. */

const parameterFunctions = require("./parameter_functions");
const airbyteReconciliation = require("./airbyte_reconciliation");
const data_functions = require("./data_functions");

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.tableSuffix || '_airbyte';

    return params.dataSchema.map(tableSchema => {
        const versionTableName = `${tableSchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const primaryKey = tableSchema.primaryKey || params.airbyteConfig.defaultPrimaryKeyField || 'id';
        const hasTimestamps = tableSchema.hasTimestamps === undefined
            ? params.hasTimestamps === true
            : tableSchema.hasTimestamps === true;

        publish(tableSchema.entityTableName + "_latest_" + params.eventSourceName + suffix, {
            ...params.defaultConfig,
            dependencies: [
                ...(params.airbyteReconciliation.enabled
                    ? [airbyteReconciliation.reconciliationNames(params, tableSchema).applyOperationName]
                    : [])
            ],
            type: tableSchema.materialisation || 'table',
            ...((tableSchema.materialisation || 'table') == "table" ? {
                assertions: {
                    uniqueKey: [primaryKey],
                    nonNull: ["last_streamed_event_occurred_at", primaryKey]
                }
            } : {}),
            bigquery: {
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase(),
                    entitytabletype: "latest"
                },
                ...((tableSchema.materialisation || 'table') == "table" ? {
                    partitionBy: hasTimestamps 
                        ? "DATE(created_at)" 
                        : "DATE(last_streamed_event_occurred_at)" 
                    } : {})
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'latest'],
            description: "[AIRBYTE] Latest version of " + tableSchema.entityTableName + ". Sourced from the Airbyte version table in the " + params.airbyteConfig.datasetName + " dataset. " + (tableSchema.description || ''),
            columns: Object.assign({
                last_streamed_event_occurred_at: "Timestamp of the last Airbyte CDC update for this entity (cdc_updated_at from the latest version).",
                [primaryKey]: {
                    description: `Primary key of the ${tableSchema.entityTableName} entity.`,
                    bigqueryPolicyTags: tableSchema.hidePrimaryKey && params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                },
                ...(hasTimestamps ? {
                        created_at: "Timestamp this entity was first saved in the database.",
                        updated_at: "Timestamp this entity was last updated in the database.",
                    } : {
                        created_at: "Always NULL. This entity does not have a created_at column in the source database (non-Rails service).",
                    }),
            }, ...parameterFunctions.getKeyColumns(tableSchema.keys))
        }).query(ctx => `
SELECT
    * EXCEPT(_airbyte_raw_id, valid_from, valid_to, is_current, is_deleted, version_number, cdc_updated_at),
    cdc_updated_at AS last_streamed_event_occurred_at
FROM
    ${ctx.ref(versionTableName)}
WHERE
    valid_to IS NULL
`)
        /* Match the dfe-analytics _latest_ tables, which get a primary key constraint in
           flattened_entity_latest.js, so downstream foreign keys can point at airbyte tables. */
        .postOps(ctx => (tableSchema.materialisation || 'table') == "table"
            ? data_functions.setKeyConstraints(ctx, dataform, { primaryKey: primaryKey })
            : ``)
    });
};