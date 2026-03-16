/* Generates {entity}_latest_{source}{suffix} tables from Airbyte raw data; Contains the most recent version of each entity. */

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';
    const primaryKey = params.airbyteConfig.primaryKeyField || 'id';

    return params.dataSchema.forEach(tableSchema => {
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${tableSchema.entityTableName}\``;

        publish(tableSchema.entityTableName + "_latest_" + params.eventSourceName + suffix, {
            ...params.defaultConfig,
            type: tableSchema.materialisation || 'table',
            ...((tableSchema.materialisation || 'table') == "table" ? {
                assertions: {
                    uniqueKey: [primaryKey],
                    nonNull: ["last_streamed_at", primaryKey]
                }
            } : {}),
            bigquery: {
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase(),
                    entitytabletype: "latest"
                },
                ...((tableSchema.materialisation || 'table') == "table" ? {
                    clusterBy: [primaryKey]
                } : {})
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte'],
            description: "[AIRBYTE] Latest version of " + tableSchema.entityTableName + ". Sourced from Airbyte raw table in the " + params.airbyteConfig.datasetName + " dataset. " + (tableSchema.description || ''),
            columns: {
                last_streamed_occurred_at: "Timestamp of the last Airbyte extraction for this entity.",
                [primaryKey]: {
                    description: `Primary key of the ${tableSchema.entityTableName} entity.`,
                    bigqueryPolicyTags: tableSchema.hidePrimaryKey && params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                },
                created_at: "Timestamp this entity was first saved in the database.",
                updated_at: "Timestamp this entity was last updated in the database."
            }
        }).query(ctx => `
SELECT
    * EXCEPT(_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, _airbyte_generation_id, _ab_cdc_lsn, _ab_cdc_deleted_at, _ab_cdc_updated_at),
    _airbyte_extracted_at AS last_streamed_occurred_at
FROM ${sourceTable}
WHERE ${primaryKey} IS NOT NULL
    AND _ab_cdc_deleted_at IS NULL
`)
    });
};
