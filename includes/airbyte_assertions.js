module.exports = (params) => {
    if (!params.enableAirbyteSource || !params.airbyteEnableAssertions) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';

    // 1. Global data freshness
    assert(`${params.eventSourceName}${suffix}_data_not_fresh`, {
        ...params.defaultConfig,
        tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'freshness'],
        description: `Fails if no Airbyte data extracted in last ${params.eventsDataFreshnessDays} day(s)`
    }).query(ctx => `
        SELECT 'airbyte_freshness' AS check_name
        FROM (
            ${params.dataSchema.map(e => `
            SELECT MAX(_airbyte_extracted_at) AS last_extraction
            FROM \`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${e.entityTableName}\`
            `).join('\nUNION ALL\n')}
        )
        HAVING MAX(last_extraction) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${params.eventsDataFreshnessDays} DAY)
    `);

}