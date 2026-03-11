module.exports = (params) => {
    if (!params.enableAirbyteSource || !params.airbyteEnableAssertions) return null;
    
    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';
    const assertions = [];
    
    // 1. Global data freshness
    assertions.push(
        assert(`${params.eventSourceName}${suffix}_data_not_fresh`, {
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
        `)
    );
    
    // 2. Per-entity assertions
    params.dataSchema.forEach(entitySchema => {
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${entitySchema.entityTableName}\``;
        const latestTable = `${entitySchema.entityTableName}_latest_${params.eventSourceName}${suffix}`;
        
        // Null PK check
        assertions.push(
            assert(`${entitySchema.entityTableName}_${params.eventSourceName}${suffix}_null_pks`, {
                tags: [params.eventSourceName.toLowerCase(), 'airbyte', entitySchema.entityTableName, 'integrity']
            }).query(ctx => `
                SELECT COUNT(*) AS null_count
                FROM ${sourceTable}
                WHERE ${primaryKey} IS NULL
                HAVING COUNT(*) > 0
            `)
        );
    }
}