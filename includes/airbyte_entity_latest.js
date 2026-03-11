/* Generates {entity}_latest_{source}{suffix} tables from Airbyte raw data; Contains the most recent version of each entity. */

const airbyteDataFunctions = require('./airbyte_data_functions');

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_latest_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';

        // Build column expressions
        const selectColumns = entitySchema.keys.map(key => {
            const castExpr = airbyteDataFunctions.getCastExpression(key.keyName, key.dataType, key.isArray);
            const alias = key.alias || key.keyName;
            return `    ${castExpr} AS \`${alias}\``;
        }).join(',\n');

        // Column documentation
        const columnDocs = {
            [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity`,
            first_seen_at: "Timestamp when this entity was first extracted by Airbyte",
            last_seen_at: "Timestamp when this entity was last extracted by Airbyte",
            ...Object.fromEntries(
                entitySchema.keys.map(k => [k.alias || k.keyName, k.description || ''])
            )
        };

        return publish(tableName, {
            type: entitySchema.materialisation || 'table',
            schema: params.bqDatasetName,
            description: `[AIRBYTE] Latest version of each ${entitySchema.entityTableName} entity. ${entitySchema.description || ''}`,
            columns: columnDocs,
            bigquery: {
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcetype: 'airbyte',
                    entitytype: 'latest'
                },
                ...(entitySchema.materialisation === 'table' ? {
                    clusterBy: [primaryKey]
                } : {})
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte', entitySchema.entityTableName],
            assertions: entitySchema.materialisation === 'table' ? {
                uniqueKey: [primaryKey],
                nonNull: [primaryKey]
            } : {}
        }).query(ctx => `
WITH ranked_records AS (
    SELECT
        ${primaryKey},
${selectColumns},
        _airbyte_extracted_at,
        _airbyte_raw_id,
        ROW_NUMBER() OVER (
            PARTITION BY ${primaryKey} 
            ORDER BY _airbyte_extracted_at DESC
        ) AS _rn
    FROM ${sourceTable}
    WHERE ${primaryKey} IS NOT NULL
),

first_seen AS (
    SELECT
        ${primaryKey},
        MIN(_airbyte_extracted_at) AS first_seen_at
    FROM ${sourceTable}
    WHERE ${primaryKey} IS NOT NULL
    GROUP BY ${primaryKey}
)

SELECT
    r.${primaryKey},
    f.first_seen_at,
    r._airbyte_extracted_at AS last_seen_at,
${entitySchema.keys.map(k => `    r.\`${k.alias || k.keyName}\``).join(',\n')}
FROM ranked_records r
JOIN first_seen f ON r.${primaryKey} = f.${primaryKey}
WHERE r._rn = 1
`);
    });
};
