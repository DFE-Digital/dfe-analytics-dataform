/* Generates {entity}_version_{source}{suffix} tables from Airbyte raw data */

const airbyteDataFunctions = require('./airbyte_data_functions');

module.exports = (params) => {
    if (!params.enableAirbyteSource || !params.airbyteEnableVersioning) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';
    const useContentHash = params.airbyteConfig.changeDetectionStrategy === 'content_hash';

    return params.dataSchema.map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${params.airbyteConfig.tablePrefix}${entitySchema.entityTableName}\``;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';

        // Build column expressions
        const selectColumns = entitySchema.keys.map(key => {
            const castExpr = airbyteDataFunctions.getCastExpression(key.keyName, key.dataType, key.isArray);
            const alias = key.alias || key.keyName;
            return `    ${castExpr} AS \`${alias}\``;
        }).join(',\n');

        // Build content hash expression
        const hashColumns = entitySchema.keys.map(key =>
            `COALESCE(CAST(\`${key.keyName}\` AS STRING), '')`
        ).join(', ');
        const contentHashExpr = `TO_HEX(MD5(CONCAT(${hashColumns})))`;

        return publish(tableName, {
            type: "incremental",
            schema: params.bqDatasetName,
            protected: true,
            uniqueKey: [primaryKey, "valid_from"],
            description: `[AIRBYTE] Version history of ${entitySchema.entityTableName} entities. ${useContentHash ? 'Uses content hashing.' : ''} ${entitySchema.description || ''}`,
            columns: {
                [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity`,
                valid_from: "Timestamp from which this version was valid",
                valid_to: "Timestamp until which this version was valid. NULL = current",
                is_current: "TRUE if this is the current version",
                version_number: "Sequential version number (1 = first)",
                content_hash: "MD5 hash of data columns for change detection",
                ...Object.fromEntries(
                    entitySchema.keys.map(k => [k.alias || k.keyName, k.description || ''])
                )
            },
            bigquery: {
                partitionBy: "DATE(valid_from)",
                clusterBy: [primaryKey],
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcetype: 'airbyte',
                    entitytype: 'version'
                }
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte', entitySchema.entityTableName],
            assertions: {
                uniqueKey: [
                    [primaryKey, "valid_from"]
                ],
                nonNull: [primaryKey, "valid_from"],
                rowConditions: ['valid_from <= valid_to OR valid_to IS NULL']
            }
        }).query(ctx => `
WITH source_data AS (
    SELECT
        CAST(${primaryKey} AS STRING) AS ${primaryKey},
        ${selectColumns},
        CAST(_airbyte_extracted_at AS TIMESTAMP) AS _airbyte_extracted_at,
        CAST(_airbyte_raw_id AS STRING) AS _airbyte_raw_id,
        ${contentHashExpr} AS content_hash
    FROM ${sourceTable}
    WHERE ${primaryKey} IS NOT NULL
    ${ctx.incremental() ? `
    AND _airbyte_extracted_at > (
        SELECT COALESCE(MAX(_airbyte_extracted_at), TIMESTAMP('2000-01-01')) 
        FROM ${ctx.self()}
    )` : ''}
),

existing_versions AS (
    ${ctx.incremental() ? `
    SELECT * FROM ${ctx.self()}
    WHERE ${primaryKey} IN (SELECT DISTINCT ${primaryKey} FROM source_data)
    ` : `
    SELECT 
        CAST(NULL AS STRING) AS ${primaryKey},
        ${entitySchema.keys.map(k => `CAST(NULL AS ${airbyteDataFunctions.getSqlType(k.dataType)}) AS \`${k.alias || k.keyName}\``).join(',\n        ')},
        CAST(NULL AS TIMESTAMP) AS _airbyte_extracted_at,
        CAST(NULL AS STRING) AS _airbyte_raw_id,
        CAST(NULL AS STRING) AS content_hash,
        CAST(NULL AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        CAST(NULL AS BOOL) AS is_current,
        CAST(NULL AS INT64) AS version_number
    FROM source_data
    WHERE FALSE
    `}
),

all_records AS (
    SELECT
        ${primaryKey},
${entitySchema.keys.map(k => `        \`${k.alias || k.keyName}\``).join(',\n')},
        _airbyte_extracted_at,
        _airbyte_raw_id,
        content_hash
    FROM existing_versions
    WHERE content_hash IS NOT NULL
    
    UNION ALL
    
    SELECT
        ${primaryKey},
${entitySchema.keys.map(k => `        \`${k.alias || k.keyName}\``).join(',\n')},
        _airbyte_extracted_at,
        _airbyte_raw_id,
        content_hash
    FROM source_data
),

${useContentHash ? `
deduplicated AS (
    SELECT *
    FROM all_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ${primaryKey}, content_hash 
        ORDER BY _airbyte_extracted_at ASC
    ) = 1
),
` : `
deduplicated AS (
    SELECT *
    FROM all_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ${primaryKey}, _airbyte_extracted_at 
        ORDER BY _airbyte_raw_id
    ) = 1
),
`}

versioned AS (
    SELECT
        ${primaryKey},
${entitySchema.keys.map(k => `        \`${k.alias || k.keyName}\``).join(',\n')},
        _airbyte_extracted_at,
        content_hash,
        _airbyte_extracted_at AS valid_from,
        LEAD(_airbyte_extracted_at) OVER (
            PARTITION BY ${primaryKey} 
            ORDER BY _airbyte_extracted_at
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY ${primaryKey} 
            ORDER BY _airbyte_extracted_at DESC
        ) = 1 AS is_current,
        ROW_NUMBER() OVER (
            PARTITION BY ${primaryKey} 
            ORDER BY _airbyte_extracted_at ASC
        ) AS version_number
    FROM deduplicated
)

SELECT
    ${primaryKey},
    valid_from,
    valid_to,
    is_current,
    version_number,
    content_hash,
${entitySchema.keys.map(k => `    \`${k.alias || k.keyName}\``).join(',\n')},
    _airbyte_extracted_at
FROM versioned
`);
    });
};
