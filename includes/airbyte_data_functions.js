function getCastExpression(fieldName, dataType, isArray) {
    const safeFieldName = `\`${fieldName}\``;

    switch (dataType) {
        case 'boolean':
            return `SAFE_CAST(${safeFieldName} AS BOOL)`;
        case 'integer':
            return `SAFE_CAST(${safeFieldName} AS INT64)`;
        case 'float':
            return `SAFE_CAST(${safeFieldName} AS FLOAT64)`;
        case 'date':
            return `SAFE_CAST(${safeFieldName} AS DATE)`;
        case 'timestamp':
            return `SAFE_CAST(${safeFieldName} AS TIMESTAMP)`;
        case 'json':
            return `SAFE.PARSE_JSON(CAST(${safeFieldName} AS STRING))`;
        case 'integer_array':
            return `ARRAY(SELECT SAFE_CAST(x AS INT64) FROM UNNEST(SPLIT(CAST(${safeFieldName} AS STRING), ',')) AS x WHERE x != '')`;
        case 'string':
        default:
            return `CAST(${safeFieldName} AS STRING)`;
    }
}

function getSqlType(dataType) {
    switch (dataType) {
        case 'boolean':
            return 'BOOL';
        case 'integer':
            return 'INT64';
        case 'float':
            return 'FLOAT64';
        case 'date':
            return 'DATE';
        case 'timestamp':
            return 'TIMESTAMP';
        case 'json':
            return 'JSON';
        default:
            return 'STRING';
    }
}

module.exports = {
    getCastExpression,
    getSqlType
};
