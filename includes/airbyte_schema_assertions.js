/* Compares columns in each Airbyte source table against the dataSchema configuration.
   Produces two assertions per entity:
   1. Fields in the source table that are NOT in the schema (could indicate unconfigured PII)
   2. Fields in the schema that are NOT in the source table (could indicate removed/missing columns)

   Note: INFORMATION_SCHEMA.COLUMNS in BigQuery can lag up to 10 minutes behind actual table schema changes. */

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;
    if (!params.dataSchema || params.dataSchema.length === 0) return null;

    return params.dataSchema.map(tableSchema => {

        // Build the list of all known/configured keys from the dataSchema
        const configuredKeys = [
            tableSchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id',
            'created_at',
            'updated_at',
            '_airbyte_raw_id',
            '_airbyte_extracted_at',
            '_airbyte_meta',
            '_airbyte_generation_id',
            '_ab_cdc_deleted_at',
            '_ab_cdc_lsn',
            '_ab_cdc_updated_at',
            ...tableSchema.keys.map(key => key.keyName)
        ];

        // Deduplicate
        const uniqueConfiguredKeys = [...new Set(configuredKeys)];

        // Build SQL array of known column names
        const knownColumnsSQL = uniqueConfiguredKeys
            .map(k => `'${k.replace(/'/g, "\\'")}'`)
            .join(', ');

        const sharedCTEs = `
        WITH source_columns AS (
            SELECT column_name
            FROM \`${params.bqProjectName}.${params.airbyteConfig.datasetName}.INFORMATION_SCHEMA.COLUMNS\`
            WHERE table_name = '${params.airbyteConfig.tablePrefix}${tableSchema.entityTableName}'
        ),
        configured_columns AS (
            SELECT column_name
            FROM UNNEST([${knownColumnsSQL}]) AS column_name
        )`;

        // Assertion 1: Source fields not in schema
        const fieldsNotInSchema = assert(
                tableSchema.entityTableName + "_airbyte_fields_not_in_schema_" + params.eventSourceName, {
                    ...params.defaultConfig,
                    type: "assertion",
                    description: `Checks that all columns in the Airbyte source table for ${tableSchema.entityTableName} are defined in the dataSchema configuration. ` +
                        `If this assertion fails, a new column exists in the source database table that has not been configured in the dataSchema. ` +
                        `You MUST add every new field to the dataSchema with the appropriate 'hidden' setting (true for PII, false otherwise) before the pipeline can proceed.`
                }
            ).tags([params.eventSourceName.toLowerCase(), 'airbyte'])
            .query(ctx => `${sharedCTEs}
            SELECT
                '${tableSchema.entityTableName}' AS entity_table_name,
                source_columns.column_name AS unconfigured_column,
            FROM source_columns
            LEFT JOIN configured_columns USING (column_name)
            WHERE configured_columns.column_name IS NULL
            ORDER BY source_columns.column_name
            `);

        // Assertion 2: Schema fields not in source
        const schemaFieldsNotInSource = assert(
                tableSchema.entityTableName + "_airbyte_schema_fields_missing_from_source_" + params.eventSourceName, {
                    ...params.defaultConfig,
                    type: "assertion",
                    description: `Checks that all fields defined in the dataSchema configuration for ${tableSchema.entityTableName} exist as columns in the Airbyte source table. ` +
                        `If this assertion fails, a field has been removed from the source database table or there is an issue with Airbyte streaming it.`
                }
            ).tags([params.eventSourceName.toLowerCase(), 'airbyte'])
            .query(ctx => `${sharedCTEs}
            SELECT
                '${tableSchema.entityTableName}' AS entity_table_name,
                configured_columns.column_name AS missing_column,
            FROM configured_columns
            LEFT JOIN source_columns USING (column_name)
            WHERE source_columns.column_name IS NULL
            ORDER BY configured_columns.column_name
            `);

        return [fieldsNotInSchema, schemaFieldsNotInSource];
    })
}
