/* This compares the actual columns in the Airbyte source table against what's defined in the dataSchema */

module.exports = (params) => {
  if (!params.enableAirbyteSource) return null;
  if (!params.dataSchema || params.dataSchema.length === 0) return null;

  return params.dataSchema.map(tableSchema => {

    // Build the list of all known/configured keys from the dataSchema
    const configuredKeys = [
      tableSchema.primaryKey || 'id',
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
      .map(k => `"${k}"`)
      .join(', ');

    assert(
      tableSchema.entityTableName + "_airbyte_pii_fields_not_in_schema_" + params.eventSourceName, {
        ...params.defaultConfig,
        type: "assertion",
        description: `Checks that all columns in the Airbyte source table for ${tableSchema.entityTableName} are defined in the dataSchema configuration. ` +
          `If this assertion fails, a new column exists in the source database table that has not been configured in the dataSchema. ` +
          `You MUST add every new field to the dataSchema with the appropriate 'hidden' setting (true for PII, false otherwise) before the pipeline can proceed. ` +
          `This prevents PII from being processed without explicit classification.`
      }
    ).tags([params.eventSourceName.toLowerCase(), 'airbyte'])
      .query(ctx => `
WITH source_columns AS (
  SELECT column_name
  FROM \`${params.bqProjectName}.${params.airbyteConfig.datasetName}.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = '${params.airbyteConfig.tablePrefix}${tableSchema.entityTableName}'
),
configured_columns AS (
  SELECT column_name
  FROM UNNEST([${knownColumnsSQL}]) AS column_name
)
SELECT
  source_columns.column_name AS unconfigured_column,
  '${tableSchema.entityTableName}' AS entity_table_name
FROM source_columns
LEFT JOIN configured_columns
  ON source_columns.column_name = configured_columns.column_name
WHERE configured_columns.column_name IS NULL
ORDER BY source_columns.column_name
`)
  })
}