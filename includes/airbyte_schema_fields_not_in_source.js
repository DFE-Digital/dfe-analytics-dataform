/* This checks for fields defined in the dataSchema that are missing from the actual Airbyte source table columns */

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

    assert(
      tableSchema.entityTableName + "_airbyte_schema_fields_missing_from_source_" + params.eventSourceName, {
        ...params.defaultConfig,
        type: "assertion",
        description: `Checks that all fields defined in the dataSchema configuration for ${tableSchema.entityTableName} exist as columns in the Airbyte source table. ` +
          `If this assertion fails, a field has been removed from the source database table or there is an issue with Airbyte streaming it.`
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
  configured_columns.column_name AS missing_column,
  '${tableSchema.entityTableName}' AS entity_table_name
FROM configured_columns
LEFT JOIN source_columns
  ON source_columns.column_name = configured_columns.column_name
WHERE source_columns.column_name IS NULL
ORDER BY configured_columns.column_name
`)
  })
}