/* Checks that all values in Airbyte source columns that have a valueMappings configuration
   are covered by that mapping. Produces one assertion per entity that has at least one
   valueMappings key.

   These assertions are NON-BLOCKING for the version table.

   Behaviour when an unmapped value is found:
   - The value will have passed through the version table as the raw source value.
   - This assertion will fail, signalling that valueMappings config needs updating.
*/

module.exports = (params) => {
    if (!params.enableAirbyteSource) return null;
    if (!params.dataSchema || params.dataSchema.length === 0) return null;

    return params.dataSchema.flatMap(tableSchema => {
        // Only generate an assertion for entities that have at least one valueMappings key
        const mappedKeys = (tableSchema.keys || []).filter(k => k.valueMappings && !k.historic);
        if (mappedKeys.length === 0) return [];

        const sourceTable = `\`${params.bqProjectName}.${params.airbyteConfig.datasetName}.${tableSchema.entityTableName}\``;

        // One UNION ALL branch per mapped key - each branch finds rows whose raw value is not present in the configured mapping.
        const unionBranches = mappedKeys.map(key => {
            const knownValues = Object.keys(key.valueMappings)
                .map(v => `'${v.replace(/'/g, "\\'")}'`)
                .join(', ');

            return `
    SELECT
        '${tableSchema.entityTableName}' AS entity_table_name,
        '${key.keyName}' AS field_name,
        CAST(\`${key.keyName}\` AS STRING) AS unmapped_raw_value,
        COUNT(*) AS row_count
    FROM ${sourceTable}
    WHERE CAST(\`${key.keyName}\` AS STRING) IS NOT NULL
      AND CAST(\`${key.keyName}\` AS STRING) NOT IN (${knownValues})
    GROUP BY unmapped_raw_value`;
        });

        return assert(
            tableSchema.entityTableName + "_airbyte_enum_values_not_in_mapping_" + params.eventSourceName, {
                ...params.defaultConfig,
                type: "assertion",
                description:
                    `Checks that all values in enum fields of the Airbyte source table for ${tableSchema.entityTableName} ` +
                    `are covered by their valueMappings configuration in dataSchema. ` +
                    `If this assertion fails, a raw source value exists that has no mapping — it will appear as the raw value ` +
                    `(e.g. "2") in the version and latest tables rather than a human-readable label. ` +
                    `Update the valueMappings config for the field(s) shown in the results.`
            }
        )
        .tags([params.eventSourceName.toLowerCase(), 'airbyte', 'airbyte_enum'])
        .query(() => unionBranches.join('\n    UNION ALL\n') );
    });
};