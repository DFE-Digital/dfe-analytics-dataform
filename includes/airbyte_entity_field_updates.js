/* Generates {entity}_field_updates_{source}{suffix} tables; Tracks individual field-level changes between versions. */

module.exports = (params) => {
    if (!params.enableAirbyteSource || !params.airbyteEnableFieldUpdates) return null;

    const suffix = params.airbyteConfig.outputSuffix || '_airbyte';

    return params.dataSchema.filter(e => e.trackFieldUpdates !== false).map(entitySchema => {
        const tableName = `${entitySchema.entityTableName}_field_updates_${params.eventSourceName}${suffix}`;
        const versionTableName = `${entitySchema.entityTableName}_version_${params.eventSourceName}${suffix}`;
        const primaryKey = entitySchema.primaryKey || params.airbyteConfig.primaryKeyField || 'id';

        const trackableKeys = entitySchema.keys.filter(k => k.trackChanges !== false);

        if (trackableKeys.length === 0) return null;

        return publish(tableName, {
            type: "table",
            schema: params.bqDatasetName,
            description: `[AIRBYTE] Field-level change history for ${entitySchema.entityTableName}. Each row = one field change.`,
            columns: {
                [primaryKey]: `Primary key of the ${entitySchema.entityTableName} entity`,
                field_name: "Name of the field that changed",
                previous_value: "Value before the change (as string)",
                new_value: "Value after the change (as string)",
                changed_at: "Timestamp when change was detected",
                from_version: "Version number before change",
                to_version: "Version number after change"
            },
            bigquery: {
                partitionBy: "DATE(changed_at)",
                clusterBy: [primaryKey, "field_name"],
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcetype: 'airbyte',
                    entitytype: 'field_updates'
                }
            },
            tags: [params.eventSourceName.toLowerCase(), 'airbyte', entitySchema.entityTableName]
        }).query(ctx => `
WITH version_pairs AS (
    SELECT
        curr.${primaryKey},
        curr.valid_from AS changed_at,
        prev.version_number AS from_version,
        curr.version_number AS to_version,
        ${trackableKeys.map(key => {
            const fieldName = key.alias || key.keyName;
            return `
        CAST(prev.\`${fieldName}\` AS STRING) AS prev_${key.keyName},
        CAST(curr.\`${fieldName}\` AS STRING) AS curr_${key.keyName}`;
        }).join(',')}
    FROM ${ctx.ref(versionTableName)} curr
    INNER JOIN ${ctx.ref(versionTableName)} prev
        ON curr.${primaryKey} = prev.${primaryKey}
        AND curr.version_number = prev.version_number + 1
)

${trackableKeys.map((key, idx) => {
    const fieldName = key.alias || key.keyName;
    return `
${idx > 0 ? 'UNION ALL' : ''}
SELECT
    ${primaryKey},
    '${fieldName}' AS field_name,
    prev_${key.keyName} AS previous_value,
    curr_${key.keyName} AS new_value,
    changed_at,
    from_version,
    to_version
FROM version_pairs
WHERE prev_${key.keyName} IS DISTINCT FROM curr_${key.keyName}`;
}).join('\n')}

ORDER BY ${primaryKey}, changed_at, field_name
`);
    }).filter(x => x !== null);
};
