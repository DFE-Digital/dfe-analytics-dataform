module.exports = (params) => {
  return assert(params.eventSourceName + "_unhandled_field_or_entity_is_being_streamed", {
    ...params.defaultConfig,
    type: "assertion",
    description: "Identifies any entities or field names that are being streamed which don't exist in the dataSchema parameter passed to dfe-analytics-dataform. If this assertion fails as a minimum it means that we either need to (a) if a whole entity is missing, add it to dataSchema or (b) if just a field is missing, add the missing field(s) to dataSchema. Depending on what the additional data identified is, we may also want to update other parts of the pipeline to make this data available."
  }).tags([params.eventSourceName.toLowerCase()]).query(ctx => `
WITH expected_entity_fields AS (
  SELECT DISTINCT
    entity_name,
    keys
  FROM
  UNNEST([
      ${params.dataSchema.map(tableSchema => {
    return `STRUCT("${tableSchema.entityTableName}" AS entity_name,
        [${tableSchema.keys.filter(key => !key.historic).map(key => { return `"${key.keyName}"`; }).join(', ')}, ${tableSchema.primary_key || `"id"`}] AS keys
        )`;
  }
  ).join(',')}  
  ])
)
SELECT
  entity_table_name,
  key AS key_missing_from_pipeline,
  updates_made_yesterday_with_this_key
FROM
  (
    SELECT
      entity_table_name,
      key,
      COUNT(DISTINCT occurred_at) AS updates_made_yesterday_with_this_key
    FROM
      ${ctx.ref("events_" + params.eventSourceName)},
      UNNEST(DATA)
    WHERE
      DATE(occurred_at) >= CURRENT_DATE - 1
      AND event_type IN ("create_entity", "update_entity", "import_entity")
    GROUP BY
      entity_table_name,
      key
  )
  LEFT JOIN expected_entity_fields
  ON entity_table_name = entity_name

WHERE
  key NOT IN UNNEST(expected_entity_fields.keys)
  AND key NOT IN ("entity_name", "created_at", "updated_at")
ORDER BY
  entity_table_name,
  key`)
}
