module.exports = (params) => {
  return assert(params.tableSuffix + "_unhandled_field_or_entity_is_being_streamed", {
    ...params.defaultConfig,
    type: "assertion",
    description: "Identifies any entities or field names that are being streamed which don't exist in the dataSchema parameter passed to dfe-analytics-dataform. If this assertion fails as a minimum it means that we either need to (a) if a whole entity is missing, add it to dataSchema or (b) if just a field is missing, add the missing field(s) to dataSchema. Depending on what the additional data identified is, we may also want to update other parts of the pipeline to make this data available."
  }).query(ctx => `SELECT
  entity_table_name,
  key AS key_missing_from_pipeline,
  updates_made_yesterday_with_this_key
FROM
  (
    SELECT
      entity_table_name,
      key,
      COUNT(DISTINCT entity_id) AS updates_made_yesterday_with_this_key
    FROM
      ${ctx.ref(params.tableSuffix + "_entity_version")},
      UNNEST(DATA)
    WHERE
      DATE(valid_from) = CURRENT_DATE - 1
    GROUP BY
      entity_table_name,
      key
  )
  LEFT JOIN ${ctx.ref(params.tableSuffix + "_analytics_yml_latest")} AS analytics_yml_latest 
  ON  entity_table_name = entity_name

WHERE
  key NOT IN UNNEST(analytics_yml_latest.keys)
  AND key NOT IN ("entity_name")
ORDER BY
  entity_table_name,
  key`)
}
