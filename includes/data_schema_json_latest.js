module.exports = (params) => {
  return publish(params.eventSourceName + "_data_schema_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "Generates a blank version of the dataSchema JSON that needs to be set in dfe_analytics_dataform.js in order for the dfe-analytics-dataform package to denormalise entity CRUD tables according to this schema. This is populated based on the entity CRUD events that were streamed yesterday, and the field names within them. By default all data types are set to string, and all metadata is set to an empty string.",
    columns: {
      dataSchemaJSON: "Blank version of the dataSchema JSON that needs to be set in dfe_analytics_dataform.js in order for the dfe-analytics-dataform package to denormalise entity CRUD tables according to this schema. This is populated based on the entity CRUD events that were streamed yesterday, and the field names within them. By default all data types are set to string, and all metadata is set to an empty string."
    },
    bigquery: {
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    }
  }).query(ctx => `SELECT
  "dataSchema: [" || STRING_AGG(tableSchemaJSON, ",\\n") || "]" AS dataSchemaJSON
FROM (
  SELECT
    entity_table_name,
    "{\\n   entityTableName: \\"" || entity_table_name || "\\",\\n   description: \\"\\",\\n   keys: [" || STRING_AGG("{\\n      keyName: \\"" || key || "\\",\\n      dataType: \\"" || data_type || "\\",\\n      description: \\"\\"\\n   }", ", ") || "]\\n}" AS tableSchemaJSON
  FROM (
    SELECT
      entity_table_name,
      key,
      /* If we identified only one non-null data type for this field, use that data type - otherwise default to string */
      IF(
          COUNT(DISTINCT data_type) = 1,
          ANY_VALUE(data_type),
          "string"
        ) AS data_type
    FROM (
      SELECT
        DISTINCT entity_table_name,
        DATA.key AS key,
      CASE
        /* Attempt to work out the data type(s) of each field, in this priority order */
        WHEN DATA.value[SAFE_OFFSET(0)] IS NULL THEN NULL
        WHEN SAFE_CAST(DATA.value[SAFE_OFFSET(0)] AS BOOL) IS NOT NULL THEN "boolean"
        WHEN SAFE_CAST(DATA.value[SAFE_OFFSET(0)] AS INT64) IS NOT NULL THEN "integer"
        WHEN SAFE_CAST(DATA.value[SAFE_OFFSET(0)] AS FLOAT64) IS NOT NULL THEN "float"
        WHEN ${data_functions.stringToTimestamp(`DATA.value[SAFE_OFFSET(0)]`)} IS NOT NULL THEN "timestamp"
        WHEN ${data_functions.stringToDate(`DATA.value[SAFE_OFFSET(0)]`)} IS NOT NULL THEN "date"
        WHEN REGEXP_CONTAINS(DATA.value[SAFE_OFFSET(0)], r"^\\[?(?:[0-9]+,)*[0-9]+\\]?$") THEN "integer_array"
        ELSE "string"
      END AS data_type
      FROM
        ${ctx.ref("events_" + params.eventSourceName)},
        UNNEST(DATA) AS DATA
      WHERE
        DATE(occurred_at) = CURRENT_DATE - 1
        AND event_type IN ("create_entity",
          "update_entity",
          "delete_entity",
          "import_entity")
        AND key NOT IN ("created_at","updated_at", "id")
      ORDER BY
        entity_table_name,
        key)
    GROUP BY entity_table_name,key
  )
  GROUP BY
    entity_table_name)`)
}
