module.exports = (params) => {
  return publish(params.eventSourceName + "_data_schema_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "Generates a blank version of the dataSchema JSON that needs to be set in dfe_analytics_dataform.js in order for the dfe-analytics-dataform package to denormalise entity CRUD tables according to this schema. This is populated based on the entity CRUD events that were streamed yesterday, and the field names and data types within them. By default all metadata is set to an empty string.",
    columns: {
      dataSchemaJSON: "Blank version of the dataSchema JSON that needs to be set in dfe_analytics_dataform.js in order for the dfe-analytics-dataform package to denormalise entity CRUD tables according to this schema. This is populated based on the entity CRUD events that were streamed yesterday, and the field names and data types within them. By default all metadata is set to an empty string."
    },
    bigquery: {
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    tags: [params.eventSourceName.toLowerCase()]
  }).query(ctx => `WITH keys_with_data_type AS
(
  SELECT
    DISTINCT entity_table_name,
    DATA.key AS key,
  CASE
    /* Attempt to work out the data type(s) of each field, in this priority order */
    WHEN LOGICAL_AND(this_value IS NULL) THEN NULL
    WHEN LOGICAL_AND(JSON_TYPE(SAFE.PARSE_JSON(this_value)) = "boolean") THEN "boolean"
    WHEN LOGICAL_AND(SAFE_CAST(this_value AS INT64) IS NOT NULL) THEN "integer"
    WHEN LOGICAL_AND(SAFE_CAST(this_value AS FLOAT64) IS NOT NULL) THEN "float"
    WHEN LOGICAL_AND(${data_functions.stringToTimestamp(`this_value`)} IS NOT NULL) THEN "timestamp"
    WHEN LOGICAL_AND(${data_functions.stringToDate(`this_value`)} IS NOT NULL) THEN "date"
    WHEN LOGICAL_AND(REGEXP_CONTAINS(this_value, r"^\\[?(?:[0-9]+,)*[0-9]+\\]?$")) THEN "integer_array"
    ELSE "string"
  END AS data_type,
  LOGICAL_OR(ARRAY_LENGTH(value) > 1) AS is_array
  FROM
    ${ctx.ref("events_" + params.eventSourceName)},
    UNNEST(DATA) AS DATA,
    UNNEST(value) AS this_value
  WHERE
    DATE(occurred_at) = CURRENT_DATE - 1
    AND event_type IN ("create_entity",
      "update_entity",
      "delete_entity",
      "import_entity")
    AND key NOT IN ("created_at","updated_at", "id")
  GROUP BY
    entity_table_name,
    key
  ORDER BY
    entity_table_name,
    key),
dataschemajson_table_part AS (
  SELECT
    entity_table_name,
    "{\\n   entityTableName: \\"" || entity_table_name || "\\",\\n   description: \\"\\",\\n   keys: [" || STRING_AGG("{\\n      keyName: \\"" || key || "\\",\\n      dataType: \\"" || data_type || "\\",\\n" || IF(is_array,"      isArray: true,\\n","") || "      description: \\"\\"\\n   }", ", ") || "]\\n}" AS tableSchemaJSON
  FROM keys_with_data_type
  GROUP BY
    entity_table_name
)
SELECT
  "dataSchema: [" || STRING_AGG(tableSchemaJSON, ",\\n") || "]" AS dataSchemaJSON
FROM dataschemajson_table_part`)
}
