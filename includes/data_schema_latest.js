module.exports = (params) => {
  return publish(params.tableSuffix + "_data_schema_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "",
    columns: {
      valid_from: "Timestamp from which this version of this entity started to be valid."
    }
  }).query(ctx => `SELECT
  "dataSchema: [" || STRING_AGG(tableSchemaJSON, ",\n") || "]" AS dataSchemaJSON
FROM (
  SELECT
    entity_table_name,
    "{\n   entityTableName: \"" || entity_table_name || "\",\n   description: \"\",\n   keys: [" || STRING_AGG("{\n      keyName: \"" || key || "\",\n      dataType: \"string\",\n      description: \"\"\n   }", ", ") || "]\n}" AS tableSchemaJSON
  FROM (
    SELECT
      DISTINCT entity_table_name,
      DATA.key AS key
    FROM
      ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)},
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
  GROUP BY
    entity_table_name)`)
}
