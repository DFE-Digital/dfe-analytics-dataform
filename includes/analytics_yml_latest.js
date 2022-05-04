module.exports = (params) => {
  return publish(params.tableSuffix + "_analytics_yml_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "Structured version of the latest version of the analytics.yml file which dfe-analytics-dataform has been configured to process entity CRUD events from. This is NOT necessarily the same as the latest version in the production Github repository.",
    columns: {
      entity_name: "Name of the table we want entities to be streamed for",
      keys: "ARRAY of STRINGs listing the names of the fields we want streamed entity events to contain."
    }
  }).query(ctx => `WITH entity_yamls AS (
  SELECT
    *
  FROM
    UNNEST(
      REGEXP_EXTRACT_ALL(${params.analyticsYmlFileLatest}, r"(  [a-z0-9_]+:\n(?:    - [a-z0-9_]+\n)+)")
    ) AS entity_yaml
)
SELECT
  REGEXP_EXTRACT(entity_yaml, "  ([a-z0-9_]+)") AS entity_name,
  REGEXP_EXTRACT_ALL(entity_yaml, "    - ([a-z0-9_]+)") AS keys
FROM
  entity_yamls`)
}
