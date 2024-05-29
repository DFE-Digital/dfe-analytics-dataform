module.exports = (params) => {
  return assert(params.eventSourceName + "_hidden_pii_configuration_does_not_match_events_streamed", {
    ...params.defaultConfig,
    type: "assertion",
    description: "Counts the number of entities updated yesterday which were either in the hidden_data field but not configured to be hidden in the dfe-analytics-dataform dataSchema, or vice versa. If this assertion fails, either change the dataSchema to hide / unhide the field(s) as appropriate, or ask a developer to change dfe-analytics configuration to hide / unhide the field(s) in streamed data."
  }).tags([params.eventSourceName.toLowerCase()]).query(ctx => `
WITH expected_entity_fields AS (
  SELECT DISTINCT
    entity_name,
    this_key.key AS key_configured,
    this_key.configured_to_be_hidden_in_data_schema AS configured_to_be_hidden_in_data_schema
  FROM
  UNNEST([
      ${params.dataSchema.map(tableSchema => {
    return `STRUCT("${tableSchema.entityTableName}" AS entity_name,
        [
            ${tableSchema.keys.filter(key => !(key.historic || (key.keyName == tableSchema.primaryKey))).map(key => { return `STRUCT("${key.keyName}" AS key, ${key.hidden || 'false'} AS configured_to_be_hidden_in_data_schema)`; }).join(', ')},
            STRUCT("${tableSchema.primaryKey || 'id'}" AS key, ${tableSchema.hidePrimaryKey || 'false'} AS configured_to_be_hidden_in_data_schema),
            STRUCT("created_at" AS key, ${tableSchema.hideCreatedAt || 'false'} AS configured_to_be_hidden_in_data_schema),
            STRUCT("updated_at" AS key, ${tableSchema.hideUpdatedAt || 'false'} AS configured_to_be_hidden_in_data_schema)
        ] AS keys
    )`;
  }
  ).join(',')}  
  ]), UNNEST(keys) AS this_key
)
SELECT
  entity_name,
  key_configured,
  configured_to_be_hidden_in_data_schema,
  COUNT(
    IF(
      ${data_functions.keyIsInEventData("data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS updates_made_yesterday_with_this_key_not_hidden,
  MIN(
    IF(
      ${data_functions.keyIsInEventData("data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS first_update_yesterday_with_this_key_not_hidden_at,
  MAX(
    IF(
      ${data_functions.keyIsInEventData("data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS last_update_yesterday_with_this_key_not_hidden_at,
  COUNT(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS updates_made_yesterday_with_this_key_hidden,
  MIN(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS first_update_yesterday_with_this_key_hidden_at,
  MAX(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS last_update_yesterday_with_this_key_hidden_at

FROM
  ${ctx.ref("events_" + params.eventSourceName)}
  JOIN expected_entity_fields ON entity_name = entity_table_name
WHERE
  DATE(occurred_at) >= CURRENT_DATE - 1
  AND event_type IN ("create_entity", "update_entity", "import_entity")
GROUP BY
  entity_name,
  key_configured,
  configured_to_be_hidden_in_data_schema
HAVING
  (updates_made_yesterday_with_this_key_hidden > 0 AND configured_to_be_hidden_in_data_schema IS FALSE)
  OR (updates_made_yesterday_with_this_key_not_hidden > 0 AND configured_to_be_hidden_in_data_schema IS TRUE)
ORDER BY
  entity_name,
  key_configured,
  configured_to_be_hidden_in_data_schema`)
}
