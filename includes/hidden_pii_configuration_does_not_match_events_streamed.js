module.exports = (params) => {
  return ["hidden_pii_configuration_does_not_match_events_streamed_yesterday", "hidden_pii_configuration_does_not_match_sample_of_historic_events_streamed"].forEach(assertionNamePart => {assert(params.eventSourceName + "_" + assertionNamePart, {
    ...params.defaultConfig,
    type: "assertion",
    description: `Counts the number of entities updated ${assertionNamePart == 'hidden_pii_configuration_does_not_match_events_streamed_yesterday' ? `yesterday` : `in a sample representing 1% of historic data`} which were either in the hidden_data field but not configured to be hidden in the dfe-analytics-dataform dataSchema, or vice versa. If this assertion fails, either change the dataSchema to hide / unhide the field(s) as appropriate, or ask a developer to change dfe-analytics configuration to hide / unhide the field(s) in streamed data. You may also need to update past entity events to move key-value pairs from data to hidden_data or vice versa as appropriate.`
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
            STRUCT("${tableSchema.primaryKey || 'id'}" AS key, ${tableSchema.hidePrimaryKey || 'false'} AS configured_to_be_hidden_in_data_schema)
        ] AS keys
    )`;
  }
  ).join(',')}  
  ]), UNNEST(keys) AS this_key
),
events_to_test AS (
  /* Test all entity events from today and yesterday on one assertion, plus a small sample of all other past events in the other assertion */
  SELECT
    occurred_at,
    entity_table_name,
    data,
    hidden_data
  FROM
    ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"} ${assertionNamePart == 'hidden_pii_configuration_does_not_match_sample_of_historic_events_streamed' ? `TABLESAMPLE SYSTEM ( 1 PERCENT )` : ``}
  WHERE
    event_type IN ("create_entity", "update_entity", "import_entity")
    ${assertionNamePart == 'hidden_pii_configuration_does_not_match_events_streamed_yesterday' ? `AND DATE(occurred_at) >= CURRENT_DATE - 1` : `AND DATE(occurred_at) < CURRENT_DATE - 1`}
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
  ) AS updates_made_with_this_key_not_hidden,
  MIN(
    IF(
      ${data_functions.keyIsInEventData("data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS first_update_with_this_key_not_hidden_at,
  MAX(
    IF(
      ${data_functions.keyIsInEventData("data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS last_update_with_this_key_not_hidden_at,
  COUNT(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS updates_made_with_this_key_hidden,
  MIN(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS first_update_with_this_key_hidden_at,
  MAX(
    IF(
      ${data_functions.keyIsInEventData("hidden_data", "key_configured", true)},
      occurred_at,
      NULL
    )
  ) AS last_update_with_this_key_hidden_at

FROM
  events_to_test
  JOIN expected_entity_fields ON entity_name = entity_table_name
GROUP BY
  entity_name,
  key_configured,
  configured_to_be_hidden_in_data_schema
HAVING
  (updates_made_with_this_key_hidden > 0 AND configured_to_be_hidden_in_data_schema IS FALSE)
  OR (updates_made_with_this_key_not_hidden > 0 AND configured_to_be_hidden_in_data_schema IS TRUE)
  ${assertionNamePart == 'hidden_pii_configuration_does_not_match_events_streamed_yesterday' ? `
    /* Don't fail if a key has started being hidden in streamed data, with <60s overlap period, and the dataSchema is configured correctly */
    AND (NOT (
        configured_to_be_hidden_in_data_schema IS TRUE
        AND TIMESTAMP_DIFF(first_update_with_this_key_hidden_at, last_update_with_this_key_not_hidden_at, SECOND) BETWEEN 0 AND 60
        AND first_update_with_this_key_not_hidden_at <= last_update_with_this_key_not_hidden_at
        AND first_update_with_this_key_hidden_at <= last_update_with_this_key_hidden_at
        ))
    /* Don't fail if a key has stopped being hidden in streamed data, with <60s overlap period, and the dataSchema is configured correctly */
    AND (NOT (
        configured_to_be_hidden_in_data_schema IS FALSE
        AND TIMESTAMP_DIFF(first_update_with_this_key_not_hidden_at, last_update_with_this_key_hidden_at, SECOND) BETWEEN 0 AND 60
        AND first_update_with_this_key_not_hidden_at <= last_update_with_this_key_not_hidden_at
        AND first_update_with_this_key_hidden_at <= last_update_with_this_key_hidden_at
        ))` : ``}
ORDER BY
  entity_name,
  key_configured,
  configured_to_be_hidden_in_data_schema`)})
}
