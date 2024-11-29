module.exports = (params) => {
  const customEventAssertions = params.customEventSchema.length > 0 && (params.customEventSchema.some(customEvent => customEvent.keys.length > 0)) ?
    ["hidden_pii_configuration_does_not_match_custom_events_streamed_yesterday",
    "hidden_pii_configuration_does_not_match_sample_of_historic_custom_events_streamed"
    ] : [];
  const entityEventAssertions = params.transformEntityEvents ?
    ["hidden_pii_configuration_does_not_match_entity_events_streamed_yesterday",
  "hidden_pii_configuration_does_not_match_sample_of_historic_entity_events_streamed",
    ] : [];
  return [...entityEventAssertions, ...customEventAssertions]
  .forEach(assertionNamePart => {assert(params.eventSourceName + "_" + assertionNamePart, {
    ...params.defaultConfig,
    type: "assertion",
    description: `Counts the number of ${assertionNamePart.includes('entity') ? `entities` : `custom events`} updated ${assertionNamePart.includes('yesterday') ? `yesterday` : `in a sample representing 1% of historic data`} which were either in the hidden_data field but not configured to be hidden in the dfe-analytics-dataform dataSchema, or vice versa. If this assertion fails, either change the ${assertionNamePart.includes('entity') ? `dataSchema` : `customEventSchema`} to hide / unhide the field(s) as appropriate, or ask a developer to change dfe-analytics configuration to hide / unhide the field(s) in streamed data. You may also need to update past events to move key-value pairs from data to hidden_data or vice versa as appropriate.`
  }).tags([params.eventSourceName.toLowerCase()]).query(ctx => `
WITH expected_fields AS (
  SELECT DISTINCT
    ${assertionNamePart.includes('entity') ? `entity_name` : `event_type`},
    this_key.key AS key_configured,
    this_key.configured_to_be_hidden_in_schema AS configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema
  FROM
  UNNEST([
      ${(assertionNamePart.includes('entity') ? params.dataSchema : params.customEventSchema).map(schema => {
    return `STRUCT("${assertionNamePart.includes('entity') ? schema.entityTableName : schema.eventType}" AS ${assertionNamePart.includes('entity') ? `entity_name` : `event_type`},
        [
            ${schema.keys.filter(key => !(key.historic || (key.keyName == schema.primaryKey))).map(key => { return `STRUCT("${key.keyName}" AS key, ${key.hidden || 'false'} AS configured_to_be_hidden_in_schema)`; }).join(', ')}
            ${assertionNamePart.includes('entity') ? `, STRUCT("${schema.primaryKey || 'id'}" AS key, ${schema.hidePrimaryKey || 'false'} AS configured_to_be_hidden_in_schema)` : ``}
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
    ${assertionNamePart.includes('entity') ? `entity_table_name` : `event_type`},
    data,
    hidden_data
  FROM
    ${ctx.ref("events_" + params.eventSourceName)} ${assertionNamePart.includes('historic') ? `TABLESAMPLE SYSTEM ( 1 PERCENT )` : ``}
  WHERE
    event_type IN (
      ${
        assertionNamePart.includes('entity') ? `"create_entity", "update_entity", "import_entity"`
        : params.customEventSchema.map(schema => {return `"${schema.eventType}"`}).join(', ')
        }
      )
    ${assertionNamePart.includes('yesterday') ? `AND DATE(occurred_at) >= CURRENT_DATE - 1` : `AND DATE(occurred_at) < CURRENT_DATE - 1`}
    )
SELECT
  expected_fields.${assertionNamePart.includes('entity') ? `entity_name` : `event_type`},
  key_configured,
  configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema,
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
  JOIN expected_fields ON expected_fields.${assertionNamePart.includes('entity') ? `entity_name` : `event_type`} = events_to_test.${assertionNamePart.includes('entity') ? `entity_table_name` : `event_type`}
GROUP BY
  expected_fields.${assertionNamePart.includes('entity') ? `entity_name` : `event_type`},
  key_configured,
  configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema
HAVING
  (updates_made_with_this_key_hidden > 0 AND configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema IS FALSE)
  OR (updates_made_with_this_key_not_hidden > 0 AND configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema IS TRUE)
  ${assertionNamePart.includes('yesterday') ? `
    /* Don't fail if a key has started being hidden in streamed data, with <60s overlap period, and the schema is configured correctly */
    AND (NOT (
        configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema IS TRUE
        AND TIMESTAMP_DIFF(first_update_with_this_key_hidden_at, last_update_with_this_key_not_hidden_at, SECOND) BETWEEN 0 AND 60
        AND first_update_with_this_key_not_hidden_at <= last_update_with_this_key_not_hidden_at
        AND first_update_with_this_key_hidden_at <= last_update_with_this_key_hidden_at
        ))
    /* Don't fail if a key has stopped being hidden in streamed data, with <60s overlap period, and the schema is configured correctly */
    AND (NOT (
        configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema IS FALSE
        AND TIMESTAMP_DIFF(first_update_with_this_key_not_hidden_at, last_update_with_this_key_hidden_at, SECOND) BETWEEN 0 AND 60
        AND first_update_with_this_key_not_hidden_at <= last_update_with_this_key_not_hidden_at
        AND first_update_with_this_key_hidden_at <= last_update_with_this_key_hidden_at
        ))` : ``}
ORDER BY
  expected_fields.${assertionNamePart.includes('entity') ? `entity_name` : `event_type`},
  key_configured,
  configured_to_be_hidden_in_${assertionNamePart.includes('entity') ? `data` : `custom_event`}_schema`)})
}
