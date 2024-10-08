module.exports = (params) => {
  return operate(params.eventSourceName + "_migrate_historic_events_to_current_hidden_pii_configuration", ctx => [`
CREATE OR REPLACE PROCEDURE
  \`${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.defaultSchema + (dataform.projectConfig.schemaSuffix ? "_" + dataform.projectConfig.schemaSuffix : "")}.migrate_historic_${params.eventSourceName}_events_to_current_hidden_pii_configuration\`() OPTIONS(description="Migrates historic events in the source events table and events_${params.eventSourceName} table such that fields in the data and hidden_data array fields are in the array they are currently expected to be in by dfe-analytics-dataform. Fails if any events from the last day are not in the expected array field.")
BEGIN
BEGIN TRANSACTION;
/* Series of IF... RAISE... statements that prevent the procedure running if it detects that certain unsupported scenarios are the case */

IF EXISTS
  (
  /* Detect whether all entity CRUD events from the last day have been in the array fields expected by dfe-analytics-dataform */
  SELECT
    *
  FROM
    ${ctx.resolve(params.eventSourceName + "_hidden_pii_configuration_does_not_match_entity_events_streamed_yesterday")}
  )
  THEN RAISE USING MESSAGE = "Entity create, update, delete and/or import events have been streamed since the beginning of yesterday with data in the wrong field (data or hidden_data). Please ensure the ${params.eventSourceName}_hidden_pii_configuration_does_not_match_entity_events_streamed_yesterday assertion passes before running this procedure.";
END IF;

${params.customEventSchema.some(customEvent => customEvent.keys.length > 0) ? `IF EXISTS
  (
  /* Detect whether all custom events from the last day have been in the array fields expected by dfe-analytics-dataform */
  SELECT
    *
  FROM
    ${ctx.resolve(params.eventSourceName + "_hidden_pii_configuration_does_not_match_custom_events_streamed_yesterday")}
  )
  THEN RAISE USING MESSAGE = "Custom events have been streamed since the beginning of yesterday with data in the wrong field (data or hidden_data). Please ensure the ${params.eventSourceName}_hidden_pii_configuration_does_not_match_custom_events_streamed_yesterday assertion passes before running this procedure.";
END IF;` : ``}

${["`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`", ctx.ref("events_" + params.eventSourceName)].map(eventsTableReference => {return `
UPDATE
  ${eventsTableReference} event
SET
  data = ARRAY(SELECT AS STRUCT data_combined.key, data_combined.value FROM UNNEST(ARRAY_CONCAT(data, hidden_data)) data_combined WHERE data_combined.key IN UNNEST(entity.visible_keys)),
  hidden_data = ARRAY(SELECT AS STRUCT data_combined.key, data_combined.value FROM UNNEST(ARRAY_CONCAT(data, hidden_data)) data_combined WHERE data_combined.key IN UNNEST(entity.hidden_keys) OR key NOT IN UNNEST(entity.visible_keys))
FROM (
  SELECT
    entity_name,
    ARRAY(SELECT DISTINCT key FROM UNNEST(keys) WHERE configured_to_be_hidden_in_data_schema IS TRUE) AS hidden_keys,
    ARRAY(SELECT DISTINCT key FROM UNNEST(keys) WHERE configured_to_be_hidden_in_data_schema IS FALSE) AS visible_keys
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
  ])
) AS entity
WHERE
  event.entity_table_name = entity.entity_name
  AND event.event_type IN ("create_entity", "update_entity", "delete_entity", "import_entity")
  AND DATE(occurred_at) <= CURRENT_DATE - 1 ;
`;}).join('\n')}

${params.customEventSchema.some(customEvent => customEvent.keys.length > 0) ? `
${["`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`", ctx.ref("events_" + params.eventSourceName)].map(eventsTableReference => {return `
UPDATE
  ${eventsTableReference} event
SET
  data = ARRAY(SELECT AS STRUCT data_combined.key, data_combined.value FROM UNNEST(ARRAY_CONCAT(data, hidden_data)) data_combined WHERE data_combined.key IN UNNEST(custom_event.visible_keys)),
  hidden_data = ARRAY(SELECT AS STRUCT data_combined.key, data_combined.value FROM UNNEST(ARRAY_CONCAT(data, hidden_data)) data_combined WHERE data_combined.key IN UNNEST(custom_event.hidden_keys) OR key NOT IN UNNEST(custom_event.visible_keys))
FROM (
  SELECT
    event_type,
    ARRAY(SELECT DISTINCT key FROM UNNEST(keys) WHERE configured_to_be_hidden_in_data_schema IS TRUE) AS hidden_keys,
    ARRAY(SELECT DISTINCT key FROM UNNEST(keys) WHERE configured_to_be_hidden_in_data_schema IS FALSE) AS visible_keys
  FROM
  UNNEST([
      ${params.customEventSchema.map(customEvent => {
    return `STRUCT("${customEvent.eventType}" AS event_type,
        [
            ${customEvent.keys.filter(key => !(key.historic)).map(key => { return `STRUCT("${key.keyName}" AS key, ${key.hidden || 'false'} AS configured_to_be_hidden_in_data_schema)`; }).join(', ')}
        ] AS keys
    )`;
  }
  ).join(',')}  
  ])
) AS custom_event
WHERE
  event.event_type = custom_event.event_type
  AND event.event_type IN (
    ${params.customEventSchema.map(customEvent => {
    return `"${customEvent.eventType}"`}).join(`, `)}
    )
  AND DATE(occurred_at) <= CURRENT_DATE - 1 ;
`;}).join('\n')}` : ``}

COMMIT TRANSACTION;
END`]).tags([params.eventSourceName.toLowerCase()])
}