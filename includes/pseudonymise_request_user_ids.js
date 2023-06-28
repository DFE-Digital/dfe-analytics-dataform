module.exports = (params) => {
  return operate(params.eventSourceName + "_pseudonymise_request_user_ids", ctx => [`
CREATE OR REPLACE PROCEDURE
  \`${params.bqProjectName}.${params.bqDatasetName}.pseudonymise_request_user_ids\`(user_table STRING)
BEGIN
BEGIN TRANSACTION;
/* Series of IF... RAISE... statements that prevent the procedure running if it detects that certain unsupported scenarios are the case */
IF NOT EXISTS (
  SELECT
    entity_id
  FROM
    ${ctx.ref(params.eventSourceName + "_entity_version")} AS entity_version
  WHERE
    entity_version.entity_table_name = user_table) THEN RAISE
USING
  MESSAGE = "Data from user_table does not appear to have been streamed by dfe-analytics.";
END IF
  ;
IF
  (
  /* Detect whether entity_id appears to be SHA256-formatted (i.e. containing only digits and lower case letters, and at least one of each) */
  SELECT
    LOGICAL_AND((REGEXP_CONTAINS(entity_id, "[0-9]")
        AND REGEXP_CONTAINS(entity_id, "[a-z]")
        AND REGEXP_CONTAINS(entity_id, "^[0-9a-z]*$")))
  FROM
    ${ctx.ref(params.eventSourceName + "_entity_version")} AS entity_version
  WHERE
    entity_version.entity_table_name = user_table) IS NOT TRUE THEN RAISE
USING
  MESSAGE = "Some versions of the table specified in your user_table parameter had non-pseudonymised UIDs. Consider (1) modifying your dfe-analytics analytics_pii.yml file to pseudonymise the ID field for this table; (2) deleting all events with event_type create_entity, update_entity, delete_entity, import_entity from your events table; (3) running a dfe-analytics backfill on this table and then (4) a full refresh on your Dataform pipeline before attempting to run this stored procedure again.";
END IF
  ;
IF
  (
  /* Detect whether request_user_id appears to be SHA256-formatted (i.e. containing only digits and lower case letters, and at least one of each) */
  SELECT
    LOGICAL_AND((REGEXP_CONTAINS(request_user_id, "[0-9]")
        AND REGEXP_CONTAINS(request_user_id, "[a-z]")
        AND REGEXP_CONTAINS(request_user_id, "^[0-9a-z]*$")))
  FROM
    ${ctx.ref("events_" + params.eventSourceName)} AS event
  LEFT JOIN
    ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)} dad_config
  ON
    /* Check whether dfe-analytics was configured to pseudonymise events at the time this particular event was streamed */
    event.occurred_at >= dad_config.valid_from
    AND (event.occurred_at < dad_config.valid_to
      OR dad_config.valid_to IS NULL)
  WHERE
    dad_config.pseudonymise_web_request_user_id) THEN RAISE
USING
  MESSAGE = "dfe-analytics configuration stated that pseudonymisation was enabled but one or more user_ids did not appear to be in SHA256 format";
END IF
  ;
IF NOT (
  SELECT
    pseudonymise_web_request_user_id
  FROM
    ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)}
  WHERE
    valid_to IS NULL) THEN RAISE
USING
  MESSAGE = "user_id is not currently configured to be pseudonymised in your dfe-analytics configuration. Configure it before running this stored procedure.";
END IF
  ;
/* Pseudonymise the user_id for events in the source events table that happened during a period when dfe-analytics was configured not to pseudonymise user_ids */
UPDATE
  ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)} AS event_to_update
SET
  user_id = TO_HEX(SHA256(user_id))
FROM
  ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)} AS dad_config
WHERE
  event_to_update.occurred_at >= dad_config.valid_from
  AND (event_to_update.occurred_at < dad_config.valid_to
    OR dad_config.valid_to IS NULL)
  AND NOT pseudonymise_web_request_user_id ;
/* Update initialise_analytics events such that it looks like dfe-analytics was configured to pseudonymise user_ids even though it wasn't - this is a failsafe to prevent this procedure accidentally being used to double-pseudonymise user_id if run a second time */
UPDATE
  ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)} AS event_to_update
SET
  DATA = ${data_functions.eventDataCreateOrReplace("DATA", "config", '{"pseudonymise_web_request_user_id":true}')}
WHERE
  event_type="initialise_analytics"
  AND ${data_functions.eventDataExtract("DATA", "config")} = '{"pseudonymise_web_request_user_id":false}' ;
COMMIT TRANSACTION;
END`]) }