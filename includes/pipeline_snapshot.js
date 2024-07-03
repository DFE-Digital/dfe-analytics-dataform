module.exports = (version, params) => {
   // if ((!params.enableMonitoring) || dataform.projectConfig.schemaSuffix) {
        /* Don't send pipeline snapshot if monitoring is disabled (enableMonitoring is true by default)
        or if we're in a Dataform development workspace (schemaSuffix is null if we're in a development workspace) */
    //    return true;
    //}
    return operate("pipeline_snapshot_" + params.eventSourceName, ctx => [`
BEGIN
INSERT \`cross-teacher-services.monitoring.pipeline_snapshots\` (
    workflow_executed_at,
    gcp_project_name,
    event_source_name,
    output_dataset_name,
    dfe_analytics_version,
    dfe_analytics_dataform_version,
    checksum_enabled,
    number_of_tables,
    number_of_tables_with_matching_checksums,
    number_of_rows,
    number_of_missing_rows,
    number_of_extra_rows,
    dfe_analytics_dataform_parameters
    )
WITH
  ${params.transformEntityEvents ? `
  entity_table_check_latest AS (
  SELECT
    *
  FROM
    ${ctx.ref("entity_table_check_scheduled_" + params.eventSourceName)}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY entity_table_name ORDER BY checksum_calculated_at ASC) = 1
  ),
  entity_table_check_scheduled_metrics AS (
  SELECT
    COUNT(DISTINCT entity_table_name) > 0 AS checksum_enabled,
    COUNT(DISTINCT entity_table_name) AS number_of_tables,
    COUNT(DISTINCT CASE WHEN database_checksum = bigquery_checksum THEN entity_table_name END) AS number_of_tables_with_matching_checksums,
    SUM(database_row_count) AS number_of_rows,
    SUM(number_of_missing_rows) AS number_of_missing_rows,
    SUM(number_of_extra_rows) AS number_of_extra_rows,
    SUM(weekly_change_in_number_of_rows) AS weekly_change_in_number_of_rows,
    SUM(weekly_change_in_number_of_missing_rows) AS weekly_change_in_number_of_missing_rows,
    SUM(weekly_change_in_number_of_extra_rows) AS weekly_change_in_number_of_extra_rows,
    MAX(error_rate) AS largest_error_rate_for_any_table,
    MAX_BY(entity_table_name, error_rate) AS table_with_largest_error_rate,
    MAX(twelve_week_projected_error_rate) AS largest_twelve_week_projected_error_rate_for_any_table,
    MAX_BY(entity_table_name, twelve_week_projected_error_rate) AS table_with_largest_twelve_week_projected_error_rate
  FROM
    entity_table_check_latest ` : `
  entity_table_check_scheduled_metrics AS (
    SELECT
    CAST(NULL AS BOOL) AS checksum_enabled,
    CAST(NULL AS INT64) AS number_of_tables,
    CAST(NULL AS INT64) AS number_of_tables_with_matching_checksums,
    CAST(NULL AS INT64) AS number_of_rows,
    CAST(NULL AS INT64) AS number_of_missing_rows,
    CAST(NULL AS INT64) AS number_of_extra_rows
    `}
  ),
  dfe_analytics_configuration_metrics AS (
  SELECT
    MAX_BY(version, valid_from) AS dfe_analytics_version
  FROM
    ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)}
  WHERE
    valid_to IS NULL ),
  events_table_metrics AS (
    SELECT
      LOGICAL_OR(ARRAY_LENGTH(hidden_data) > 0) AS hidden_pii_streamed_within_the_last_week
    FROM
      ${ctx.ref("events_" + params.eventSourceName)}
    WHERE
      DATE(occurred_at) >= CURRENT_DATE - 7
  )
SELECT
  CURRENT_TIMESTAMP AS workflow_executed_at,
  "${params.bqProjectName}" AS gcp_project_name,
  "${params.eventSourceName}" AS event_source_name,
  "${dataform.projectConfig.defaultSchema + (dataform.projectConfig.schemaSuffix ? "_" + dataform.projectConfig.schemaSuffix : "")}" AS output_dataset_name,
  dfe_analytics_configuration_metrics.dfe_analytics_version,
  "${version}" AS dfe_analytics_dataform_version,
  entity_table_check_scheduled_metrics.checksum_enabled,
  entity_table_check_scheduled_metrics.number_of_tables,
  entity_table_check_scheduled_metrics.number_of_tables_with_matching_checksums,
  entity_table_check_scheduled_metrics.number_of_rows,
  entity_table_check_scheduled_metrics.number_of_missing_rows,
  entity_table_check_scheduled_metrics.number_of_extra_rows,
  """${JSON.stringify(params)}""" AS dfe_analytics_dataform_parameters,
  /* New parameters from dfe-analytics-dataform v2.0.0 below */
  events_table_metrics.hidden_pii_streamed_within_the_last_week,
  ${params.hiddenPolicyTagLocation ? "TRUE" : "FALSE"} AS hidden_pii_configured,
  entity_table_check_scheduled_metrics.weekly_change_in_number_of_rows,
  entity_table_check_scheduled_metrics.weekly_change_in_number_of_missing_rows,
  entity_table_check_scheduled_metrics.weekly_change_in_number_of_extra_rows,
  entity_table_check_scheduled_metrics.largest_error_rate_for_any_table,
  entity_table_check_scheduled_metrics.table_with_largest_error_rate,
  entity_table_check_scheduled_metrics.largest_twelve_week_projected_error_rate_for_any_table,
  entity_table_check_scheduled_metrics.table_with_largest_twelve_week_projected_error_rate
FROM
  entity_table_check_scheduled_metrics,
  dfe_analytics_configuration_metrics,
  events_table_metrics;
EXCEPTION WHEN ERROR THEN
  IF LOWER(@@error.message) LIKE "%access denied%" THEN RAISE USING MESSAGE = "Your Dataform service account does not have the required permissions to send data to the monitoring.pipeline_snapshots table in the cross-teacher-services GCP project. Please ask the Data & Insights team on Slack (#twd_data_insights) to give your Dataform service account the BigQuery Data Editor role on this table.";
  ELSE RAISE;
  END IF;
END;
  `]).tags([params.eventSourceName.toLowerCase()])
}
