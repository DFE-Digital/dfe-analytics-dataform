module.exports = (version, params) => {
    if ((!params.enableMonitoring) || dataform.projectConfig.schemaSuffix) {
        /* Don't send pipeline snapshot if monitoring is disabled (enableMonitoring is true by default)
        or if we're in a Dataform development workspace (schemaSuffix is null if we're in a development workspace) */
        return true;
    }
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
  entity_table_check_scheduled_metrics AS (
  ${params.transformEntityEvents ? `
  SELECT
    COUNT(DISTINCT entity_table_name) > 0 AS checksum_enabled,
    COUNT(DISTINCT entity_table_name) AS number_of_tables,
    COUNT(DISTINCT CASE WHEN database_checksum = bigquery_checksum THEN entity_table_name END) AS number_of_tables_with_matching_checksums,
    SUM(database_row_count) AS number_of_rows,
    SUM(CASE WHEN database_row_count > bigquery_row_count THEN database_row_count - bigquery_row_count ELSE 0 END) AS number_of_missing_rows,
    SUM(CASE WHEN database_row_count < bigquery_row_count THEN bigquery_row_count - database_row_count ELSE 0 END) AS number_of_extra_rows
  FROM
    ${ctx.ref("entity_table_check_scheduled_" + params.eventSourceName)} ` : `
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
    valid_to IS NULL )
SELECT
  CURRENT_TIMESTAMP AS workflow_executed_at,
  "${params.bqProjectName}" AS gcp_project_name,
  "${params.eventSourceName}" AS event_source_name,
  "${dataform.projectConfig.defaultSchema + (dataform.projectConfig.schemaSuffix ? "_" + dataform.projectConfig.schemaSuffix : "")}" AS output_dataset_name,
  dfe_analytics_configuration_metrics.*,
  "${version}" AS dfe_analytics_dataform_version,
  entity_table_check_scheduled_metrics.*,
  """${JSON.stringify(params)}""" AS dfe_analytics_dataform_parameters
FROM
  entity_table_check_scheduled_metrics,
  dfe_analytics_configuration_metrics;
EXCEPTION WHEN ERROR THEN
  IF LOWER(@@error.message) LIKE "%access denied%" THEN RAISE USING MESSAGE = "Your Dataform service account does not have the required permissions to send data to the monitoring.pipeline_snapshots table in the cross-teacher-services GCP project. Please ask the Data & Insights team on Slack (#twd_data_insights) to give your Dataform service account the BigQuery Data Editor role on this table.";
  ELSE RAISE;
  END IF;
END;
  `]).tags([params.eventSourceName.toLowerCase()])
}
