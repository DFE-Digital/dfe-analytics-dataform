module.exports = (version, params) => {
    // Determine the target table based on the environment (development or production):
    // - In development, the table includes a schema suffix specific to the workspace.
    // - In production, the table uses the standard monitoring schema.

    const isDevelopment = !!dataform.projectConfig.schemaSuffix;

    const targetSchema = isDevelopment
        ? "`" + params.bqProjectName + "." + dataform.projectConfig.defaultSchema + "_" + dataform.projectConfig.schemaSuffix + "`" // dev target schema
        : "`cross-teacher-services.monitoring`"; // production target schema

    const targetTable = isDevelopment
        ? "`" + params.bqProjectName + "." + dataform.projectConfig.defaultSchema + "_" + dataform.projectConfig.schemaSuffix + ".pipeline_table_snapshots`" // dev target table
        : "`cross-teacher-services.monitoring.pipeline_table_snapshots`"; // production target table

     const aggTargetTable = isDevelopment
        ? "`" + params.bqProjectName + "." + dataform.projectConfig.defaultSchema + "_" + dataform.projectConfig.schemaSuffix + ".pipeline_snapshots`" // dev aggregation target table
        : "`cross-teacher-services.monitoring.pipeline_snapshots`"; // production aggregation target table

    if (!params.enableMonitoring) {
        // Don't send pipeline snapshot if monitoring is disabled (enableMonitoring is true by default)
        return true;
    }

    return [
      // Step 1: Create the pipeline monitoring in table-level
      operate("pipeline_table_snapshots_" + params.eventSourceName, ctx => [`

      BEGIN
      CREATE SCHEMA IF NOT EXISTS ${targetSchema};
      
      CREATE TABLE IF NOT EXISTS ${targetTable} (
          workflow_executed_at TIMESTAMP OPTIONS(
            description="The time the Dataform pipeline that took this snapshot of itself was executed"),
          gcp_project_name STRING OPTIONS(
            description="The name of the GCP project this Dataform pipeline was executed within"),
          event_source_name STRING OPTIONS(
            description="The eventSourceName included in the name of each table compiled via this dfeAnalyticsDataform() JS function"),
          output_dataset_name STRING OPTIONS(
            description="The name of the BigQuery dataset this pipeline output transformed tables into"),
          entity_table_name STRING OPTIONS(
            description="Name of the entity table"),
          checksum_calculated_at TIMESTAMP OPTIONS(
            description="Timestamp when checksum was calculated"),
          matching_checksums BOOLEAN OPTIONS(
            description="TRUE if nightly checksums are being streamed and match for this table"),
          number_of_rows INTEGER OPTIONS(
            description="The total number of rows in this table according to the latest nightly checksum"),
          number_of_missing_rows INTEGER OPTIONS(
            description="The total number of rows missing in BigQuery but present in the source database"),
          number_of_extra_rows INTEGER OPTIONS(
            description="The total number of rows present in BigQuery but no longer in the source database"),
          weekly_change_in_number_of_rows INTEGER OPTIONS(
            description="Difference in number_of_rows compared to 7 days ago"),
          weekly_change_in_number_of_missing_rows INTEGER OPTIONS(
            description="Difference in number_of_missing_rows compared to 7 days ago"),
          weekly_change_in_number_of_extra_rows INTEGER OPTIONS(
            description="Difference in number_of_extra_rows compared to 7 days ago"),
          error_rate FLOAT64 OPTIONS(
            description="Largest error rate for any table as a proportion of total rows"),
          twelve_week_projected_error_rate FLOAT64 OPTIONS(
            description="Projected error rate 12 weeks in the future assuming current trends"),
          hidden_pii_streamed_within_the_last_week BOOLEAN OPTIONS(
            description="TRUE if hidden_data has been used in this events table within the last week"),
          hidden_pii_configured BOOLEAN OPTIONS(
            description="TRUE if a policy tag is configured in dfe-analytics-dataform for this pipeline")
          )
          OPTIONS (
            description = "Table-level pipeline monitoring data providing a detailed overview of project tables, including checksum verification and the number of missing or extra rows in BigQuery compared to the source database."
          );

      INSERT ${targetTable} (
          workflow_executed_at,
          gcp_project_name,
          event_source_name,
          output_dataset_name,
          entity_table_name,
          checksum_calculated_at,
          matching_checksums,
          number_of_rows,
          number_of_missing_rows,
          number_of_extra_rows,
          weekly_change_in_number_of_rows,
          weekly_change_in_number_of_missing_rows,
          weekly_change_in_number_of_extra_rows,
          error_rate,
          twelve_week_projected_error_rate,
          hidden_pii_streamed_within_the_last_week,
          hidden_pii_configured 
          )
      WITH
        ${params.transformEntityEvents ? `
        entity_table_check_latest AS (
        SELECT
          *
        FROM
          ${ctx.ref("entity_table_check_scheduled_" + params.eventSourceName)}
        QUALIFY
          ROW_NUMBER() OVER (PARTITION BY entity_table_name ORDER BY checksum_calculated_at DESC) = 1
        ),
        entity_table_check_scheduled_metrics AS (
        SELECT
          entity_table_name,
          checksum_calculated_at,
          CASE WHEN database_checksum = bigquery_checksum THEN true else false END AS matching_checksums,
          database_row_count AS number_of_rows,
          IFNULL(number_of_missing_rows, CASE WHEN database_row_count > 0 THEN 0 END) AS number_of_missing_rows,
          IFNULL(number_of_extra_rows, CASE WHEN database_row_count > 0 THEN 0 END) AS number_of_extra_rows,
          weekly_change_in_number_of_rows,
          weekly_change_in_number_of_missing_rows,
          weekly_change_in_number_of_extra_rows,
          error_rate,
          twelve_week_projected_error_rate,
        FROM
          entity_table_check_latest ` : `
        entity_table_check_scheduled_metrics AS (
          SELECT
          CAST(NULL AS BOOL) AS matching_checksums,
          CAST(NULL AS INT64) AS number_of_rows,
          CAST(NULL AS INT64) AS number_of_missing_rows,
          CAST(NULL AS INT64) AS number_of_extra_rows
          `}
        ),
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
        entity_table_check_scheduled_metrics.entity_table_name,
        entity_table_check_scheduled_metrics.checksum_calculated_at,
        entity_table_check_scheduled_metrics.matching_checksums,
        entity_table_check_scheduled_metrics.number_of_rows,
        entity_table_check_scheduled_metrics.number_of_missing_rows,
        entity_table_check_scheduled_metrics.number_of_extra_rows,
        entity_table_check_scheduled_metrics.weekly_change_in_number_of_rows,
        entity_table_check_scheduled_metrics.weekly_change_in_number_of_missing_rows,
        entity_table_check_scheduled_metrics.weekly_change_in_number_of_extra_rows,
        entity_table_check_scheduled_metrics.error_rate,
        entity_table_check_scheduled_metrics.twelve_week_projected_error_rate,
        /* New parameters from dfe-analytics-dataform v2.0.0 below */
        events_table_metrics.hidden_pii_streamed_within_the_last_week,
        ${params.hiddenPolicyTagLocation ? "TRUE" : "FALSE"} AS hidden_pii_configured
        ${params.transformEntityEvents ? `` : ``}
      FROM
        entity_table_check_scheduled_metrics,
        events_table_metrics;
      EXCEPTION WHEN ERROR THEN
        IF LOWER(@@error.message) LIKE "%access denied%" THEN RAISE USING MESSAGE = "Your Dataform service account does not have the required permissions to send data to the monitoring.pipeline_snapshots table in the cross-teacher-services GCP project. Please ask the Data & Insights team on Slack (#twd_data_insights) to give your Dataform service account the BigQuery Data Editor role on this table.";
        ELSE RAISE;
        END IF;
      END;
        `]).tags([params.eventSourceName.toLowerCase()])
    ];
};
