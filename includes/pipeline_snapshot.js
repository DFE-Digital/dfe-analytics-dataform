module.exports = (version, params) => {
    // Determine the target table based on the environment (development or production):
    // - In development, the table includes a schema suffix specific to the workspace.
    // - In production, the table uses the standard monitoring schema.

    const isDevelopment = !!dataform.projectConfig.schemaSuffix;

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
      // Step 2: Aggregate metrics to the output_dataset_name level
      operate("pipeline_snapshots_" + params.eventSourceName, ctx => [`
      BEGIN
      CREATE TABLE IF NOT EXISTS ${aggTargetTable} (
        workflow_executed_at TIMESTAMP OPTIONS(
          description="Timestamp when the Dataform pipeline took this snapshot of itself."),
        gcp_project_name STRING OPTIONS(
          description="Name of the GCP project within which this Dataform pipeline was executed."),
        event_source_name STRING OPTIONS(
          description="The eventSourceName included in the name of each table compiled via the dfeAnalyticsDataform() function."),
        dfe_analytics_version STRING OPTIONS(
          description="Version of dfe-analytics last used to stream events into the events table for this pipeline."),
        dfe_analytics_dataform_version STRING OPTIONS(
          description="Version of dfe-analytics-dataform used to generate the SQL queries executed in this pipeline."),
        number_of_tables INTEGER OPTIONS(
          description="Total number of tables in the application database currently configured to stream to BigQuery."),
        number_of_tables_with_matching_checksums INTEGER OPTIONS(
          description="Number of tables fully and accurately loaded into BigQuery with matching checksums."),
        checksum_enabled BOOLEAN OPTIONS(
          description="Indicates if nightly checksums are being streamed to BigQuery by this application."),
        number_of_missing_rows INTEGER OPTIONS(
          description="Total rows present in the database but missing in BigQuery across all tables."),
        number_of_extra_rows INTEGER OPTIONS(
          description="Total rows present in BigQuery but no longer present in the source database."),
        number_of_rows INTEGER OPTIONS(
          description="Total rows across all tables in the database, based on the latest nightly checksum events."),
        dfe_analytics_dataform_parameters STRING OPTIONS(
          description="JSON string containing all configuration parameters used in this call to dfeAnalyticsDataform()."),
        output_dataset_name STRING OPTIONS(
          description="Name of the BigQuery dataset where this pipeline outputs transformed tables."),
        hidden_pii_streamed_within_the_last_week BOOLEAN OPTIONS(
          description="Indicates if hidden_data has been used in the events table within the last week."),
        hidden_pii_configured BOOLEAN OPTIONS(
          description="Indicates if a policy tag is configured in dfe-analytics-dataform for this pipeline."),
        weekly_change_in_number_of_rows INTEGER OPTIONS(
          description="Difference in number_of_rows compared to the value 7 days prior."),
        weekly_change_in_number_of_missing_rows INTEGER OPTIONS(
          description="Difference in number_of_missing_rows compared to the value 7 days prior."),
        weekly_change_in_number_of_extra_rows INTEGER OPTIONS(
          description="Difference in number_of_extra_rows compared to the value 7 days prior."),
        largest_error_rate_for_any_table FLOAT64 OPTIONS(
          description="Largest error rate for any table based on missing or extra rows as a proportion of total rows; always positive."),
        table_with_largest_error_rate STRING OPTIONS(
          description="Name of the table corresponding to the largest error rate."),
        largest_twelve_week_projected_error_rate_for_any_table FLOAT64 OPTIONS(
          description="Projected largest error rate for any table 12 weeks after checksum calculation, assuming current trends continue."),
        table_with_largest_twelve_week_projected_error_rate STRING OPTIONS(
          description="Name of the table with the largest projected twelve-week error rate.")
      )
      OPTIONS (
            description = "Event-level pipeline monitoring data providing a detailed overview of how many tables in the project have matching checksums, along with counts of rows missing or extra in BigQuery compared to the source database."
          );
      INSERT INTO ${aggTargetTable}
      WITH dfe_analytics_configuration_metrics AS (
        SELECT
          MAX_BY(version, valid_from) AS dfe_analytics_version
        FROM
          ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)}
        WHERE
          valid_to IS NULL ),
      latest_data AS (
        SELECT
          *
        FROM
          ${targetTable}
        WHERE event_source_name = "${params.eventSourceName}"
        QUALIFY
          ROW_NUMBER() OVER (PARTITION BY entity_table_name ORDER BY workflow_executed_at DESC) = 1
        )
      SELECT
        workflow_executed_at,
        gcp_project_name,
        event_source_name,
        dfe_analytics_configuration_metrics.dfe_analytics_version,
        "${version}" AS dfe_analytics_dataform_version,
        COUNT(DISTINCT entity_table_name) AS number_of_tables,
        SUM(CAST(matching_checksums AS INTEGER)) as number_of_tables_with_matching_checksums,
        COUNT(DISTINCT entity_table_name) > 0 AS checksum_enabled,
        SUM(number_of_missing_rows) AS number_of_missing_rows,
        SUM(number_of_extra_rows) AS number_of_extra_rows,
        SUM(number_of_rows) AS number_of_rows,
        """${JSON.stringify(params)}""" AS dfe_analytics_dataform_parameters,
        output_dataset_name,
        hidden_pii_streamed_within_the_last_week,
        hidden_pii_configured,
        SUM(weekly_change_in_number_of_rows) AS weekly_change_in_number_of_rows,
        SUM(weekly_change_in_number_of_missing_rows) AS weekly_change_in_number_of_missing_rows,
        SUM(weekly_change_in_number_of_extra_rows) AS weekly_change_in_number_of_extra_rows,
        MAX(error_rate) AS largest_error_rate_for_any_table,
        MAX_BY(entity_table_name, error_rate) AS table_with_largest_error_rate,
        MAX(twelve_week_projected_error_rate) AS largest_twelve_week_projected_error_rate_for_any_table,
        MAX_BY(entity_table_name, twelve_week_projected_error_rate) AS table_with_largest_twelve_week_projected_error_rate
      FROM
        latest_data,
        dfe_analytics_configuration_metrics
      GROUP BY
        all;
      EXCEPTION WHEN ERROR THEN
        IF LOWER(@@error.message) LIKE "%access denied%" THEN RAISE USING MESSAGE = "Your Dataform service account does not have the required permissions to send data to the monitoring.pipeline_snapshots table in the cross-teacher-services GCP project. Please ask the Data & Insights team on Slack (#twd_data_insights) to give your Dataform service account the BigQuery Data Editor role on this table.";
        ELSE RAISE;
        END IF;
      END;
        `]).dependencies(["pipeline_table_snapshots_" + params.eventSourceName])
        .tags([params.eventSourceName.toLowerCase()])
    ];
};
