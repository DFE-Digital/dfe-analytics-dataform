module.exports = (version, params) => {
    // Determine the target table based on the environment (development or production):
    // - In development, the table includes a schema suffix specific to the workspace.
    // - In production, the table uses the standard monitoring schema.

    const isDevelopment = dataform.projectConfig.schemaSuffix;

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
        workflow_executed_at TIMESTAMP,
        gcp_project_name STRING,
        event_source_name STRING,
        dfe_analytics_version STRING,
        dfe_analytics_dataform_version STRING,
        number_of_tables INTEGER,
        number_of_tables_with_matching_checksums INTEGER,
        checksum_enabled BOOLEAN,
        number_of_missing_rows INTEGER,
        number_of_extra_rows INTEGER,
        number_of_rows INTEGER,
        dfe_analytics_dataform_parameters STRING,
        output_dataset_name STRING,
        hidden_pii_streamed_within_the_last_week BOOLEAN,
        hidden_pii_configured BOOLEAN,
        weekly_change_in_number_of_rows INTEGER,
        weekly_change_in_number_of_missing_rows INTEGER,
        weekly_change_in_number_of_extra_rows INTEGER,
        largest_error_rate_for_any_table FLOAT64,
        table_with_largest_error_rate STRING,
        largest_twelve_week_projected_error_rate_for_any_table FLOAT64,
        table_with_largest_twelve_week_projected_error_rate STRING
      );
      INSERT INTO ${aggTargetTable}
      WITH dfe_analytics_configuration_metrics AS (
        SELECT
          MAX_BY(version, valid_from) AS dfe_analytics_version
        FROM
          ${ctx.ref("dfe_analytics_configuration_" + params.eventSourceName)}
        WHERE
          valid_to IS NULL )
      SELECT
        CURRENT_TIMESTAMP AS workflow_executed_at,
        gcp_project_name,
        event_source_name,
        dfe_analytics_configuration_metrics.dfe_analytics_version,
        "${version}" AS dfe_analytics_dataform_version,
        COUNT(DISTINCT entity_table_name) AS number_of_tables,
        SUM(CAST(matching_checksums AS INT64)) as number_of_tables_with_matching_checksums,
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
        ${targetTable},
        dfe_analytics_configuration_metrics
      GROUP BY
        all;
      EXCEPTION WHEN ERROR THEN
        IF LOWER(@@error.message) LIKE "%access denied%" THEN RAISE USING MESSAGE = "Your Dataform service account does not have the required permissions to send data to the monitoring.pipeline_snapshots table in the cross-teacher-services GCP project. Please ask the Data & Insights team on Slack (#twd_data_insights) to give your Dataform service account the BigQuery Data Editor role on this table.";
        ELSE RAISE;
        END IF;
      END;
        `]).tags([params.eventSourceName.toLowerCase()])
    ];
};
