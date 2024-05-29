module.exports = (params) => {
    return publish(
            "entity_table_check_import_" + params.eventSourceName, {
                ...params.defaultConfig,
                type: "incremental",
                assertions: {
                    uniqueKey: ["entity_table_name", "import_id"],
                    nonNull: ["entity_table_name"]
                },
                dependencies: [params.eventSourceName + "_entities_are_missing_expected_fields", params.eventSourceName + "_hidden_pii_configuration_does_not_match_events_streamed"],
                bigquery: {
                    labels: {
                        eventsource: params.eventSourceName.toLowerCase(),
                        sourcedataset: params.bqDatasetName.toLowerCase()
                    }
                },
                tags: [params.eventSourceName.toLowerCase()],
                description: "Checksum events streamed by dfe-analytics after application database table imports to allow dfe-analytics-dataform to verify that tables have been fully loaded in to BigQuery, and correct this where possible, together with row counts and checksums calculated for data loaded into BigQuery for comparison.",
                columns: {
                    entity_table_name: "Name of the table in the database that this checksum was calculated for",
                    import_id: "UID of the import. Each import event for that table has the same import_id. Different tables and different imports of the same table have different import_ids.",
                    database_row_count: "Number of rows in the database table at checksum_calculated_at.",
                    database_checksum: "Checksum for the database table at checksum_calculated_at. The checksum is calculated by ordering all the entity IDs by order_column, concatenating them and then using the SHA256 algorithm.",
                    bigquery_checksum: "Checksum for the group of import_entity events with this import_id in the events table. The checksum is calculated by ordering all the entity IDs by order_column, concatenating them and then using the SHA256 algorithm.",
                    checksum_calculated_at: "The time that database_checksum was calculated.",
                    final_import_event_received_at: "The time that the final import_entity or import_entity_table_check event was received from dfe-analytics for this import",
                    order_column: "The column used to order entity IDs as part of the checksum calculation algorithm for both database_checksum and bigquery_checksum. May be updated_at (default), created_at or id.",
                    bigquery_row_count: "The number of unique IDs for this entity in the group of import_entity events with this import_id in the events table. ",
                    imported_entity_ids: {
                        description: "Array of UIDs for entities included in this import.",
                        bigqueryPolicyTags: params.hidePrimaryKey && params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                    }
                }
            }
        ).tags([params.eventSourceName.toLowerCase()])
        .query(ctx =>
            `WITH
        import_event AS (
          SELECT
            event_tags[0] AS import_id,
            entity_table_name,
            occurred_at,
            ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "id")} AS id,
            ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "created_at", false, "timestamp")} AS created_at,
            ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "updated_at", false, "timestamp")} AS updated_at
          FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
          WHERE
            event_type = "import_entity"
            AND ARRAY_LENGTH(event_tags) = 1
            AND occurred_at > event_timestamp_checkpoint
          ),
        latest_import_event AS (
        /* In rare cases duplicate import events are streamed. This deduplicates by taking only the most recent one. */
          SELECT
            import_id,
            entity_table_name,
            occurred_at,
            id,
            created_at,
            updated_at
          FROM import_event
          WHERE id IS NOT NULL
          QUALIFY ROW_NUMBER() OVER (PARTITION BY import_id, entity_table_name, id ORDER BY COALESCE(updated_at, created_at, occurred_at) DESC) = 1
        ),
        check_event AS (
          SELECT
            *
          FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
          WHERE
            event_type = "import_entity_table_check"
            AND occurred_at > event_timestamp_checkpoint
        ),
        check AS (
        SELECT
          entity_table_name,
          occurred_at,
          ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "row_count", false, "integer")} AS row_count,
          ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "checksum", false, "string")} AS checksum,
          ${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "checksum_calculated_at", false, "timestamp")} AS checksum_calculated_at,
          LOWER(${data_functions.eventDataExtract("ARRAY_CONCAT(data, hidden_data)", "order_column", false, "string")}) AS order_column,
          event_tags[0] AS import_id
        FROM
          check_event
        ),
        /* BigQuery has a 100MB limit for the data processed across all aggregate functions within an individual subquery. */
        /* Calculating checksums with two different sort orders depending on order_column causes this limit to be breached if completed within the same subquery. */
        /* To work around this each order is calculated in a separate subquery below and then recombined. */
        ${["updated_at", "created_at", "id"].map(sortField =>
        `imports_with_${sortField}_metrics AS (
          SELECT
            check.entity_table_name,
            check.import_id,
            check.row_count AS database_row_count,
            COUNT(DISTINCT latest_import_event.id) AS bigquery_row_count,
            ARRAY_AGG(latest_import_event.id IGNORE NULLS) AS imported_entity_ids,
            check.checksum AS database_checksum,
            check.order_column,
            TO_HEX(MD5(STRING_AGG(${sortField == "id" ? `latest_import_event.id` : `CASE WHEN TIMESTAMP_TRUNC(${sortField}, MILLISECOND) < TIMESTAMP_TRUNC(check.checksum_calculated_at, MILLISECOND) THEN latest_import_event.id END`}, ""
                ORDER BY
                  ${sortField == "id" ? `latest_import_event.id ASC` : `TIMESTAMP_TRUNC(latest_import_event.${sortField}, MILLISECOND) ASC, latest_import_event.id ASC`}))) AS bigquery_checksum,
            check.checksum_calculated_at,
            GREATEST(MAX(latest_import_event.occurred_at), MAX(check.occurred_at)) AS final_import_event_received_at
          FROM
            check
          LEFT JOIN
            latest_import_event
          ON
            check.entity_table_name = latest_import_event.entity_table_name
            AND check.import_id = latest_import_event.import_id
          WHERE
            check.order_column = "${sortField}"
            ${(sortField == "updated_at") ? "/* Default to sorting by updated_at for backwards compatibility */ OR check.order_column IS NULL":""}
          GROUP BY
            check.entity_table_name,
            check.import_id,
            check.row_count,
            check.checksum,
            check.checksum_calculated_at,
            check.order_column )`
          ).join(',\n')}
      SELECT
        check.import_id,
        check.entity_table_name,
        check.row_count AS database_row_count,
        COALESCE(imports_with_updated_at_metrics.bigquery_row_count, imports_with_created_at_metrics.bigquery_row_count, imports_with_id_metrics.bigquery_row_count) AS bigquery_row_count,
        check.checksum AS database_checksum,
        check.order_column,
        check.checksum_calculated_at,
        CASE check.order_column
          WHEN "id" THEN imports_with_id_metrics.final_import_event_received_at
          WHEN "created_at" THEN imports_with_created_at_metrics.final_import_event_received_at
          ELSE imports_with_updated_at_metrics.final_import_event_received_at
        END AS final_import_event_received_at,
        CASE
          WHEN NOT COALESCE(imports_with_updated_at_metrics.bigquery_row_count, imports_with_created_at_metrics.bigquery_row_count, imports_with_id_metrics.bigquery_row_count) > 0 THEN TO_HEX(MD5(""))
          WHEN check.order_column = "created_at" THEN imports_with_created_at_metrics.bigquery_checksum
          WHEN check.order_column = "id" THEN imports_with_id_metrics.bigquery_checksum
        ELSE
          /* Default to sorting by updated_at for backwards compatibility */
          imports_with_updated_at_metrics.bigquery_checksum
        END AS bigquery_checksum,
        CASE check.order_column
            WHEN "created_at" THEN imports_with_created_at_metrics.imported_entity_ids
            WHEN "id" THEN imports_with_id_metrics.imported_entity_ids
            ELSE imports_with_updated_at_metrics.imported_entity_ids
        END AS imported_entity_ids
      FROM
        check
      LEFT JOIN
        imports_with_updated_at_metrics
      USING (entity_table_name, import_id)
      LEFT JOIN
        imports_with_created_at_metrics
      USING (entity_table_name, import_id)
      LEFT JOIN
        imports_with_id_metrics
      USING (entity_table_name, import_id)`
        ).preOps(ctx => `
    DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), `SELECT MAX(final_import_event_received_at) FROM ${ctx.self()}`, `SELECT TIMESTAMP("2000-01-01")`)});`
  )
}
