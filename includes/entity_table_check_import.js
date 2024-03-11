module.exports = (params) => {
    return publish(
            "entity_table_check_import_" + params.eventSourceName, {
                ...params.defaultConfig,
                type: "table",
                uniqueKey: ["entity_table_name", "import_id"],
                assertions: {
                    uniqueKey: ["entity_table_name", "import_id"],
                    nonNull: ["entity_table_name"]
                },
                dependencies: [params.eventSourceName + "_entities_are_missing_expected_fields"],
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
                    order_column: "The column used to order entity IDs as part of the checksum calculation algorithm for both database_checksum and bigquery_checksum. May be updated_at (default), created_at or id.",
                    bigquery_row_count: "The number of unique IDs for this entity in the group of import_entity events with this import_id in the events table. ",
                    imported_entity_ids: "Array of UIDs for entities included in this import."
                }
            }
        ).tags([params.eventSourceName.toLowerCase()])
        .query(ctx =>
            `WITH
        import_event AS (
          SELECT
            event_tags[0] AS import_id,
            entity_table_name,
            ${data_functions.eventDataExtract("data", "id")} AS id,
            ${data_functions.eventDataExtract("data", "created_at", false, "timestamp")} AS created_at,
            ${data_functions.eventDataExtract("data", "updated_at", false, "timestamp")} AS updated_at
          FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
          WHERE
            event_type = "import_entity"
            AND ARRAY_LENGTH(event_tags) = 1
          ),
        check_event AS (
          SELECT
            *
          FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
          WHERE
            event_type = "import_entity_table_check"
        ),
        check AS (
        SELECT
          entity_table_name,
          ${data_functions.eventDataExtract("data", "row_count", false, "integer")} AS row_count,
          ${data_functions.eventDataExtract("data", "checksum", false, "string")} AS checksum,
          ${data_functions.eventDataExtract("data", "checksum_calculated_at", false, "timestamp")} AS checksum_calculated_at,
          LOWER(${data_functions.eventDataExtract("data", "order_column", false, "string")}) AS order_column,
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
            COUNT(DISTINCT import_event.id) AS bigquery_row_count,
            ARRAY_AGG(import_event.id) AS imported_entity_ids,
            check.checksum AS database_checksum,
            check.order_column,
            TO_HEX(MD5(STRING_AGG(${sortField == "id" ? `import_event.id` : `CASE WHEN ${sortField} < check.checksum_calculated_at THEN import_event.id END`}, ""
                ORDER BY
                  import_event.${sortField} ASC))) AS bigquery_checksum,
            check.checksum_calculated_at
          FROM
            check
          LEFT JOIN
            import_event
          ON
            check.entity_table_name = import_event.entity_table_name
            AND check.import_id = import_event.import_id
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
        )
}