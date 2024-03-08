module.exports = (params) => {
    return publish(
            "entity_table_check_scheduled_" + params.eventSourceName, {
                ...params.defaultConfig,
                type: "table",
                uniqueKey: ["entity_table_name"],
                assertions: {
                    uniqueKey: ["entity_table_name"],
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
                description: "Scheduled checksum events streamed by dfe-analytics to allow dfe-analytics-dataform to verify that tables have been fully loaded in to BigQuery, together with row counts and checksums calculated for data loaded into BigQuery for comparison.",
                columns: {
                    entity_table_name: "Name of the table in the database that this checksum was calculated for",
                    database_row_count: "Number of rows in the database table at checksum_calculated_at.",
                    database_checksum: "Checksum for the database table at checksum_calculated_at. The checksum is calculated by ordering all the entity IDs by order_column, concatenating them and then using the SHA256 algorithm.",
                    bigquery_checksum: "Checksum for this entity in the entity_version table at checksum_calculated_at, or the group of import_entity events with this import_id in the events table (as applicable). The checksum is calculated by ordering all the entity IDs by order_column, concatenating them and then using the SHA256 algorithm.",
                    checksum_calculated_at: "The time that database_checksum was calculated.",
                    order_column: "The column used to order entity IDs as part of the checksum calculation algorithm for both database_checksum and bigquery_checksum. May be updated_at (default), created_at or id.",
                    bigquery_row_count: "The number of unique IDs for this entity in the entity_version table at checksum_calculated_at, or the group of import_entity events with this import_id in the events table (as applicable). ",
                    bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation: "The number of unique IDs for this entity in the entity_version table at checksum_calculated_at which have a timestamp value (created_at or updated_at) for order_column which is earlier than checksum_calculated_at. This indicates that they likely changed during checksum calculation in the database and so have been excluded from the checksum."
                }
            }
        ).tags([params.eventSourceName.toLowerCase()])
        .query(ctx =>
            `WITH
        entity_version AS (
        /* Pre filter entity_version on its valid_to partition to accelerate performance */
        SELECT
          entity_table_name,
          entity_id AS id,
          valid_from,
          valid_to,
          created_at,
          updated_at
        FROM
          ${ctx.ref(params.eventSourceName + "_entity_version")}
        WHERE
          valid_to IS NULL
          OR DATE(valid_to) >= CURRENT_DATE - 1 ),
        check_event AS (
        SELECT
          *
        FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
        WHERE
          event_type = "entity_table_check"
          /* Only process checksums since yesterday */
          AND DATE(occurred_at) >= CURRENT_DATE - 1
          /* Only process the most recent check for each entity_table_name */
          QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_table_name, event_type ORDER BY occurred_at DESC) = 1
        ),
        check AS (
        SELECT
          entity_table_name,
          ${data_functions.eventDataExtract("data", "row_count", false, "integer")} AS row_count,
          ${data_functions.eventDataExtract("data", "checksum", false, "string")} AS checksum,
          ${data_functions.eventDataExtract("data", "checksum_calculated_at", false, "timestamp")} AS checksum_calculated_at,
          LOWER(${data_functions.eventDataExtract("data", "order_column", false, "string")}) AS order_column
        FROM
          check_event
        ),
        /* BigQuery has a 100MB limit for the data processed across all aggregate functions within an individual subquery. */
        /* Calculating checksums with two different sort orders depending on order_column causes this limit to be breached if completed within the same subquery. */
        /* To work around this each order is calculated in a separate subquery below and then recombined. */
        ${["updated_at", "created_at", "id"].map(sortField =>
        `tables_with_${sortField}_metrics AS (
          SELECT
            check.entity_table_name,
            check.row_count AS database_row_count,
            COUNT(DISTINCT entity_version.id) AS bigquery_row_count,
            ${sortField == "id" ? `` : `COUNT(DISTINCT CASE WHEN NOT ${sortField} < check.checksum_calculated_at THEN entity_version.id END) AS bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation,`}
            check.checksum AS database_checksum,
            check.order_column,
            TO_HEX(MD5(STRING_AGG(${sortField == "id" ? `entity_version.id` : `CASE WHEN ${sortField} < check.checksum_calculated_at THEN entity_version.id END`}, ""
                ORDER BY
                  entity_version.${sortField} ASC))) AS bigquery_checksum,
            check.checksum_calculated_at
          FROM
            check
          LEFT JOIN
            entity_version
          ON
            check.entity_table_name = entity_version.entity_table_name
            /* Join on to entity versions which were valid at the time the checksum was calculated from the database */
            AND ((entity_version.valid_to IS NULL
                OR entity_version.valid_to > check.checksum_calculated_at)
              AND entity_version.valid_from <= check.checksum_calculated_at)
          WHERE
            check.order_column = "${sortField}"
            ${(sortField == "updated_at") ? "/* Default to sorting by updated_at for backwards compatibility */ OR check.order_column IS NULL":""}
          GROUP BY
            check.entity_table_name,
            check.row_count,
            check.checksum,
            check.checksum_calculated_at,
            check.order_column )`
          ).join(',\n')}
      SELECT
        check.entity_table_name,
        check.row_count AS database_row_count,
        COALESCE(tables_with_updated_at_metrics.bigquery_row_count, tables_with_created_at_metrics.bigquery_row_count, tables_with_id_metrics.bigquery_row_count) AS bigquery_row_count,
        COALESCE(tables_with_updated_at_metrics.bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation, tables_with_created_at_metrics.bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation) AS bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation,
        check.checksum AS database_checksum,
        check.order_column,
        check.checksum_calculated_at,
        CASE
          WHEN NOT COALESCE(tables_with_updated_at_metrics.bigquery_row_count, tables_with_created_at_metrics.bigquery_row_count, tables_with_id_metrics.bigquery_row_count) > 0 THEN TO_HEX(MD5(""))
          WHEN check.order_column = "created_at" THEN tables_with_created_at_metrics.bigquery_checksum
          WHEN check.order_column = "id" THEN tables_with_id_metrics.bigquery_checksum
        ELSE
          /* Default to sorting by updated_at for backwards compatibility */
          tables_with_updated_at_metrics.bigquery_checksum
        END AS bigquery_checksum
      FROM
        check
      LEFT JOIN
        tables_with_updated_at_metrics
      USING (entity_table_name)
      LEFT JOIN
        tables_with_created_at_metrics
      USING (entity_table_name)
      LEFT JOIN
        tables_with_id_metrics
      USING (entity_table_name)`
        )
}
