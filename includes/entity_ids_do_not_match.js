module.exports = (params) => {
  function tablesWithMetricsSql(sortFields, ctx) {
    return sortFields.map(sortField => {
    return `tables_with_${sortField}_metrics AS (
          SELECT
            check.entity_table_name,
            check.row_count AS database_row_count,
            COUNT(DISTINCT entity_version.entity_id) AS bigquery_row_count,
            COUNT(DISTINCT CASE WHEN ${sortField} < check.checksum_calculated_at THEN entity_version.entity_id END) AS bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation,
            check.checksum AS database_checksum,
            check.order_column,
            TO_HEX(MD5( STRING_AGG(CASE WHEN NOT ${sortField} < check.checksum_calculated_at THEN entity_version.entity_id END, ""
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
            check.order_column ),`;
  }).join('');
  }
  if (params.compareChecksums) {
    return assert(
      params.eventSourceName + "_entity_ids_do_not_match", {
      ...params.defaultConfig
    }
    ).tags([params.eventSourceName.toLowerCase()])
      .query(ctx =>
        `WITH
          entity_version AS (
          /* Pre filter entity_version on its valid_to partition to accelerate performance */
          SELECT
            *
          FROM
            ${ctx.ref(params.eventSourceName + "_entity_version")}
          WHERE
            valid_to IS NULL
            OR DATE(valid_to) >= CURRENT_DATE - 1 ),
          check AS (
          SELECT
            entity_table_name,
            ${data_functions.eventDataExtract("data", "row_count", false, "integer")} AS row_count,
            ${data_functions.eventDataExtract("data", "checksum", false, "string")} AS checksum,
            ${data_functions.eventDataExtract("data", "checksum_calculated_at", false, "timestamp")} AS checksum_calculated_at,
            LOWER(${data_functions.eventDataExtract("data", "order_column", false, "string")}) AS order_column
          FROM
            ${ctx.ref("events_" + params.eventSourceName)}
          WHERE
            event_type = "entity_table_check"
            /* Only process checksums since yesterday */
            AND DATE(occurred_at) >= CURRENT_DATE - 1
            /* Only process the most recent check for each entity_table_name */
            QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_table_name ORDER BY occurred_at DESC) = 1
          ),
          /* BigQuery has a 100MB limit for the data processed across all aggregate functions within an individual subquery. */
          /* Calculating checksums with two different sort orders depending on order_column causes this limit to be breached if completed within the same subquery. */
          /* To work around this each order is calculated in a separate subquery below and then recombined. */
          ${tablesWithMetricsSql(["updated_at", "created_at"], ctx)}
          tables_with_metrics AS (
          SELECT
            check.entity_table_name,
            check.row_count AS database_row_count,
            COALESCE(tables_with_updated_at_metrics.bigquery_row_count, tables_with_created_at_metrics.bigquery_row_count) AS bigquery_row_count,
            COALESCE(tables_with_updated_at_metrics.bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation, tables_with_created_at_metrics.bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation) AS bigquery_rows_excluded_because_they_may_have_changed_during_checksum_calculation,
            check.checksum AS database_checksum,
            check.order_column,
            check.checksum_calculated_at,
            CASE
              WHEN NOT COALESCE(tables_with_updated_at_metrics.bigquery_row_count, tables_with_created_at_metrics.bigquery_row_count) > 0 THEN TO_HEX(MD5(""))
              WHEN check.order_column = "created_at" THEN tables_with_created_at_metrics.bigquery_checksum
            ELSE
            /* Default to sorting by updated_at for backwards compatibility */
            tables_with_updated_at_metrics.bigquery_checksum
          END
            AS bigquery_checksum
          FROM
            check
          LEFT JOIN
            tables_with_updated_at_metrics
          USING
            (entity_table_name)
          LEFT JOIN
            tables_with_created_at_metrics
          USING
            (entity_table_name)
          )
        SELECT
          *,
          CASE
            WHEN database_row_count > 0 AND (bigquery_row_count IS NULL OR bigquery_row_count = 0) THEN "Row count in BigQuery is zero even though rows existed in source database"
            WHEN database_row_count > bigquery_row_count THEN "Row count in BigQuery is less than row count in source database"
            WHEN database_row_count < bigquery_row_count THEN "Row count in BigQuery is more than row count in source database. Perhaps some deletion events were not streamed as intended?"
            WHEN database_checksum != bigquery_checksum THEN "Set of IDs in BigQuery ordered by " || order_column || " did not exactly match the source database even though row counts do match."
          ELSE
          "None"
        END
          AS issue_description
        FROM
          tables_with_metrics
        WHERE
          /* Only fail if something doesn't match */
          database_checksum != bigquery_checksum
          OR database_row_count != bigquery_row_count
        ORDER BY
          entity_table_name ASC`
      )
  }
}