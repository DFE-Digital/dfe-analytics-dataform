module.exports = (params) => {
  if (params.compareChecksums) {
    return assert(
      params.eventSourceName + "_entity_ids_do_not_match", {
      ...params.defaultConfig
    }
    ).tags([params.eventSourceName.toLowerCase()])
      .query(ctx =>
        `SELECT
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
          ${ctx.ref("entity_table_check_scheduled_" + params.eventSourceName)}
        WHERE
        (
          /* Only fail if something doesn't match */
          database_checksum != bigquery_checksum
          OR database_row_count != bigquery_row_count
          )
        ORDER BY
          entity_table_name ASC`
      )
  }
}