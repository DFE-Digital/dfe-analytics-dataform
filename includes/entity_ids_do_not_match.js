module.exports = (params) => {
  if (params.compareChecksums) {
    return assert(
      params.eventSourceName + "_entity_ids_do_not_match", {
      ...params.defaultConfig
    }
    ).tags([params.eventSourceName.toLowerCase()])
      .query(ctx =>
        `WITH
          check AS (
          SELECT
            entity_table_name,
            ${data_functions.eventDataExtract("data", "row_count", false, "integer")} AS row_count,
            ${data_functions.eventDataExtract("data", "checksum", false, "string")} AS checksum,
            ${data_functions.eventDataExtract("data", "checksum_calculated_at", false, "timestamp")} AS checksum_calculated_at
          FROM
            ${ctx.ref("events_" + params.eventSourceName)}
          WHERE
            event_type = "entity_table_check"
            /* Only process checksums since yesterday */
            AND DATE(occurred_at) >= CURRENT_DATE - 1
            /* Only process the most recent check for each entity_table_name */
            QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_table_name ORDER BY occurred_at DESC) = 1
          ),
          tables_with_metrics AS (
          SELECT
            check.entity_table_name,
            check.row_count AS database_row_count,
            COUNT(DISTINCT entity_version.updated_at) AS bigquery_row_count,
            check.checksum AS database_checksum,
            TO_HEX(MD5(STRING_AGG(entity_version.entity_id, ""
                ORDER BY
                  entity_version.updated_at ASC))) AS bigquery_checksum,
            check.checksum_calculated_at
          FROM
            check
          LEFT JOIN
            ${ctx.ref(params.eventSourceName + "_entity_version")} AS entity_version
          ON
            check.entity_table_name = entity_version.entity_table_name
            /* Join on to entity versions which were valid at the time the checksum was calculated from the database */
            AND ((entity_version.valid_to IS NULL
                OR entity_version.valid_to > check.checksum_calculated_at)
              AND entity_version.valid_from <= check.checksum_calculated_at)
          GROUP BY
            check.entity_table_name,
            check.row_count,
            check.checksum,
            check.checksum_calculated_at
          )
        SELECT
          *,
          CASE
            WHEN database_row_count > 0 AND (bigquery_row_count IS NULL OR bigquery_row_count = 0) THEN "Row count in BigQuery is zero even though rows existed in source database"
            WHEN database_row_count > bigquery_row_count THEN "Row count in BigQuery is less than row count in source database"
            WHEN database_row_count < bigquery_row_count THEN "Row count in BigQuery is more than row count in source database"
            WHEN database_checksum != bigquery_checksum THEN "Set of IDs in BigQuery did not exactly match the set of IDs in source database even though row counts do match"
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