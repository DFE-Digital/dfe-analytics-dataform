module.exports = (params) => {
  return assert(params.eventSourceName + "_entities_have_not_been_backfilled", {
    ...params.defaultConfig,
    type: "assertion",
    description: "Identifies any entities configured in the dfe-analytics-dataform dataSchema which have no import_entity or import_entity_table_check events in the events table. These entities almost certainly have incomplete data as a result."
  }).tags([params.eventSourceName.toLowerCase()]).query(ctx => `
WITH expected_entity AS (
    SELECT DISTINCT
        entity_table_name,
        expiry_days,
        CURRENT_DATE - expiry_days AS approximate_expiry_date
    FROM
        UNNEST([
            ${params.dataSchema.map(tableSchema => {
          return `STRUCT("${tableSchema.entityTableName}" AS entity_table_name,
              SAFE_CAST("${tableSchema.expirationDays || params.expirationDays}" AS INT64) AS expiry_days
              )`;
  }
  ).join(',')}  
  ])
),
import_counts AS (
    SELECT
        entity_table_name,
        IFNULL(COUNT(occurred_at), 0) AS number_of_import_events
    FROM
        ${ctx.ref("events_" + params.eventSourceName)} AS events
    WHERE event_type IN ("import_entity", "import_entity_table_check")
    GROUP BY entity_table_name
  )
SELECT
    expected_entity.*,
    latest_check.bigquery_row_count,
    latest_check.database_row_count
FROM
    expected_entity
LEFT JOIN
    import_counts USING(entity_table_name)
LEFT JOIN
    ${ctx.ref("entity_table_check_scheduled_" + params.eventSourceName)} AS latest_check
    ON
      expected_entity.entity_table_name = latest_check.entity_table_name
      AND DATE(latest_check.checksum_calculated_at) = CURRENT_DATE
WHERE
    number_of_import_events = 0
ORDER BY
    entity_table_name ASC
  `)
}
