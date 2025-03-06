module.exports = (params) => {
    return assert(params.eventSourceName + "_unhandled_custom_event_is_being_streamed", {
        ...params.defaultConfig,
        type: "assertion",
        description: "Identifies any custom events that are being streamed which don't exist in the customEventSchema parameter passed to dfe-analytics-dataform. If this assertion fails it means that we need to add the custom event to customEventSchema. We may also want to update other parts of the pipeline to make this data available."
    }).tags([params.eventSourceName.toLowerCase()]).query(ctx => `

WITH expected_custom_events AS (
  SELECT COALESCE(
      ARRAY_AGG(eventType), 
      ARRAY<STRING>[]  -- Ensures a valid array even if no events exist
  ) AS expected_event_types
  FROM (
      SELECT eventType FROM UNNEST([
          ${params.customEventSchema.length > 0 
            ? params.customEventSchema.map(event => `"${event.eventType}"`).join(',') 
            : `CAST(NULL AS STRING)`}  -- NULL is valid inside UNNEST()
      ]) AS eventType
      WHERE eventType IS NOT NULL  -- Prevents NULL values in the array
  )
),

event_data AS (
  SELECT
    event_type AS unexpected_event_type,
    COUNT(DISTINCT occurred_at) AS updates_made_yesterday_with_this_key
  FROM
    ${ctx.ref("events_" + params.eventSourceName)}
  WHERE
  DATE(occurred_at) >= CURRENT_DATE - 1
  GROUP BY event_type
)

SELECT
  unexpected_event_type,
  updates_made_yesterday_with_this_key
FROM event_data
WHERE unexpected_event_type NOT IN UNNEST((SELECT expected_event_types FROM expected_custom_events))
 -- Exclude the 'non-custom' events
  AND unexpected_event_type NOT  IN (
    'create_entity',
    'delete_entity',
    'entity_table_check',
    'import_entity',
    'import_entity_table_check',
    'initialise_analytics',
    'update_entity',
    'web_request'
  )
ORDER BY unexpected_event_type
`)
}

