module.exports = (params) => {
  return assert(
        params.eventSourceName + "_events_data_is_not_fresh",
        {
          ...params.defaultConfig
        }
      )
    .query(ctx =>
      "SELECT MAX(occurred_at) AS event_last_streamed_at FROM "
      + ctx.ref(params.bqDatasetName,params.bqEventsTableName)
      + " HAVING event_last_streamed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL " + params.eventsDataFreshnessDays + " DAY)"
        )
}