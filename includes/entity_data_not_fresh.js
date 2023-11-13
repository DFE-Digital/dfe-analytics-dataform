module.exports = (params) => {
  return params.dataSchema
    // Only generate assertions for tables which have dataFreshnessDays configured
    .filter(tableSchema => tableSchema.dataFreshnessDays)
    // Generate a separate assertion with a different name for each entityname_latest_eventsourcename table called something like entityname_data_not_fresh_eventsourcename
    .forEach(tableSchema => {
      if (!Number.isInteger(tableSchema.dataFreshnessDays) || tableSchema.dataFreshnessDays < 1) {
        throw new Error(`dataFreshnessDays parameter for the ${tableSchema.entityTableName} entityTableName is not a positive integer.`);
      } else if (tableSchema.dataFreshnessDisableDuringRange && params.disableAssertionsNow) {
        // Don't return an assertion because it's disabled right now
      } else {
        return assert(
          tableSchema.entityTableName +
          "_data_not_fresh_" +
          params.eventSourceName, {
          ...params.defaultConfig
        }
        ).tags([params.eventSourceName.toLowerCase()])
          .query(ctx =>
            "SELECT MAX(last_streamed_event_occurred_at) AS event_last_streamed_at FROM " +
            ctx.ref(tableSchema.entityTableName + "_latest_" + params.eventSourceName) +
            " HAVING event_last_streamed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL " +
            tableSchema.dataFreshnessDays +
            " DAY)"
          )
      }
    })
}
