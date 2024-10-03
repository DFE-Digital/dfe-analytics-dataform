module.exports = (params) => {
  return params.customEventSchema
    // Only generate assertions for custom events which have dataFreshnessDays configured
    .filter(customEvent => customEvent.dataFreshnessDays)
    // Generate a separate assertion with a different name for each eventtype_eventsourcename custom event called something like eventtype_custom_event_data_not_fresh_eventsourcename
    .forEach(customEvent => {
      if (!Number.isInteger(customEvent.dataFreshnessDays) || customEvent.dataFreshnessDays < 1) {
        throw new Error(`dataFreshnessDays parameter for the ${customEvent.eventType} eventType is not a positive integer.`);
      } else if (customEvent.dataFreshnessDisableDuringRange && params.disableAssertionsNow) {
        // Don't return an assertion because it's disabled right now
      } else {
        return assert(
          customEvent.eventType +
          "_custom_event_data_not_fresh_" +
          params.eventSourceName, {
          ...params.defaultConfig
        }
        ).tags([params.eventSourceName.toLowerCase()])
          .query(ctx =>
            "SELECT MAX(occurred_at) AS event_last_occurred_at FROM " +
            ctx.ref(customEvent.eventType + "_" + params.eventSourceName) +
            " HAVING occurred_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL " +
            customEvent.dataFreshnessDays +
            " DAY)"
          )
      }
    })
}
