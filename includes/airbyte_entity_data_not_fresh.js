module.exports = (params) => {
  if (!params.enableAirbyteSource) return null;

    return params.dataSchema

    // Only generate assertions for tables which have dataFreshnessDays configured
    .filter(tableSchema => tableSchema.dataFreshnessDays)
    // Generate a separate assertion with a different name for each entityname_latest_eventsourcename table called something like entityname_data_not_fresh_eventsourcename
    .map(tableSchema => {
      if (!Number.isInteger(tableSchema.dataFreshnessDays) || tableSchema.dataFreshnessDays < 1) {
        throw new Error(`dataFreshnessDays parameter for the ${tableSchema.entityTableName} entityTableName is not a positive integer.`);
      } else if (tableSchema.dataFreshnessDisableDuringRange && params.disableAssertionsNow) {
        // Don't return an assertion because it's disabled right now
      } else {
        return assert(
          tableSchema.entityTableName +
          "_airbyte_data_not_fresh_" +
          params.eventSourceName, {
          ...params.defaultConfig
        }
        ).tags([params.eventSourceName.toLowerCase(),  'airbyte'])
          .query(ctx =>
            "SELECT MAX(CAST(updated_at AS TIMESTAMP)) AS last_updated_at FROM " +
                        "`" + params.bqProjectName + "." + params.airbyteConfig.datasetName + "." + params.airbyteConfig.tablePrefix + tableSchema.entityTableName + "`" +
                        " HAVING last_updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL " +
                        tableSchema.dataFreshnessDays +
                        " DAY)"
          )
      }
    })
}
