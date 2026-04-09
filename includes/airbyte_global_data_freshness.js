/* Checks that the Airbyte heartbeat table has been updated recently. Triggers an assertion failure if no new data has arrived within the configured number of hours. */

module.exports = (params) => {
  if (!params.enableAirbyteSource) return null;

  const freshnessHours = params.airbyteConfig.freshnessHours;
  const heartbeatProject = params.airbyteConfig.projectName;
  const heartbeatDataset = params.airbyteConfig.datasetName;
  const heartbeatTable = params.airbyteConfig.tableName;

  if (params.airbyteConfig.disableFreshnessCheckDuringRange && params.disableAssertionsNow) {
    return null;
  }

  return assert(
    params.eventSourceName + "_airbyte_global_data_not_fresh", {
      ...params.defaultConfig,
      type: "assertion",
      description: `Checks that the Airbyte heartbeat table (${heartbeatDataset}.${heartbeatTable}) has been updated within the last ${freshnessHours} hours. The heartbeat table contains a single row with the _airbyte_extracted_at timestamp of the latest Airbyte sync. If this assertion fails, Airbyte has not synced recently — investigate the Airbyte connection or source.`
    }
  ).tags([params.eventSourceName.toLowerCase(), 'airbyte'])
    .query(ctx =>
      "SELECT _airbyte_extracted_at AS last_airbyte_sync_at " +
      "FROM `" + heartbeatProject + "." + heartbeatDataset + "." + heartbeatTable + "` " +
      "WHERE _airbyte_extracted_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + freshnessHours + " HOUR)"
    )
}