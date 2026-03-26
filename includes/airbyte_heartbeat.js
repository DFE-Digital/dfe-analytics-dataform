/* This checks the airbyte_heartbeat table to verify Airbyte synced within the last 12 hours. */

module.exports = (params) => {
  if (!params.enableAirbyteSource) return null;

  // Default to 12 hours
  // Allow override via params.airbyteConfig.heartbeatFreshnessHours
  const freshnessHours = (params.airbyteConfig && params.airbyteConfig.heartbeatFreshnessHours)
    ? params.airbyteConfig.heartbeatFreshnessHours
    : 12;

  // Allow override of heartbeat table location via airbyteConfig
  const heartbeatProject = (params.airbyteConfig && params.airbyteConfig.heartbeatProjectName)
    ? params.airbyteConfig.heartbeatProjectName
    : params.bqProjectName;
  const heartbeatDataset = (params.airbyteConfig && params.airbyteConfig.heartbeatDatasetName)
    ? params.airbyteConfig.heartbeatDatasetName
    : 'rtt_airbyte_production';
  const heartbeatTable = (params.airbyteConfig && params.airbyteConfig.heartbeatTableName)
    ? params.airbyteConfig.heartbeatTableName
    : 'airbyte_heartbeat';

  if (params.airbyteConfig.heartbeatFreshnessDisableDuringRange && params.disableAssertionsNow) {
    return null;
  }

  return assert(
    params.eventSourceName + "_airbyte_global_data_not_fresh", {
      ...params.defaultConfig,
      type: "assertion",
      description: `Checks that the Airbyte heartbeat table (${heartbeatDataset}.${heartbeatTable}) has been updated within the last ${freshnessHours} hours. The heartbeat table contains a single row with the _airbyte_extracted_at timestamp of the latest Airbyte sync. If this assertion fails, Airbyte has not synced recently — investigate the Airbyte connection or source.`
    }
  ).tags([params.eventSourceName.toLowerCase(), 'airbyte', 'freshness'])
    .query(ctx =>
      "SELECT _airbyte_extracted_at AS last_airbyte_sync_at " +
      "FROM `" + heartbeatProject + "." + heartbeatDataset + "." + heartbeatTable + "` " +
      "HAVING last_airbyte_sync_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + freshnessHours + " HOUR)"
    )
}