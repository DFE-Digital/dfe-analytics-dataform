/* Checks that the Airbyte heartbeat table has been updated recently. Triggers an assertion failure if no new data has arrived within the configured number of hours. */

module.exports = (params) => {
  if (!params.enableAirbyteSource) return null;

  const heartbeat = params.airbyteHeartbeat;
  const projectName = params.bqProjectName;

  if (heartbeat.disableFreshnessCheckDuringRange && params.disableAssertionsNow) {
    return null;
  }

  return assert(
    params.eventSourceName + "_airbyte_global_data_not_fresh", {
      ...params.defaultConfig,
      type: "assertion",
      description: `Checks that the Airbyte heartbeat table (${heartbeat.datasetName}.${heartbeat.tableName}) has been updated within the last ${heartbeat.freshnessHours} hours. Number of hours to wait before triggering an assertion failure, if no new data has been received from Airbyte.`
    }
  ).tags([params.eventSourceName.toLowerCase(), 'airbyte'])
    .query(ctx =>
      "SELECT _airbyte_extracted_at AS last_airbyte_sync_at " +
      "FROM `" + projectName + "." + heartbeat.datasetName + "." + heartbeat.tableName + "`" +
      "WHERE _airbyte_extracted_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL " + heartbeat.freshnessHours + " HOUR)"
    )
}