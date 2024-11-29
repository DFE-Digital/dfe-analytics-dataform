module.exports = (params) => {
  return publish("dfe_analytics_configuration_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "table",
    protected: false,
    bigquery: {
      partitionBy: "DATE(valid_to)",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    tags: [params.eventSourceName.toLowerCase()],
    description: "Configuration versions for dfe-analytics events streamed from " + params.eventSourceName + " into the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project.",
    dependencies: params.dependencies,
    columns: {
      valid_from: "Timestamp at which this configuration of dfe-analytics began to apply.",
      valid_to: "Timestamp at which this configuration of dfe-analytics stopped applying. NULL if this is the current configuration.",
      version: "The version of dfe-analytics that was used to stream events between these timestamps."
    }
  }).query(ctx => `
    SELECT
      occurred_at AS valid_from,
      FIRST_VALUE(occurred_at) OVER (ORDER BY occurred_at ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS valid_to,
      ${data_functions.eventDataExtract("data", "analytics_version")} AS version
    FROM
      ${ctx.ref("events_" + params.eventSourceName)}
    WHERE
      event_type = "initialise_analytics"
    UNION ALL
    SELECT
      (
        SELECT
          MIN(occurred_at)
        FROM
          ${ctx.ref("events_" + params.eventSourceName)}
      ) AS valid_from,
      (
        SELECT
          MIN(occurred_at)
        FROM
          ${ctx.ref("events_" + params.eventSourceName)}
        WHERE
          event_type = "initialise_analytics"
      ) AS valid_to,
      CAST(NULL AS STRING) AS version
  `)
}
