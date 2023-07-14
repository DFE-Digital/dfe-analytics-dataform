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
      environment: "environment which this configuration of dfe-analytics applied to.",
      version: "The version of dfe-analytics that was used to stream events between these timestamps.",
      pseudonymise_web_request_user_id: "TRUE if request_user_id in " + params.bqEventsTableName + " was pseudonymised using SHA256 between these dates, or if it appears to be in the absence of an initialise_analytics event for this time period. FALSE if not."
    }
  }).query(ctx => `
    SELECT
      occurred_at AS valid_from,
      FIRST_VALUE(occurred_at) OVER (PARTITION BY environment ORDER BY occurred_at ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS valid_to,
      environment,
      ${data_functions.eventDataExtract("data", "analytics_version")} AS version,
      CAST(JSON_VALUE(${data_functions.eventDataExtract("data", "config")}, "$.pseudonymise_web_request_user_id") AS BOOLEAN) AS pseudonymise_web_request_user_id
    FROM
      ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
    WHERE
      event_type = "initialise_analytics"
    UNION ALL
    SELECT
      (
        SELECT
          MIN(occurred_at)
        FROM
          ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
      ) AS valid_from,
      (
        SELECT
          MIN(occurred_at)
        FROM
          ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
        WHERE
          event_type = "initialise_analytics"
      ) AS valid_to,
      (
        SELECT
          IF(
            COUNT(DISTINCT environment) = 1,
            ANY_VALUE(environment),
            NULL)
        FROM
          ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
        WHERE
          occurred_at <
            (
              SELECT
                MIN(occurred_at)
              FROM
                ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
              WHERE
                event_type = "initialise_analytics"
            )
      ) AS environment,
      CAST(NULL AS STRING) AS version,
      (
        SELECT
        /* Attempt to work out whether all user_ids match SHA256 output format or are null; if so set pseudonymise_web_request_user_id to TRUE */
          LOGICAL_AND(
            REGEXP_CONTAINS(user_id, r"^[0-9a-z]*$")
            AND REGEXP_CONTAINS(user_id, r"[a-z]")
            AND REGEXP_CONTAINS(user_id, r"[0-9]")
          )
        FROM
          ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
        WHERE
          occurred_at <
            (
              SELECT
                MIN(occurred_at)
              FROM
                ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
              WHERE
                event_type = "initialise_analytics"
            )
      ) AS pseudonymise_web_request_user_id
  `)
}
