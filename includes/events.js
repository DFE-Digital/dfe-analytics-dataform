module.exports = (params) => {
  return publish("events_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      clusterBy: ["event_type"],
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    description: "Initial transformation of the events table streamed from " + params.eventSourceName + " into the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project.",
    columns: {
      occurred_at: "The timestamp at which the event occurred in the application.",
      event_type: "The type of the event, for example web_request. This determines the schema of the data which will be included in the data field.",
      environment: "The application environment that the event was streamed from.",
      namespace: "The namespace of the instance of dfe-analytics that streamed this event. For example this might identify the name of the service that streamed the event.",
      request_user_id: "If a user was logged in when they sent a web request event that is, or caused, this event, then this is the UID of this user.",
      request_uuid: "UUID of the web request that either is this event, or that caused this event.",
      request_method: "Whether the web request that either is this event, or caused this event, was a GET or a POST request.",
      request_path: "The path, starting with a / and excluding any query parameters, of the web request that either is this event, or caused this event.",
      request_user_agent: "The user agent of the web request that either is this event or caused this event. Allows a user's browser and operating system to be identified.",
      request_referer: "The URL of any page the user was viewing when they initiated the web request that either is this event or caused this event. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
      request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that was this event or caused this event.",
      response_content_type: "Content type of any data that was returned to the browser following the web request that either was this event or caused this event. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
      response_status: "HTTP response code returned by the application in response to the web request that either was this event or caused this event. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
      data: "ARRAY of STRUCTs, each with a key and a value. Contains a set of data points appropriate to the event_type of this event. For example, if this event was an entity create, update, delete or import event, data will contain the values of each field in the database after this event took place - according to the settings in the analytics.yml configured for this instance of dfe-analytics. Value be anonymised as a one way hash, depending on configuration settings.",
      entity_table_name: "If event_type was an entity create, update, delete or import event, the name of the table in the database that this entity is stored in. NULL otherwise.",
      anonymised_user_agent_and_ip: "One way hash of a combination of the user's IP address and user agent. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates."
    }
  }).query(ctx => `WITH
  minimal_earliest_event_for_web_request AS (
  SELECT
    request_uuid,
    MIN(occurred_at) AS occurred_at
  FROM
    ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)}
  WHERE
    event_type = "web_request"
    AND occurred_at > TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY)
  GROUP BY
    request_uuid),
  earliest_event_for_web_request AS (
  SELECT
    minimal_earliest_event_for_web_request.occurred_at,
    minimal_earliest_event_for_web_request.request_uuid,
    request_path,
    user_id AS request_user_id,
    request_method,
    request_user_agent,
    request_referer,
    request_query,
    response_content_type,
    response_status,
    anonymised_user_agent_and_ip
  FROM
    minimal_earliest_event_for_web_request
  LEFT JOIN
     ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)} AS web_request
  ON
    minimal_earliest_event_for_web_request.request_uuid = web_request.request_uuid
    AND minimal_earliest_event_for_web_request.occurred_at = web_request.occurred_at
  WHERE
    event_type = "web_request"
    /* Process web requests as far back as 1 day before the timestamp we're updating this table from, to ensure that we do find the web request for each non-web request event, even if the non-web request event occurred the other side of event_timestamp_checkpoint from the web request event that caused it */
    AND minimal_earliest_event_for_web_request.occurred_at > TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY)
    AND web_request.occurred_at > TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY)
)
SELECT
  event.*
EXCEPT(
    request_path,
    user_id,
    request_method,
    request_user_agent,
    request_referer,
    request_query,
    response_content_type,
    response_status,
    anonymised_user_agent_and_ip
  ),
  COALESCE(event.request_path,earliest_event_for_web_request.request_path) AS request_path,
  COALESCE(event.user_id,earliest_event_for_web_request.request_user_id) AS request_user_id,
  COALESCE(event.request_method,earliest_event_for_web_request.request_method) AS request_method,
  COALESCE(event.request_user_agent,earliest_event_for_web_request.request_user_agent) AS request_user_agent,
  COALESCE(event.request_referer,earliest_event_for_web_request.request_referer) AS request_referer,
  IF(ARRAY_LENGTH(event.request_query)>0,event.request_query,earliest_event_for_web_request.request_query) AS request_query,
  COALESCE(event.response_status,earliest_event_for_web_request.response_status) AS response_status,
  COALESCE(event.response_content_type,earliest_event_for_web_request.response_content_type) AS response_content_type,
  COALESCE(event.anonymised_user_agent_and_ip,earliest_event_for_web_request.anonymised_user_agent_and_ip) AS anonymised_user_agent_and_ip
FROM
  ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)} AS event
  LEFT JOIN earliest_event_for_web_request ON DATE(event.occurred_at) = DATE(earliest_event_for_web_request.occurred_at)
  AND event.request_uuid = earliest_event_for_web_request.request_uuid
WHERE
  event.occurred_at > event_timestamp_checkpoint`).preOps(ctx => `DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(),`SELECT MAX(occurred_at) FROM ${ctx.self()}`,`SELECT TIMESTAMP("2000-01-01")`)}
    