module.exports = (params) => {
  var namespaceFilterSql = '';
  if (params.bqEventsTableNameSpace) {
    namespaceFilterSql = `AND namespace = '${params.bqEventsTableNameSpace}'`;
  }
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
    tags: [params.eventSourceName.toLowerCase()],
    description: "Initial transformation of the events table streamed from " + params.eventSourceName + " into the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project.",
    dependencies: params.dependencies,
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
      anonymised_user_agent_and_ip: "One way hash of a combination of the user's IP address and user agent. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
      device_category: "The category of device used to cause this event - desktop, mobile, bot or unknown.",
      browser_name: "The name of the browser used to cause this event.",
      browser_version: "The version of the browser used to cause this event.",
      operating_system_name: "The name of the operating system used to cause this event.",
      operating_system_vendor: "The vendor of the operating system used to cause this event.",
      operating_system_version: "The version of the operating system used to cause this event."
    }
  }).query(ctx => `WITH
  minimal_earliest_event_for_web_request AS (
  SELECT
    request_uuid,
    MIN(occurred_at) AS occurred_at
  FROM
    ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)}
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
     ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)} AS web_request
  ON
    minimal_earliest_event_for_web_request.request_uuid = web_request.request_uuid
    AND minimal_earliest_event_for_web_request.occurred_at = web_request.occurred_at
  WHERE
    event_type = "web_request"
    /* Process web requests as far back as 1 day before the timestamp we're updating this table from, to ensure that we do find the web request for each non-web request event, even if the non-web request event occurred the other side of event_timestamp_checkpoint from the web request event that caused it */
    AND minimal_earliest_event_for_web_request.occurred_at > TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY)
    AND web_request.occurred_at > TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY)
    ${namespaceFilterSql}
),
event_with_web_request_data AS (
  SELECT
    event.occurred_at,
    event.request_uuid,
    event.event_type,
    event.environment,
    event.namespace,
    event.data,
    event.entity_table_name,
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
    ${ctx.ref(params.bqDatasetName, params.bqEventsTableName)} AS event
    LEFT JOIN earliest_event_for_web_request
    ON event.request_uuid = earliest_event_for_web_request.request_uuid
    AND event.event_type != "web_request"
  WHERE
    event.occurred_at > event_timestamp_checkpoint ${namespaceFilterSql})
SELECT
  event_with_web_request_data.*,
  IF(REGEXP_CONTAINS(request_user_agent, '(?i)(bot|http|python|scan|check|spider|curl|trend|ruby|bash|batch|verification|qwantify|nuclei|ai|crawler|perl|java|test|scoop|fetch|adreview|cortex|nessus|bitdiscovery|postplanner|faraday|restsharp|hootsuite|mattermost|shortlink|retriever|auto|scrper|alyzer|dispatch|traackr|fiddler|crowsnest|gigablast|wakelet|installatron|intently|openurl|anthill|curb|trello|inject|ahc|sleep|sysdate|=|cloudinary|statuscake|cloudfront|archive|sleuth|bingpreview|facebookexternalhit|newspaper|econtext|postmanruntime|probe)'),"bot",
  CASE parseUserAgent(request_user_agent).category
    WHEN "smartphone" THEN "mobile"
    WHEN "pc" THEN "desktop"
    WHEN "crawler" THEN "bot"
  ELSE
  "unknown"
  END)
  AS device_category,
  REPLACE(parseUserAgent(request_user_agent).name,"UNKNOWN","unknown") AS browser_name,
  REPLACE(parseUserAgent(request_user_agent).version,"UNKNOWN","unknown") AS browser_version,
  REPLACE(parseUserAgent(request_user_agent).os,"UNKNOWN","unknown") AS operating_system_name,
  REPLACE(parseUserAgent(request_user_agent).vendor,"UNKNOWN","unknown") AS operating_system_vendor,
  REPLACE(parseUserAgent(request_user_agent).os_version,"UNKNOWN","unknown") AS operating_system_version
FROM
  event_with_web_request_data
  `).preOps(ctx => `
    DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), `SELECT MAX(occurred_at) FROM ${ctx.self()}`, `SELECT TIMESTAMP("2000-01-01")`)});
    /* Uses the Woothee Javascript library to categorise user agents by user category (PC i.e. desktop, smartphone, mobile phone, crawler, applicance, unknown or misc), browser name, browser version, operating system, browser vendor and operating system version. To function correctly this script needs to be stored in Google Cloud Storage at the public URL below. The latest version of this script can be found at https://github.com/woothee/woothee-js/blob/master/release/woothee.js .*/
    CREATE TEMP FUNCTION parseUserAgent(user_agent STRING)
    RETURNS STRUCT < category STRING, name STRING, version STRING, os STRING, vendor STRING, os_version STRING >
      LANGUAGE js
      AS "return {category:woothee.parse(user_agent).category,name:woothee.parse(user_agent).name,version:woothee.parse(user_agent).version,os:woothee.parse(user_agent).os,vendor:woothee.parse(user_agent).vendor,os_version:woothee.parse(user_agent).os_version};"
      OPTIONS(library = 'https://storage.googleapis.com/public-dfe-analytics-dataform-scripts/woothee.js')`
  )
}