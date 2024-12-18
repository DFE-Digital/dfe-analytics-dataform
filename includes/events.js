const {decodeUriComponent} = require('./data_functions');
const {standardisePathQuery} = require('./data_functions');

module.exports = (params) => {
    return publish("events_" + params.eventSourceName, {
        ...params.defaultConfig,
        type: "incremental",
        protected: false,
        bigquery: {
            partitionBy: "DATE(occurred_at)",
            clusterBy: ["event_type", "request_uuid"],
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
            request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that was this event or caused this event.",
            request_path_and_query: "This is a string containing both the request path and request query. The request query is ordered alphabetically to ensure consistency between the request_path_and_query and referer_request_path_and_query fields.",
            request_user_agent: "The user agent of the web request that either is this event or caused this event. Allows a user's browser and operating system to be identified.",
            request_referer: "The URL of any page the user was viewing when they initiated the web request that either is this event or caused this event. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
            request_referer_domain: "This is a string containing the domain extracted from the request referer URL.",
            request_referer_path_and_query: "This is a string containing both the referer request path and referer request query extracted from the request referer URL. The request query is ordered alphabetically to ensure consistency between the referer_request_path_and_query and request_path_and_query fields.",
            response_content_type: "Content type of any data that was returned to the browser following the web request that either was this event or caused this event. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
            response_status: "HTTP response code returned by the application in response to the web request that either was this event or caused this event. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
            data: {
                description: "ARRAY of STRUCTs, each with a key and a value. Contains a set of data points appropriate to the event_type of this event. For example, if this event was an entity create, update, delete or import event, data will contain the values of each field in the database after this event took place - according to the settings in the analytics.yml configured for this instance of dfe-analytics. Value be anonymised as a one way hash, depending on configuration settings.",
                columns: {
                    key: "Name of the field in the entity_table_name table in the database after it was created or updated, or just before it was imported or destroyed.",
                    value: "Contents of the field in the database after it was created or updated, or just before it was imported or destroyed."
                }
            },
            hidden_data: {
                description: "The same as 'data', except dfe-analytics-dataform will attach a policy tag to this field and fields in other tables generated by it to allow it to be - depending on GCP policy tag configuration - either masked or hidden from users without permission to access it.",
                columns: {
                    key: "Name of the field in the entity_table_name table in the database after it was created or updated, or just before it was imported or destroyed.",
                    value: {
                        description: "Contents of the field in the database after it was created or updated, or just before it was imported or destroyed.",
                        bigqueryPolicyTags: params.hiddenPolicyTagLocation ? [params.hiddenPolicyTagLocation] : []
                    }
                }
            },
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
  earliest_web_request_event_for_request AS (
  SELECT DISTINCT
    occurred_at,
    request_uuid,
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
    ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"} AS web_request
  WHERE
    event_type = "web_request"
  /* Process web requests as far back as the day before the timestamp we're updating this table from, to ensure that we do find the web request for each non-web request event, even if the non-web request event occurred the other side of event_timestamp_checkpoint from the web request event that caused it */
  /* This uses the DATE() in order to take advantage of partitioning on the table to reduce query costs. */
    AND DATE(occurred_at) >= DATE(TIMESTAMP_SUB(event_timestamp_checkpoint, INTERVAL 1 DAY))
    ${params.bqEventsTableNameSpace ? `AND namespace = '${params.bqEventsTableNameSpace}'` : ``}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY request_uuid ORDER BY occurred_at ASC) = 1
),
event_with_web_request_data AS (
  SELECT DISTINCT
    event.occurred_at,
    event.request_uuid,
    event.event_type,
    event.environment,
    event.namespace,
    event.data,
    event.hidden_data,
    event.entity_table_name,
    COALESCE(event.request_path,earliest_web_request_event_for_request.request_path) AS request_path,
    COALESCE(event.user_id,earliest_web_request_event_for_request.request_user_id) AS request_user_id,
    COALESCE(event.request_method,earliest_web_request_event_for_request.request_method) AS request_method,
    COALESCE(event.request_user_agent,earliest_web_request_event_for_request.request_user_agent) AS request_user_agent,
    COALESCE(event.request_referer,earliest_web_request_event_for_request.request_referer) AS request_referer,
    IF(ARRAY_LENGTH(event.request_query)>0,event.request_query,earliest_web_request_event_for_request.request_query) AS request_query,
    COALESCE(event.response_status,earliest_web_request_event_for_request.response_status) AS response_status,
    COALESCE(event.response_content_type,earliest_web_request_event_for_request.response_content_type) AS response_content_type,
    COALESCE(event.anonymised_user_agent_and_ip,earliest_web_request_event_for_request.anonymised_user_agent_and_ip) AS anonymised_user_agent_and_ip
  FROM
    ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"} AS event
    LEFT JOIN earliest_web_request_event_for_request
    ON event.request_uuid = earliest_web_request_event_for_request.request_uuid
    AND event.event_type != "web_request"
  WHERE
    event.occurred_at > event_timestamp_checkpoint
  ${params.bqEventsTableNameSpace ? `AND namespace = '${params.bqEventsTableNameSpace}'` : ``}),
events_with_path_and_query AS (
  SELECT *,
  -- The purpose of the below block is to produce the request_path_and_query using CONCAT(request_path, '?', request_query). As CONCAT will return NULL if the request_query is NULL, we use COALESCE to replace a NULL query with ''. This ensures this field is never NULL if a request_path exists. 
  CONCAT(
  request_path,
  COALESCE(
    CONCAT('?', 
      REPLACE((
        SELECT STRING_AGG(CONCAT(rq.key, '=', value), '&')
        FROM UNNEST(event_with_web_request_data.request_query) AS rq,
             UNNEST(rq.value) AS value
      ), ' ', '+')), '')) AS request_path_and_query,
  REGEXP_EXTRACT(request_referer, r'https?:\/\/([^\/]+)') AS request_referer_domain,
  ${decodeUriComponent("REGEXP_EXTRACT(request_referer, r'https?:\/\/[^\/]+(\/.*)')")} AS request_referer_path_and_query,
  FROM event_with_web_request_data
)
SELECT
  events_with_path_and_query.occurred_at,
  events_with_path_and_query.request_uuid,
  events_with_path_and_query.event_type,
  events_with_path_and_query.environment,
  events_with_path_and_query.namespace,
  events_with_path_and_query.data,
  events_with_path_and_query.hidden_data,
  events_with_path_and_query.entity_table_name,
  events_with_path_and_query.request_path,
  events_with_path_and_query.request_query,
  ${standardisePathQuery("events_with_path_and_query.request_path_and_query")} AS request_path_and_query,
  events_with_path_and_query.request_user_id,
  events_with_path_and_query.request_method,
  events_with_path_and_query.request_user_agent,
  events_with_path_and_query.request_referer,
  events_with_path_and_query.request_referer_domain,
  ${standardisePathQuery("events_with_path_and_query.request_referer_path_and_query")} AS request_referer_path_and_query,
  events_with_path_and_query.response_status,
  events_with_path_and_query.response_content_type,
  events_with_path_and_query.anonymised_user_agent_and_ip,
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
  events_with_path_and_query
  `).preOps(ctx => `
    DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.incremental() ? `
          SELECT MAX(occurred_at) FROM ${ctx.self()}
        ` : `
          SELECT TIMESTAMP("2000-01-01")
        `});
    /* Update the data retention schedule for the source events table if one is specified */
    ALTER TABLE ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
      SET OPTIONS (partition_expiration_days = ${params.expirationDays || `NULL`});
    /* Delete web requests from the source events table which are older than the specified web request event data retention schedule if it is shorter than the top level schedule */
    ${params.webRequestEventExpirationDays ? `
      DELETE FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
        WHERE event_type = "web_request" AND DATE(occurred_at) < CURRENT_DATE - ${params.webRequestEventExpirationDays};
      ${ctx.incremental() ?
        `DELETE FROM ${ctx.self()}
          WHERE event_type = "web_request" AND DATE(occurred_at) < CURRENT_DATE - ${params.webRequestEventExpirationDays};
        ` : ``}
    ` : ``}
    /* Delete data for the configured table level retention schedule if one is specified and it is shorter than the top level schedule */
    ${params.dataSchema.some(tableSchema => tableSchema.expirationDays && tableSchema.entityTableName) ? `
      DELETE FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
        WHERE
          event_type IN ("create_entity", "update_entity", "delete_entity", "import_entity", "entity_table_check_scheduled", "entity_table_check_import")
          AND (${params.dataSchema.filter(tableSchema => tableSchema.expirationDays && tableSchema.entityTableName).map(tableSchema => `\n(entity_table_name = "${tableSchema.entityTableName}"
            AND DATE(occurred_at) < CURRENT_DATE - ${tableSchema.expirationDays})`).join(' OR ')})
          ;
      ${ctx.incremental() ? `
        DELETE FROM ${ctx.self()}
          WHERE
            event_type IN ("create_entity", "update_entity", "delete_entity", "import_entity", "entity_table_check_scheduled", "entity_table_check_import")
            AND (${params.dataSchema.filter(tableSchema => tableSchema.expirationDays && tableSchema.entityTableName).map(tableSchema => `\n(entity_table_name = "${tableSchema.entityTableName}"
              AND DATE(occurred_at) < CURRENT_DATE - ${tableSchema.expirationDays})`).join(' OR ')})
            ;
      ` : ``}
    ` : ``}

    /* Delete data for the configured custom event level retention schedule if one is specified and it is shorter than the top level schedule */
    ${params.customEventSchema.some(customEvent => customEvent.expirationDays && customEvent.eventType) ? `
      DELETE FROM ${"`" + params.bqProjectName + "." + params.bqDatasetName + "." + params.bqEventsTableName + "`"}
        WHERE
          ${params.customEventSchema.filter(customEvent => customEvent.expirationDays && customEvent.eventType).map(customEvent => `
          (event_type = "${customEvent.eventType}"
          AND DATE(occurred_at) < CURRENT_DATE - ${customEvent.expirationDays})`).join(' OR ')};
      ${ctx.incremental() ? `
        DELETE FROM ${ctx.self()}
          WHERE
            ${params.customEventSchema.filter(customEvent => customEvent.expirationDays && customEvent.eventType).map(customEvent => `
            (event_type = "${customEvent.eventType}"
            AND DATE(occurred_at) < CURRENT_DATE - ${customEvent.expirationDays})`).join(' OR ')}
            ;
      ` : ``}
    ` : ``}

    /* Uses the Woothee Javascript library to categorise user agents by user category (PC i.e. desktop, smartphone, mobile phone, crawler, applicance, unknown or misc), browser name, browser version, operating system, browser vendor and operating system version. To function correctly this script needs to be stored in Google Cloud Storage at the public URL below. The latest version of this script can be found at https://github.com/woothee/woothee-js/blob/master/release/woothee.js .*/
    CREATE TEMP FUNCTION parseUserAgent(user_agent STRING)
    RETURNS STRUCT < category STRING, name STRING, version STRING, os STRING, vendor STRING, os_version STRING >
      LANGUAGE js
      AS "return {category:woothee.parse(user_agent).category,name:woothee.parse(user_agent).name,version:woothee.parse(user_agent).version,os:woothee.parse(user_agent).os,vendor:woothee.parse(user_agent).vendor,os_version:woothee.parse(user_agent).os_version};"
      OPTIONS(library = 'https://storage.googleapis.com/public-dfe-analytics-dataform-scripts-cross-service/woothee.js')`)
  .postOps(ctx => `
    ALTER TABLE ${ctx.self()}
      SET OPTIONS (partition_expiration_days = ${params.expirationDays || `NULL`});
    `)
}
