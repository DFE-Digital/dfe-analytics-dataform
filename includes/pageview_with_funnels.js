module.exports = (params) => {
  const stepNumbers = Array.from({length: params.funnelDepth}, (_, i) => i + 1);
  const stepFieldMetadata = stepNumbers.map(stepNumber => ({
        ["preceding_request_path_grouped_" + stepNumber]: `The request_path_grouped for the pageview that took place ${stepNumber} step(s) before this pageview . If this was the first pageview for that day, this will be the string 'Arrived on site' rather than NULL. If there was a pageview that took place ${stepNumber+1} steps before this pageview, but the referer of this pageview did not match the path for that pageview (so it wasn't a click) then this will be the string 'Different window or tab' rather than NULL.`,
        ["following_request_path_grouped_" + stepNumber]: `The request_path_grouped for the pageview that took place ${stepNumber} step(s) after this pageview . If this was the last pageview for that day, this will be the string 'Left site' rather than NULL. If there was a pageview that took place ${stepNumber+1} steps after this pageview, but the referer of this pageview did not match the path for that pageview (so it wasn't a click) then this will be the string 'Different window or tab' rather than NULL.`,
      ["seconds_between_preceding_steps_" + (stepNumber - 1) + "_and_" + stepNumber]: `The number of seconds (to microsecond precision) that elapsed between the pageview that took place ${stepNumber - 1} step before this pageview and the pageview that took place ${stepNumber} steps before this pageview (and so on for further steps in other fields with names that follow this pattern).`,
      ["seconds_between_following_steps_" + (stepNumber - 1) + "_and_" + stepNumber]: `The number of seconds (to microsecond precision) that elapsed between the pageview that took place ${stepNumber - 1} step after this pageview and the pageview that took place ${stepNumber} steps after this pageview (and so on for further steps in other fields with names that follow this pattern).`
      })
    );
  function stepFields() {
  var sqlToReturn = '';
  if((!(Number.isInteger(params.funnelDepth))) || params.funnelDepth < 1) {
    throw new Error(`${params.funnelDepth} is not valid. funnelDepth must be a positive integer.`);
  }
  for (let i = 2; i <= params.funnelDepth; i++) {
    sqlToReturn += `
  CASE
    WHEN preceding_user_requests[SAFE_ORDINAL(${i})].request_path_grouped IS NOT NULL THEN preceding_user_requests[SAFE_ORDINAL(${i})].request_path_grouped
    WHEN preceding_user_requests[SAFE_ORDINAL(${i-1})].request_path_grouped IS NOT NULL
  AND total_number_of_preceding_steps_in_funnel = ${i} THEN "Different window or tab"
    WHEN preceding_user_requests[SAFE_ORDINAL(${i-1})].request_path_grouped IS NOT NULL THEN "Arrived on site"
    ELSE NULL
  END AS preceding_request_path_grouped_${i},
  CASE
    WHEN following_user_requests[SAFE_ORDINAL(${i})].request_path_grouped IS NOT NULL THEN following_user_requests[SAFE_ORDINAL(${i})].request_path_grouped
    WHEN following_user_requests[SAFE_ORDINAL(${i-1})].request_path_grouped IS NOT NULL
  AND total_number_of_following_steps_in_funnel = ${i} THEN "Different window or tab"
    WHEN following_user_requests[SAFE_ORDINAL(${i-1})].request_path_grouped IS NOT NULL THEN "Left site"
    ELSE NULL
  END AS following_request_path_grouped_${i},
  CASE
    WHEN preceding_user_requests[SAFE_ORDINAL(${i})].request_path_grouped IS NOT NULL THEN preceding_user_requests[SAFE_ORDINAL(${i-1})].seconds_since_previous_step
    ELSE NULL
  END AS seconds_between_preceding_steps_${i-1}_and_${i},
  CASE
    WHEN following_user_requests[SAFE_ORDINAL(${i})].request_path_grouped IS NOT NULL THEN following_user_requests[SAFE_ORDINAL(${i-1})].seconds_until_next_step
    ELSE NULL
  END AS seconds_between_following_steps_${i-1}_and_${i},\n`;
    }
  return sqlToReturn;
  }
  return publish("pageview_with_funnels_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    description: "Pageview events from the events table streamed from " + params.eventSourceName + " into the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project, with fields added containing the paths for the previous and following " + params.funnelDepth + " pageview events in strict time AND referer order, numbered to allow funnel analysis. Fields containing the time between each step are also included.",
    columns: Object.assign({
      occurred_at: "The timestamp at which the event occurred in the application.",
      event_type: "The type of the event, for example web_request. This determines the schema of the data which will be included in the data field.",
      environment: "The application environment that the event was streamed from.",
      namespace: "The namespace of the instance of dfe-analytics that streamed this event. For example this might identify the name of the service that streamed the event.",
      request_user_id: "If a user was logged in when they sent a web request event that is, or caused, this event, then this is the UID of this user.",
      request_uuid: "UUID of the web request that either is this event, or that caused this event.",
      request_method: "Whether the web request that either is this event, or caused this event, was a GET or a POST request.",
      request_path: "The path, starting with a / and excluding any query parameters, of the web request that either is this event, or caused this event.",
      request_path_grouped: "request_path, except with any string between two forward slashes (/) that contains a digit (0-9) replaced with the string 'UID'. Useful for grouping pages in funnel analysis.",
      request_user_agent: "The user agent of the web request that either is this event or caused this event. Allows a user's browser and operating system to be identified.",
      request_referer: "The URL of any page the user was viewing when they initiated the web request that either is this event or caused this event. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
      request_referer_path: "The path part of request_referer, starting with a / and excluding any query parameters.",
      request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that was this event or caused this event.",
      request_referer_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters present in request_referer.",
      response_content_type: "Content type of any data that was returned to the browser following the web request that either was this event or caused this event. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
      response_status: "HTTP response code returned by the application in response to the web request that either was this event or caused this event. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
      anonymised_user_agent_and_ip: "One way hash of a combination of the user's IP address and user agent. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
      device_category: "The category of device used to cause this event - desktop, mobile, bot or unknown.",
      browser_name: "The name of the browser used to cause this event.",
      browser_version: "The version of the browser used to cause this event.",
      operating_system_name: "The name of the operating system used to cause this event.",
      operating_system_vendor: "The vendor of the operating system used to cause this event.",
      operating_system_version: "The version of the operating system used to cause this event.",
      seconds_since_preceding_step_1: "The number of seconds (to microsecond precision) that elapsed between this pageview and the pageview that took place 1 step before this pageview.",
      seconds_until_following_step_1: "The number of seconds (to microsecond precision) that elapsed between this pageview and the pageview that took place 1 step after this pageview."
    }, ...stepFieldMetadata)
  }).query(ctx => `
WITH
  web_request AS (
  /* Filter out non-web request events, non-HTML pages and bot events, remove URI formatted characters from the referer and flatten repeated parameter values in the query string into separate parameters */
  SELECT
    * EXCEPT(request_query,
      request_referer,
      DATA,
      entity_table_name),
    ARRAY(
    SELECT
      AS STRUCT query.key,
      value
    FROM
      UNNEST(request_query) AS query,
      UNNEST(value) AS value) AS request_query,
    REPLACE(DECODE_URI_COMPONENT(request_referer),"+"," ") AS request_referer
  FROM
    ${ctx.ref("events_" + params.eventSourceName)}
  WHERE
    event_type="web_request"
    AND device_category != "bot"
    AND CONTAINS_SUBSTR(response_content_type,
      "text/html")
    AND DATE(occurred_at) < CURRENT_DATE
    AND DATE(occurred_at) > event_date_checkpoint),
  web_request_with_processed_referer AS (
  SELECT
    *,
    REGEXP_EXTRACT(request_referer, r"[^\\/](\\/[^\\/][^?]*)(?:\\?|$)") AS request_referer_path,
    ARRAY(
    SELECT
      AS STRUCT REGEXP_EXTRACT(string, r"^([^=]+)=") AS key,
      REGEXP_EXTRACT(string, r"=([^=]+)$") AS value
    FROM
      UNNEST(REGEXP_EXTRACT_ALL(request_referer, r"[?&]([^&]+)(?:&|$)")) AS string) AS request_referer_query,
    ARRAY_TO_STRING( ARRAY(
      SELECT
      IF
        ( REGEXP_CONTAINS(path_part, "[0-9]"),
          "UID",
          path_part )
      FROM
        UNNEST(SPLIT(request_path, "/")) AS path_part ), "/" ) AS request_path_grouped
  FROM
    web_request),
  web_request_with_funnels AS (
  /* Give each web request two ARRAYs of STRUCTs containing its ${params.funnelDepth} preceding and following web requests for the user who made it. Limited to ${params.funnelDepth} to ensure the query is able to run within BQ limits */
  SELECT
    *,
    ARRAY_AGG(STRUCT(occurred_at,
        request_uuid,
        request_referer,
        request_path,
        request_path_grouped,
        request_query,
        request_referer_path,
        request_referer_query)) OVER (PARTITION BY anonymised_user_agent_and_ip, DATE(occurred_at)
    ORDER BY
      occurred_at ASC ROWS BETWEEN ${params.funnelDepth} PRECEDING
      AND 1 PRECEDING) AS preceding_user_requests,
    ARRAY_AGG(STRUCT(occurred_at,
        request_uuid,
        request_referer,
        request_path,
        request_path_grouped,
        request_query,
        request_referer_path,
        request_referer_query)) OVER (PARTITION BY anonymised_user_agent_and_ip, DATE(occurred_at)
    ORDER BY
      occurred_at ASC ROWS BETWEEN 1 FOLLOWING
      AND ${params.funnelDepth} FOLLOWING) AS following_user_requests
  FROM
    web_request_with_processed_referer),
  web_request_with_numbered_funnels AS (
  SELECT
    * EXCEPT (preceding_user_requests,
      following_user_requests),
    ARRAY(
    SELECT
      AS STRUCT *,
      ROW_NUMBER() OVER all_preceding_requests_in_reverse_time_order AS step_number_backwards,
      FIRST_VALUE(preceding_request.request_referer_path) OVER next_request AS next_referer_path,
      FIRST_VALUE(preceding_request.request_referer_query) OVER next_request AS next_referer_query,
      SAFE_DIVIDE(TIMESTAMP_DIFF(LAG(preceding_request.occurred_at) OVER all_preceding_requests_in_reverse_time_order,preceding_request.occurred_at, MICROSECOND), 1000000) AS seconds_until_next_step,
      SAFE_DIVIDE(TIMESTAMP_DIFF(preceding_request.occurred_at, LEAD(preceding_request.occurred_at) OVER all_preceding_requests_in_reverse_time_order, MICROSECOND), 1000000) AS seconds_since_previous_step
    FROM
      UNNEST(preceding_user_requests) AS preceding_request
    WINDOW
      all_preceding_requests_in_reverse_time_order AS (
      ORDER BY
        preceding_request.occurred_at DESC ),
      next_request AS (
      ORDER BY
        preceding_request.occurred_at ASC ROWS BETWEEN 1 FOLLOWING
        AND 1 FOLLOWING)) AS preceding_user_requests,
    ARRAY(
    SELECT
      AS STRUCT *,
      ROW_NUMBER() OVER all_following_requests_in_increasing_time_order AS step_number_forwards,
      FIRST_VALUE(following_request.request_path) OVER previous_request AS previous_request_path,
      FIRST_VALUE(following_request.request_query) OVER previous_request AS previous_request_query,
      SAFE_DIVIDE(TIMESTAMP_DIFF(following_request.occurred_at, LAG(following_request.occurred_at) OVER all_following_requests_in_increasing_time_order, MICROSECOND), 1000000) AS seconds_since_previous_step,
      SAFE_DIVIDE(TIMESTAMP_DIFF(LEAD(following_request.occurred_at) OVER all_following_requests_in_increasing_time_order, following_request.occurred_at, MICROSECOND), 1000000) AS seconds_until_next_step
    FROM
      UNNEST(following_user_requests) AS following_request
    WINDOW
      all_following_requests_in_increasing_time_order AS (
      ORDER BY
        following_request.occurred_at ASC ),
      previous_request AS (
      ORDER BY
        following_request.occurred_at ASC ROWS BETWEEN 1 PRECEDING
        AND 1 PRECEDING)) AS following_user_requests
  FROM
    web_request_with_funnels),
  web_request_referral_aware AS (
  /* Ad a referred_to_next_request or referred_from_previous_request BOOL field to each next/previous request in the funnel for each event. This is TRUE if both the path and query for event N+1 match the path and query extracted from the referrer for event N */
  SELECT
    * EXCEPT (preceding_user_requests,
      following_user_requests),
    ARRAY(
    SELECT
      AS STRUCT *,
      IFNULL(preceding_request.next_referer_path = preceding_request.request_path
        AND (NOT EXISTS (
          SELECT
            *
          FROM
            UNNEST(preceding_request.next_referer_query) AS next_ref_query
          INNER JOIN
            UNNEST(preceding_request.request_query) AS this_query
          USING
            (key)
          WHERE
            (next_ref_query.key IS NULL
              OR this_query.key IS NULL
              OR next_ref_query.value != this_query.value)))
        OR (step_number_backwards = 1
          AND web_request_with_numbered_funnels.request_referer_path = preceding_request.request_path
          AND (NOT EXISTS (
            SELECT
              *
            FROM
              UNNEST(web_request_with_numbered_funnels.request_referer_query) AS next_ref_query
            INNER JOIN
              UNNEST(preceding_request.request_query) AS this_query
            USING
              (key)
            WHERE
              (next_ref_query.key IS NULL
                OR this_query.key IS NULL
                OR next_ref_query.value != this_query.value)))),
        FALSE) AS referred_to_next_request
    FROM
      UNNEST(preceding_user_requests) AS preceding_request ) AS preceding_user_requests,
    ARRAY(
    SELECT
      AS STRUCT *,
      IFNULL(following_request.previous_request_path = following_request.request_referer_path
        AND (NOT EXISTS (
          SELECT
            *
          FROM
            UNNEST(following_request.previous_request_query) AS prev_query
          INNER JOIN
            UNNEST(following_request.request_referer_query) AS this_query
          USING
            (key)
          WHERE
            (prev_query.key IS NULL
              OR this_query.key IS NULL
              OR prev_query.value != this_query.value)))
        OR (step_number_forwards = 1
          AND following_request.request_referer_path = web_request_with_numbered_funnels.request_path
          AND (NOT EXISTS (
            SELECT
              *
            FROM
              UNNEST(web_request_with_numbered_funnels.request_query) AS prev_query
            INNER JOIN
              UNNEST(following_request.request_referer_query) AS this_query
            USING
              (key)
            WHERE
              (prev_query.key IS NULL
                OR this_query.key IS NULL
                OR prev_query.value != this_query.value)))),
        FALSE) AS referred_from_previous_request
    FROM
      UNNEST(following_user_requests) AS following_request ) AS following_user_requests
  FROM
    web_request_with_numbered_funnels),
  web_request_with_unbroken_funnels_only AS (
  /* Find points in the referral chain in preceding/following_user_requests where the chain breaks - i.e. where the referer no longer matches the immediately preceding request - and exclude everything before the most recent break point */
  SELECT
    * EXCEPT(preceding_user_requests,
      following_user_requests),
    ARRAY_LENGTH(preceding_user_requests) AS total_number_of_preceding_steps_in_funnel,
    ARRAY_LENGTH(following_user_requests) AS total_number_of_following_steps_in_funnel,
    ARRAY(
    SELECT
      AS STRUCT *,
      IFNULL(MIN(
        IF
          (NOT referred_to_next_request,
            step_number_backwards,
            NULL)) OVER all_preceding_requests_in_reverse_time_order - 1,
        MAX(step_number_backwards) OVER all_preceding_requests_in_reverse_time_order) AS total_number_of_unbroken_preceding_steps_in_funnel
    FROM
      UNNEST(preceding_user_requests) AS preceding_request QUALIFY step_number_backwards <= total_number_of_unbroken_preceding_steps_in_funnel
    WINDOW
      all_preceding_requests_in_reverse_time_order AS (
      ORDER BY
        preceding_request.step_number_backwards ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING) ) AS preceding_user_requests,
    ARRAY(
    SELECT
      AS STRUCT *,
      IFNULL(MIN(
        IF
          (NOT referred_from_previous_request,
            step_number_forwards,
            NULL)) OVER all_following_requests_in_increasing_time_order - 1,
        MAX(step_number_forwards) OVER all_following_requests_in_increasing_time_order) AS total_number_of_unbroken_following_steps_in_funnel
    FROM
      UNNEST(following_user_requests) AS following_request QUALIFY step_number_forwards <= total_number_of_unbroken_following_steps_in_funnel
    WINDOW
      all_following_requests_in_increasing_time_order AS (
      ORDER BY
        following_request.step_number_forwards ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING) ) AS following_user_requests
  FROM
    web_request_referral_aware)
SELECT
  * EXCEPT(preceding_user_requests,
    following_user_requests,
    total_number_of_preceding_steps_in_funnel,
    total_number_of_following_steps_in_funnel),
  CASE
    WHEN preceding_user_requests[SAFE_ORDINAL(1)].request_path_grouped IS NOT NULL THEN preceding_user_requests[SAFE_ORDINAL(1)].request_path_grouped
    WHEN total_number_of_preceding_steps_in_funnel > 0 THEN "Different window or tab"
    WHEN total_number_of_preceding_steps_in_funnel = 0 THEN "Arrived on site"
    ELSE NULL
  END AS preceding_request_path_grouped_1,
  CASE
    WHEN following_user_requests[SAFE_ORDINAL(1)].request_path_grouped IS NOT NULL THEN following_user_requests[SAFE_ORDINAL(1)].request_path_grouped
    WHEN total_number_of_following_steps_in_funnel > 0 THEN "Different window or tab"
    WHEN total_number_of_following_steps_in_funnel = 0 THEN "Left site"
    ELSE NULL
  END AS following_request_path_grouped_1,
  SAFE_DIVIDE(TIMESTAMP_DIFF(occurred_at,preceding_user_requests[SAFE_ORDINAL(1)].occurred_at, MICROSECOND), 1000000) AS seconds_since_preceding_step_1,
  SAFE_DIVIDE(TIMESTAMP_DIFF(following_user_requests[SAFE_ORDINAL(1)].occurred_at, occurred_at, MICROSECOND), 1000000) AS seconds_until_following_step_1,
${stepFields(params.funnelDepth)}
FROM
  web_request_with_unbroken_funnels_only
`).preOps(ctx => `
    DECLARE event_date_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(),`SELECT MAX(DATE(occurred_at)) FROM ${ctx.self()}`,`SELECT DATE("2000-01-01")`)});
/* Referer URLs in events include URI-formatted codes for some characters e.g. '%20' for ' '. This UDF parses them. */
CREATE TEMP FUNCTION
  DECODE_URI_COMPONENT(url STRING) AS ((
    SELECT
      STRING_AGG(
      IF
        (REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'),
          SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))),
          y), ''
      ORDER BY
        i )
    FROM
      UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y
    WITH
    OFFSET
      AS i ));`
      )
}