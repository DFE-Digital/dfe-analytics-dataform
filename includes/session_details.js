// Referer URLs in events include URI-formatted codes for some characters e.g. '%20' for ' '. This JS function parses them.
function decodeUriComponent(url) {
  return `
    (
      SELECT
        STRING_AGG(
          IF(
            REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'),
            SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))),
            y
          ),
          '' ORDER BY i
        )
      FROM UNNEST(REGEXP_EXTRACT_ALL(${url}, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y WITH OFFSET AS i
    )
  `;
}

/*

This function is designed to standardise the request query section of urls (path and query part) to allow for accurate comparison between page_path_and_query and referer_path_and_query. 

There are two parts to the standardisation:
1. Remove duplicate key value pairs
2. Organise key value pairs alphabetically

The process is as follows:
1. IF the path_query is NULL then return NULL (necessary as the function would otherwise return a blank string)
2. IF the path_query contains a string after the '?' then process it, otherwise return a blank string. 
3. SPLIT the query section of the path_query (string following '?') into key_value_pairs (separated by '&')
4. SELECT DISTINCT key_value_pairs. This removes duplicates. It is necessary to unnest the array of key_value_pairs for this purpose. 
5. ORDER the key_value_pairs (alphabetically) and combine them into a single string (STRING_AGG)
6. CONCAT the standardised string of key_value_pairs with '?' to make it the complete query part of the url.
7. CONCAT the standardised query with the path to form the complete url (path and query part)
* SAFE_OFFSET is used wherever possible to avoid errors preventing the script from running

This function is applied to both the path url and the referer url to ensure they are formatted consistently.

 */

function standardisePathQuery(path_query) {
  return `
    CASE WHEN ${path_query} IS NOT NULL THEN
      SPLIT(${path_query}, '?')[SAFE_OFFSET(0)] || 
      IF(
        ARRAY_LENGTH(SPLIT(${path_query}, '?')) > 1,
        '?' || (
          SELECT STRING_AGG(DISTINCT key_value_pair, '&' ORDER BY key_value_pair)
          FROM UNNEST(SPLIT(SPLIT(${path_query}, '?')[SAFE_OFFSET(1)], '&')) AS key_value_pair
        ),
        ''
      )
    END
  `;
}

module.exports = (params) => {
  if (!params.enableSessionDetailsTable) {
    return true;
  }  
  return publish("session_details_" + params.eventSourceName, {
        ...params.defaultConfig,
        type: "incremental",
        protected: false,
        bigquery: {
            partitionBy: "DATE(session_start_timestamp)",
            updatePartitionFilter: "final_session_page_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)",
            labels: {
                eventsource: params.eventSourceName.toLowerCase(),
                sourcedataset: params.bqDatasetName.toLowerCase()
            }
        },
        assertions: {
                uniqueKey: ["session_id"]
        },
        tags: [params.eventSourceName.toLowerCase()],
        description: "This table contains data on sessions and accompanying metrics. The session_details table is an alternative to the standard sessions table that is produced as part of the dfe-analytics dataform pipeline. This table uses the Google Analytics definition of a session: A session is a group of user interactions with the website that that occur continuously without a break of more than 30 minutes of inactivity or until the user navigates away from the site. Therefore it will not align with the sessions_itt_mentor table. Each row is a single session. This table is used to calculate the fact_session and fact_page visit tables. These are calculated as a single table first in order to ensure the session_id aligns between the two tables. This does not include sessions or page visits from bots.",
        dependencies: params.dependencies,
        columns: {
          session_id: "The unique ID of the session",
          user_id: "UUID of the user. This is only available for users who have signed into the service during their session.",
          start_page: "The page URL of the first page visited in the session",
          exit_page: "The page URL of the last page visited in the session",
          session_start_timestamp: "Timestamp of the first page visit in the session",
          final_session_page_timestamp: "Timestamp of the last page visit in the session",
          session_time_in_seconds: "The duration of the session in seconds.",
          count_pages_visited: "The number of pages visited during the session.",
          pages_visited_details: {
              description: "The pages visited within this session and associated metrics.",
              columns: {
                  anonymised_user_agent_and_ip: "One way hash of a combination of the user's IP address and user agent. Multiple users may share a single anonymised_user_agent_and_ip.",
                  page: "The URL of the page visited",
                  page_entry_time: "Timestamp indicating when the page was entered.",
                  page_exit_time: "Timestamp indicating when the page was exited and the next page visit in the session began. This will be NULL is the user left the site after this page.",
                  duration: "The difference in seconds between page_entry_time and page_exit_time. This will be NULL is the user left the site after this page.",
                  next_step: "String indicating whether, at the end of this funnel, the user 'Left site immediately after this' or 'Visited subsequent pages'",
                  exit_page_flag: "Indicates is the user left the site after this page. This is included as there are a small number of instances where a user has no subsequent page visits but 'next step' field shows 'Visited subsequent pages'"
              }
          }
        } 
  }).query(ctx => `
  
/* 

  The session_details table is an alternative to the standard sessions table that is produced as part of the dfe-analytics dataform pipeline.

  The session_details table uses Google Analytics definition of session:

  "A session is a group of user interactions with your website that take place within a given time frame. For example a single session can contain multiple page views, events, social interactions, and ecommerce transactions."
  In the context of Google Analytics, a given timeframe refers to any sequence of user interactions on a website that occur continuously without a break of more than 30 minutes of inactivity, until the user navigates away from the site or until a new user signs in with the same anonymised_user_agent_and_ip.
  Importantly, by this definition a session is unique to a single user.

  This table is necessary to accurately calculate the following metrics of interest:
  1. Number of total sessions
  2. Number of user sessions
  3. Session duration
  4. Page depth
  5. Page duration
  6. User Journeys

  This table is used to produce the fact_session_started tables and the fact_page_visit tables. These are underpinned by the same table to ensure that the session_id is consistent between the two tables.

  */

  -- Define the base CTE with necessary fields and filters from the events dataset
WITH
  events AS (
  SELECT
    anonymised_user_agent_and_ip,
    -- Unique identifier for user's ip/device combination
    request_user_id,
    -- User ID for the session (only available if the user signs in)
    occurred_at,
    -- Timestamp of the web request
    request_path,
    -- Path requested by the user
    REGEXP_EXTRACT(request_referer, r'https?:\/\/([^\/]+)') AS request_referer_domain,
    REGEXP_EXTRACT(request_referer, r'https?:\/\/[^\/]+(\/.*)') AS request_referer_path_and_query,
    -- Where the refererer URL contains 'gov' THEN extract the path and query from the referer url and decode the referer URL to keep consistent with the page_path_and_query. This is to ensure it is consistent for linking pages in sessions together. If it does not contain 'gov', then we assume this is an external page so the full url is kept. 
    REPLACE((
      SELECT
        STRING_AGG(CONCAT(rq.key, '=', value), '&')
      FROM
        UNNEST(request_query) AS rq,
        UNNEST(rq.value) AS value), ' ', '+') AS request_query_string
    -- Concatenate all key and value pairs from the request_query arrays into a single query string. This also replaces the spaces in the newly created string with '+'.
  FROM
    ${ctx.ref("events_" + params.eventSourceName)}
    -- Source table containing web event data
  WHERE
    event_type = "web_request"
    AND device_category != "bot"
    -- Do not include bot visits
    AND CONTAINS_SUBSTR(response_content_type, "text/html")
    -- Only web page visits
    AND response_status NOT LIKE "3__"
    -- Do not include redirects 
    AND occurred_at > event_timestamp_checkpoint),
    -- only events that occurred within 24 hours of the latest session start,
  events_with_decoded_url AS (
  SELECT 
    *,
    ${decodeUriComponent("request_referer_path_and_query")} AS referer_path_and_query
  FROM 
    events),
  events_with_url AS (
  SELECT
    anonymised_user_agent_and_ip,
    request_user_id,
    occurred_at,
    request_path,
    CONCAT(request_path,
    IF
      (request_query_string IS NOT NULL
        AND request_query_string != '', CONCAT('?', request_query_string), '')) AS page_path_and_query,
    -- If a request_query_string exists, append the query string to the request_path to form the full page url.
    request_referer_domain,
    IF
    (SUBSTR(referer_path_and_query, -1) = '?', SPLIT(referer_path_and_query, "?")[0], referer_path_and_query) AS referer_path_and_query,
    -- Formatting to remove "?" if it is the last character. For consistency with page_path_and_query.
  FROM
    events_with_decoded_url
  ),
   events_with_standardised_url AS (
    select 
    anonymised_user_agent_and_ip,
    request_user_id,
    occurred_at,
    request_path,
    ${standardisePathQuery("page_path_and_query")} AS page_path_and_query,
    request_referer_domain,
    ${standardisePathQuery("referer_path_and_query")} AS referer_path_and_query
    from events_with_url
  ),
  /*

The events_with_next_visit_to_url CTE left joins, to each page, all the pages that directly follow a page visit. Importantly, multiple future pages DO NOT indicate a journey of page visits but instead suggest 'branching' page visits (the user visited visited multiple following pages from the current page.)

A following page is identified by a referer_path_and_query that matches the current pages page_path_and_query AND meets the following criteria:
1. The anonymised_user_agent is the same for both pages
2. The request_user_id is the same for both pages. This captures time-outs which are indicated by a user beging returned to the sign in page after 30 mins of inactivity. In this instance, the referral page at sign-in will show the last page the user visited, but the user_id will be set to NULL so the page will not be counted as a following page visit. 
3. The page visits occurred on the same day.
4. The following page visit ocurred AFTER the current page. 
5. The following page visit occurred after the current page, but BEFORE the next page with the same URL. This is to ensure that if a user visits a page multiple time, following pages are assigned to the most recent prior visit to that page. 

*/ 
-- The events_with_next_visit_to_url CTE adds a field showing the timestamp of the next visit to the same url of the user or anonymised_user_agent_and_ip. The purpose of this is to ensure that when 'next pages' are joined in the events_with_next_page_details, we can ensure that pages are only joined by their referral_url to the most recent visit to the same page_path_and_query. 
events_with_next_visit_to_url AS (
  SELECT
    *,
    COALESCE(
    LEAD(occurred_at) OVER (PARTITION BY request_user_id, page_path_and_query, DATE(occurred_at)
    ORDER BY
      occurred_at),
    LEAD(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip, page_path_and_query, DATE(occurred_at)
    ORDER BY
      occurred_at))
      AS next_same_page_path_and_query_occurred_at
      -- This identifies the time of the users next visit to the current page url on the same day. IF the user does not visit the same page, then the next visit to the same page by the anonymised_user_agent_and_ip is used instead.
  FROM
    events_with_standardised_url ),
  events_with_next_page_details AS (
  SELECT
    e1.*,
    e2.next_page_path_and_query,
    e2.next_page_user_id,
    e2.next_page_timestamp,
  FROM
    events_with_next_visit_to_url e1
  LEFT JOIN (
    SELECT
      anonymised_user_agent_and_ip,
      page_path_and_query,
      referer_path_and_query,
      page_path_and_query AS next_page_path_and_query,
      request_user_id AS next_page_user_id,
      occurred_at AS next_page_timestamp
    FROM
      events_with_standardised_url) e2
  ON
    ((e1.anonymised_user_agent_and_ip = e2.anonymised_user_agent_and_ip)
      OR (e1.request_user_id = e2.next_page_user_id))
    AND e1.page_path_and_query = e2.referer_path_and_query
    AND DATE(e1.occurred_at) = DATE(e2.next_page_timestamp)
    AND e1.occurred_at < e2.next_page_timestamp
    AND (e1.next_same_page_path_and_query_occurred_at IS NULL
      OR e2.next_page_timestamp < e1.next_same_page_path_and_query_occurred_at)),

/* 

The above join to create the events_with_next_page_details CTE results in some page visits with multiple rows in instances where a single page visit is followed by multiple pages. 

The events_with_following_pages CTE groups all the individual page visits into a single row and creates a STRUCT ARRAY of each of the following page visits. 

Importantly, multiple future pages IS NOT a journey of consecutive page visits. It is instead a series of branching page visits following the current page. 

*/ 

events_with_following_pages AS (
  SELECT
    anonymised_user_agent_and_ip,
    request_user_id,
    request_path,
    page_path_and_query,
    request_referer_domain,
    referer_path_and_query,
    occurred_at,
    ARRAY_AGG(STRUCT(next_page_path_and_query,
        next_page_user_id,
        next_page_timestamp)
    ORDER BY
      next_page_timestamp) AS following_pages
  FROM
    events_with_next_page_details
  GROUP BY
    request_user_id,
    anonymised_user_agent_and_ip,
    request_path,
    page_path_and_query,
    request_referer_domain,
    referer_path_and_query,
    occurred_at),

/*
  The preferred identifier for a session is a user_id as this will remain consistent until the user times out or signs out. 

  However, there are instances where a page visit will not have a user_id:
  1. The page was visited before the user signed in
  2. The user signs out and returns to the sign in page
  3. The visitor does not sign in

  The following code seeks to address reasons 1 and 2 by assigning the most likely user_id to them.

  Every page is assigned a next_user_id and a previous_user_id based on the user_id of the next row that meets a set of criteria that identify pages within the same session (listed below). 

  The next user_id in the same session is identified as the next page visit that meets the following criteria:
  1. A page visit within 30 mins that shares the same anonymised_user_agent_and_ip and has a user_id, indicating that the user signed-in.
  2. A following page (as defined in the previous step) has a user_id indicating that the user signed-in. 

  The previous_user_id in the same session is identified as the previous page visit that meets the following criteria:
  1. Occur within 30 mins of the previous page and share the same anonymised_user_agent_and_ip as the previous page

*/ 

  events_with_next_and_prev_user_id AS (
  SELECT
    *,
    CASE
      WHEN TIMESTAMP_DIFF(LEAD(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at), occurred_at, MINUTE) <= 30 
      THEN FIRST_VALUE(request_user_id IGNORE NULLS) 
      OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY UNIX_SECONDS(occurred_at) RANGE BETWEEN CURRENT ROW AND 1800 FOLLOWING )
         -- The FIRST_VALUE is used to find the first NON-NULL user_id within 30 mins, because the following row could occur within 30 mins but also have a NULL user_id. 
         -- The RANGE BETWEEN function on this code will include the user_id of the CURRENT row. If we wanted to only find the user_id of following rows we could set this to micro-second, but this makes the code harder to interpret and does not effect the functionality. 
    ELSE
    (
    SELECT
      MIN_BY(next_page_path_and_query, next_page_timestamp)
    FROM
      UNNEST(following_pages) AS fp
    WHERE
      fp.next_page_user_id IS NOT NULL)
    -- If there is no next_page_user_id (indicating there is no following page that meets the criteria) then this will result in a NULL next_user_id.
  END
    AS next_user_id,
    CASE
      WHEN TIMESTAMP_DIFF( occurred_at, LAG(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at), MINUTE ) <= 30 
      THEN LAST_VALUE(request_user_id IGNORE NULLS) 
      OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY UNIX_SECONDS(occurred_at) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
  END
    AS prev_user_id
  FROM
    events_with_following_pages),

/*

The events_with_users_estimated CTE uses COALESCE function to create the the estimated_user_id field. The logic of this function is as follows:
  
  1. If a request_user_id already exists THEN request user_id.
  
  2. If no request_user_id exists AND there is a request_user_id for a future page in the same session (next_user_id), THEN next_user_id.
  
  3. If no request_user_id exists AND no next_user_id exists AND a request_user_id existis for a previous page in the same session (prev_user_id), THEN prev_user_id
  
  * The purpose of this ordering is to correctly assign page visits in instances where multiple user_id share an anonymised_user_agent_and_ip. This primarily impacts visits to the sign-in page which will always have a NULL user_id. In the case that a user signs out and a new user signs in on the same device, the sign-in page is assigned to the latter users session. However, if the user returns to the sign in page and no new user signs in within the time contraints of a single session, then the sign-in page visit is assigned to the previous users session. 
  
*/

  events_with_users_estimated AS (
  SELECT
    anonymised_user_agent_and_ip,
    occurred_at,
    request_user_id,
    COALESCE( request_user_id,
      -- First preference: use the existing user_id if it's not null
      next_user_id,
      -- Second preference: use the nearest future user_id within 30 mins, or is a following page.
      prev_user_id
      -- Third preference: use the most recent past user_id within 30 mins
      ) AS estimated_user_id,
    -- The final estimated user_id based on the conditions
    request_path,
    page_path_and_query,
    request_referer_domain,
    referer_path_and_query,
     IF((
    SELECT
      MIN_BY(next_page_path_and_query, next_page_timestamp)
    FROM
      UNNEST(following_pages) AS fp) IS NOT NULL, TRUE, FALSE) AS visited_future_page
    -- Used in user_page_visits CTE to check if the page visit has a 'following page' in the same session
  FROM
    events_with_next_and_prev_user_id),

/*

  All page visits that occur within 30 mins or directly refer to a page visit with a known user_id are now assigned an estimated user_id.
  
  This means that any pages that have no estimated_user_id are not part of a session with a known user.

  For page visits with users, we partition by user_id. 

  For pages visits with no known users, we partition by anonymised_user_agent_and_ip because this is the best available identifier.

  As user page visits and non-user page visits are partitioned differently, the sessions are calculated seperately and joined with a UNION function.

  In order to assign session_id that do not change as more rows are added (as a UNION means these will be added in the middle of the table), the session_id is calculated according to the following logic:
  1. If a user_id exists, then CONCAT(user_id, "-", [First page_visit_at in session])
  2. If no user_id exists, then CONCAT(anonymised_user_agent_and_ip, "-", [First page_visit_at in session])

*/ 

  user_page_visits AS (
  SELECT
    *,
    CASE
      WHEN LEAD(estimated_user_id) OVER page_visits_for_this_estimated_user IS NOT NULL -- users only
    AND estimated_user_id = LEAD(estimated_user_id) OVER page_visits_for_this_estimated_user -- user_id matches next user_id
    AND (TIMESTAMP_DIFF(LEAD(occurred_at) OVER page_visits_for_this_estimated_user, occurred_at, MINUTE) <= 30 -- the same user visits a new page within 30 mins
    OR visited_future_page) -- As we have already filtered the 'following pages' for eligility, if this is not NULL then there is a following page in the session. 
    THEN "Visited subsequent pages"
    ELSE
    "Left site immediately after this"
    -- If the above session criteria are not met, then next_step = "Left site immediately after this"
  END
    AS next_step
  FROM
    events_with_users_estimated
  WHERE
    estimated_user_id IS NOT NULL
  WINDOW
  page_visits_for_this_estimated_user AS (
    PARTITION BY estimated_user_id
    ORDER BY occurred_at
  )),
 user_page_visits_with_session_boundaries AS (
  SELECT
    anonymised_user_agent_and_ip,
    estimated_user_id AS user_id,
    occurred_at AS page_visit_at,
    request_path AS page_path,
    request_referer_domain,
    REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
    next_step,
    CASE
      WHEN LAG(next_step) OVER page_visits_for_this_estimated_user IS NULL THEN TRUE
    -- Sets the first new_session value to 1
      WHEN LAG(next_step) OVER page_visits_for_this_estimated_user = "Left site immediately after this" THEN TRUE
    -- If the previous row next_step is "Left site immediately after this", then the current row is a new row. (Marked by new_session = 1). New sessions are marked (as opposed to session end points) so that the new_session field can be summed to get session numbers.
    ELSE
    FALSE
  END
    AS new_session
  FROM
    user_page_visits
    WINDOW
    page_visits_for_this_estimated_user AS (
    PARTITION BY estimated_user_id
    ORDER BY occurred_at
  )),
  user_page_visits_with_session_number AS (
  SELECT
    *,
    COUNT(CASE WHEN new_session THEN 1 END) OVER (PARTITION BY user_id ORDER BY page_visit_at) as session_number
  FROM
    user_page_visits_with_session_boundaries),
  user_page_visits_with_session_id AS (
  SELECT
    *,
    -- Concatenate the estimated_user_id with the cumulative sum of new_sessions field to form a session ID. This approach ensures that as new rows are added each time the code is ran, no session_ids are changed.
    CONCAT(user_id, "-", CAST(FIRST_VALUE(page_visit_at) OVER (PARTITION BY user_id, session_number ORDER BY page_visit_at) AS STRING)) AS session_id
  FROM
    user_page_visits_with_session_number),
  non_user_page_visits AS (
  SELECT
    *,
    CASE
      WHEN LEAD(estimated_user_id) OVER page_visits_for_this_anonymised_user_agent_and_ip IS NULL 
      AND anonymised_user_agent_and_ip = LEAD(anonymised_user_agent_and_ip) OVER page_visits_for_this_anonymised_user_agent_and_ip 
      AND (TIMESTAMP_DIFF(LEAD(occurred_at) OVER page_visits_for_this_anonymised_user_agent_and_ip, occurred_at, MINUTE) <= 30 
      OR visited_future_page) THEN "Visited Subsequent Pages"
    ELSE
    "Left site immediately after this"
    -- If the above session criteria are not met, then next_step = "Left site immediately after this"
  END
    AS next_step
  FROM
    events_with_users_estimated
  WHERE
    estimated_user_id IS NULL 
  WINDOW page_visits_for_this_anonymised_user_agent_and_ip AS (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at)),
  non_user_page_visits_with_session_boundaries AS (
  SELECT
    anonymised_user_agent_and_ip,
    estimated_user_id AS user_id,
    occurred_at AS page_visit_at,
    request_path AS page_path,
    request_referer_domain,
    REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
    next_step,
    CASE
      WHEN LAG(next_step) OVER page_visits_for_this_anonymised_user_agent_and_ip IS NULL THEN TRUE
    -- Sets the first new_session value to 1
      WHEN LAG(next_step) OVER page_visits_for_this_anonymised_user_agent_and_ip = "Left site immediately after this" THEN TRUE
    -- If the previous row next_step is "Left site immediately after this", then the current row is a new row. (Marked by new_session = 1). New sessions are marked (as opposed to session end points) so that the new_session field can be summed to get session numbers.
    ELSE
    FALSE
  END
    AS new_session
  FROM
    non_user_page_visits
  WINDOW page_visits_for_this_anonymised_user_agent_and_ip AS (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at)),
  non_user_page_visits_with_session_number AS (
  SELECT
    *,
    COUNT(CASE WHEN new_session THEN 1 END) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY page_visit_at) as session_number
  FROM
    non_user_page_visits_with_session_boundaries),
  nonuser_page_visits_with_session_id AS (
  SELECT
    *,
    CONCAT(anonymised_user_agent_and_ip, "-", CAST(FIRST_VALUE(page_visit_at) OVER (PARTITION BY anonymised_user_agent_and_ip, session_number ORDER BY page_visit_at) AS STRING)) AS session_id
    -- Concatenate the anonymised_user_agent_and_ip with the cumulative sum of new sessions to form a session ID. This approach ensures that as new rows are added each time the code is ran, no session_ids are changed.
  FROM
    non_user_page_visits_with_session_number),
  -- Join the user and non user sessions together into a single table
  sessions_grouped AS (
  SELECT
    anonymised_user_agent_and_ip,
    user_id,
    page_visit_at,
    page_path,
    request_referer_domain,
    previous_page_path,
    next_step,
    session_id
  FROM
    user_page_visits_with_session_id
  UNION ALL
  SELECT
    anonymised_user_agent_and_ip,
    user_id,
    page_visit_at,
    page_path,
    request_referer_domain,
    previous_page_path,
    next_step,
    session_id
  FROM
    nonuser_page_visits_with_session_id),
  -- Calculate page level metrics
  page_times AS (
  SELECT
    session_id,
    anonymised_user_agent_and_ip,
    user_id,
    page_path,
    request_referer_domain,
    previous_page_path,
    page_visit_at AS page_entry_time,
    LEAD(page_visit_at) OVER session_window AS page_exit_time,
    -- page_exit_time is set to the page_entry_time of the following page in the session.
    TIMESTAMP_DIFF(LEAD(page_visit_at) OVER session_window, page_visit_at, SECOND) AS page_duration_seconds,
    -- page_duration_seconds is set to the different between the page_entry_time of the current page and the page_entry_time of the following page in the session
    next_step
  FROM
    sessions_grouped
  WINDOW session_window AS (PARTITION BY session_id ORDER BY page_visit_at)),
  -- Calculate session level metrics
  session_metrics AS (
  SELECT
    session_id,
    user_id,
    -- A session can only have one user_id
    ARRAY_AGG(STRUCT( anonymised_user_agent_and_ip,
        -- A session can have multiple anonymised_user_agent_and_ip so this is included in the page level fields.
        page_path AS page,
        request_referer_domain as previous_page_domain,
        previous_page_path AS previous_page,
        page_entry_time,
        page_exit_time,
        page_duration_seconds AS duration,
        next_step)
    ORDER BY
      page_entry_time) AS pages_visited_details,
    MIN(page_entry_time) AS session_start_timestamp,
    -- session_start_timestamp is the page_entry_time of the first page
    MAX(page_entry_time) AS final_session_page_timestamp,
    -- session_end_timestamp is the page_entry_time of the final page. This is because we cannot know when they left that page as there is no page visit afterwards.
    COUNT(*) AS count_pages_visited,
    -- Count of all rows in a session is the pages visited in that session.
  FROM
    page_times
  GROUP BY
    session_id,
    user_id)
  SELECT
    session_id,
    user_id,
    pages_visited_details[0].previous_page_domain AS session_referer_domain,
    pages_visited_details[0].page AS start_page,
    -- set start_page to the first page in the pages_visited_details array for a single session
   ARRAY_REVERSE(pages_visited_details)[0].page AS exit_page,
    -- set exit_page to the last page in the pages_visited_details array for a single session
    session_start_timestamp,
    final_session_page_timestamp,
    CASE /* This is for single page sessions. By default these will have a session time of 0 but this is not a legitimate value because the start and end time are taken from the same page. Therefore, we set them to NULL so they do not impact calculations of averages */
      WHEN final_session_page_timestamp = session_start_timestamp THEN NULL
    ELSE
    TIMESTAMP_DIFF(final_session_page_timestamp, session_start_timestamp, SECOND)
  END
    AS session_time_in_seconds,
    count_pages_visited,
    pages_visited_details
  FROM
    session_metrics
  `).preOps((ctx) => {
    // This pre operation is used to filter the events table to only events that started AFTER the maximum event start time in the current session_details table. 
    const isIncremental = ctx.incremental(); // Check if the run is incremental
    const eventTimestampCheckpoint = isIncremental
    ? `SELECT MAX(session_start_timestamp) FROM ${ctx.self()}`
    : `SELECT TIMESTAMP("2000-01-01")`;
    return `
    -- Declare variable for filtering events
    DECLARE event_timestamp_checkpoint TIMESTAMP DEFAULT (
      ${eventTimestampCheckpoint}
    );
  `;
})
}