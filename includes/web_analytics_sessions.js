const identityConfig = require("../definitions/web_analytics_identity_inference_config_upd");
const parameter_functions = require("./parameter_functions");

module.exports = params => {
  if (!params.enableSessionDetailsTable) {
    return true;
  }

    /* --------------------------------------------------------------------------
     1. Mode flags
  -------------------------------------------------------------------------- */

  const enableIdentityResolution =
    params.enableWebRequestIdentityResolution === true;

  const sessionBuildMode = enableIdentityResolution
    ? "identity_aware"
    : "device_only";

  /* --------------------------------------------------------------------------
     2. Optional service-specific config

     Session generation should not require identity config.
     Identity config is only mandatory in the identity-resolution script itself.
  -------------------------------------------------------------------------- */

  const webAnalytics = identityConfig[params.eventSourceName] || {};

  const paths = webAnalytics.paths || {};

  const startDate =
    webAnalytics.startDate ||
    "2025-06-01";

  /* --------------------------------------------------------------------------
     3. Dynamic table names
  -------------------------------------------------------------------------- */

  const finalName =
    params.sessionDetailsTableName ||
    `session_details_cfg_${params.eventSourceName}`;

  const rawEventsName =
    `events_${params.eventSourceName}`;

  const identityEventsName =
    params.identityEventsTableName ||
    `identity_solved_events_cfg_${params.eventSourceName}`;

  const sessionInputEventsName = enableIdentityResolution
    ? identityEventsName
    : rawEventsName;

  /* --------------------------------------------------------------------------
     4. Path config
  -------------------------------------------------------------------------- */

  const unique = values => [...new Set(values)];

  const preAuthPagePaths = paths.preAuth || ["/", "/?"];

  const incrementalReadLookbackHours =
    params.sessionIncrementalReadLookbackHours || 12;

  const incrementalReplaceLookbackHours =
    params.sessionIncrementalReplaceLookbackHours || 6;

  /* --------------------------------------------------------------------------
     5. SQL helper functions
  -------------------------------------------------------------------------- */

  function sqlString(value) {
    return `'${String(value).replace(/'/g, "\\'")}'`;
  }

  function sqlStringArray(values = []) {
    return `[${values.map(sqlString).join(", ")}]`;
  }

  function sqlInList(field, values = []) {
    if (!values.length) {
      return "FALSE";
    }
    return `${field} IN UNNEST(${sqlStringArray(values)})`;
  }

  function attributionParamFields() {
    if (typeof parameter_functions.attributionParamFields === "function") {
      return parameter_functions.attributionParamFields(params);
    }

    return `
      REGEXP_EXTRACT(request_query, r'(?:^|&)utm_source=([^&]*)') AS utm_source,
      REGEXP_EXTRACT(request_query, r'(?:^|&)utm_medium=([^&]*)') AS utm_medium,
      REGEXP_EXTRACT(request_query, r'(?:^|&)utm_campaign=([^&]*)') AS utm_campaign
    `;
  }

  function requestToMedium(ctx) {
    if (typeof parameter_functions.requestToMedium === "function") {
      return parameter_functions.requestToMedium(ctx);
    }

    return `
      CASE
        WHEN utm_medium IS NOT NULL THEN utm_medium
        WHEN request_referer_domain IS NULL THEN "direct"
        ELSE "referral"
      END
    `;
  }

  const tags = [
    params.eventSourceName.toLowerCase(),
    "sessionisation"
  ];

  return publish(finalName, {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    bigquery: {
      partitionBy: "DATE(session_start_timestamp)",
      updatePartitionFilter:
        `session_start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${incrementalReplaceLookbackHours + 24} HOUR)`,
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    assertions: {
      uniqueKey: [["session_id"]]
    },
    tags,
     description: enableIdentityResolution
      ? "This table contains data on sessions and accompanying metrics. This service is configured to also use the identity resolution script which infers the identity of the user of each web request event using a server-side methodology. Each row is a single session. This table uses the Google Analytics definition of a session: A session is a group of user interactions with the website that that occur continuously without a break of more than 30 minutes of inactivity or until the user navigates away from the site. This does not include sessions or page visits from bots."
      : "This table contains data on sessions and accompanying metrics. This service is NOT configured to infer the identities of users, which means that the sessions are constructed using only device level identifiers. Each row is a single session. This table uses the Google Analytics definition of a session: A session is a group of user interactions with the website that that occur continuously without a break of more than 30 minutes of inactivity or until the user navigates away from the site. This does not include sessions or page visits from bots.",
    dependencies: [...(params.dependencies || []), sessionInputEventsName],
    columns: {
      session_id: "Stable hashed session identifier.",
      user_id: "Resolved user identity for user sessions. Null for anonymous device sessions.",
      user_signed_in: "True when the session contains resolved user identity.",
      session_type: "Either user_session or device_session.",
      known_multi_user_device: "True when the AUID has historically been associated with more than one IUID.",
      multiple_auid_session: "True when a session contains more than one AUID.",
      device_id: "Single AUID for single-AUID sessions, or synthetic multi-AUID session device id.",
      session_namespace: "Namespace of the first page in the session.",
      session_referer_domain: "Referer domain of the first page in the session.",
      start_page: "First page path in the session.",
      exit_page: "Last page path in the session.",
      session_start_timestamp: "Timestamp of the first page in the session.",
      final_session_page_timestamp: "Timestamp of the final page in the session.",
      session_time_in_seconds: "Time from first to final page in the session.",
      count_pages_visited: "Number of pages in the session.",
      pages_visited_details: "Ordered page-level details for the session.",
      journey_id: "Identifier joining adjacent device and user sessions where applicable."
    }
  }).query(ctx => `

WITH

/* --------------------------------------------------------------------------
   1. Base page-view style web events from final identity output

   Identity has already done the heavy attribution work. This layer consumes
   current_iuid and accompanying resolution metadata.
-------------------------------------------------------------------------- */

/* This JS function will create the base events table from either the identity resolved events (if enableWebRequestIdentityResolution = true) or the raw events table (if enableWebRequestIdentityResolution = false) */

${enableIdentityResolution ? `
/* --------------------------------------------------------------------------
   1. Base page-view style web events from final identity output

   Identity has already done the heavy attribution work. This layer consumes
   current_iuid and accompanying resolution metadata.
-------------------------------------------------------------------------- */

events AS (
  SELECT
    request_uuid,
    auid AS anonymised_user_agent_and_ip,
    CAST(current_iuid AS STRING) AS user_id,

    occurred_at,
    namespace,
    request_path,
    request_query,
    request_path_and_query AS page_path_and_query,
    request_referer_domain,

    IF(
      SUBSTR(request_referer_path_and_query, -1) = "?",
      SPLIT(request_referer_path_and_query, "?")[OFFSET(0)],
      request_referer_path_and_query
    ) AS referer_path_and_query,

    request_method,
    SAFE_CAST(response_status AS STRING) AS response_status,
    response_content_type,
    device_category,

    auid_distinct_iuid_count,
    auid_risk_classification,

    current_iuid_method,
    current_resolution_stage,
    current_iteration,
    is_currently_resolved,
    identity_resolution_priority,
    identity_resolution_locked,

    assigned_by_activity_window_fallback,
    window_assignment_reason,
    window_assignment_supporting_evidence,
    matched_windows_total,
    matched_clean_windows,
    matched_non_clean_windows,
    matched_overlapping_windows,
    matched_post_conflict_windows,
    matched_unknown_preexisting_activity_windows,

    parent_request_uuid_pass1,
    parent_match_confidence_pass1,
    parent_match_source_pass1,
    chain_id_pass1,
    likely_shunt_arrival,

    ${attributionParamFields()}

  FROM ${ctx.ref(identityEventsName)}
  WHERE event_type = "web_request"
    AND COALESCE(device_category, "") != "bot"
    AND CONTAINS_SUBSTR(COALESCE(response_content_type, ""), "text/html")
    AND SAFE_CAST(response_status AS STRING) NOT LIKE "3__"
    AND SAFE_CAST(response_status AS STRING) NOT LIKE "4__"
    AND occurred_at >= event_read_checkpoint
)
` : `
/* --------------------------------------------------------------------------
   1. Base page-view style web events from raw events output

   Identity resolution is disabled. This layer only exposes the fields needed
   for device-session construction.
-------------------------------------------------------------------------- */

events AS (
  SELECT
    request_uuid,
    anonymised_user_agent_and_ip,
    CAST(NULL AS STRING) AS user_id,

    occurred_at,
    namespace,
    request_path,
    request_query,
    request_path_and_query AS page_path_and_query,
    request_referer_domain,

    IF(
      SUBSTR(request_referer_path_and_query, -1) = "?",
      SPLIT(request_referer_path_and_query, "?")[OFFSET(0)],
      request_referer_path_and_query
    ) AS referer_path_and_query,

    request_method,
    SAFE_CAST(response_status AS STRING) AS response_status,
    response_content_type,
    device_category,

    ${attributionParamFields()}

  FROM ${ctx.ref(rawEventsName)}
  WHERE event_type = "web_request"
    AND anonymised_user_agent_and_ip IS NOT NULL
    AND COALESCE(device_category, "") != "bot"
    AND CONTAINS_SUBSTR(COALESCE(response_content_type, ""), "text/html")
    AND SAFE_CAST(response_status AS STRING) NOT LIKE "3__"
    AND SAFE_CAST(response_status AS STRING) NOT LIKE "4__"
    AND occurred_at >= event_read_checkpoint
)
`},

events_with_medium AS (
  SELECT
    *,
    ${requestToMedium(ctx)} AS medium
  FROM events
),

anonymous_events AS (
  SELECT *
  FROM events_with_medium
  WHERE user_id IS NULL
),

/* This JS function will prevent the script running the user session construction if enableWebRequestIdentityResolution = false. */

${enableIdentityResolution ? `
/* --------------------------------------------------------------------------
   2. Signed-in / resolved user sessions

   Sessions are partitioned by resolved user identity, intentionally allowing
   continuity across AUID/IP changes when the identity model resolved them.
-------------------------------------------------------------------------- */

signed_in_events AS (
  SELECT *
  FROM events_with_medium
  WHERE user_id IS NOT NULL
),

signed_in_events_ordered AS (
  SELECT
    *,
    LAG(occurred_at) OVER user_window AS prev_occurred_at,
    LAG(page_path_and_query) OVER user_window AS prev_page_path_and_query
  FROM signed_in_events
  WINDOW user_window AS (
    PARTITION BY user_id
    ORDER BY occurred_at, request_uuid
  )
),

signed_in_events_with_session_boundaries AS (
  SELECT
    *,

    CASE
      WHEN prev_occurred_at IS NULL THEN TRUE

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
        THEN FALSE

      WHEN referer_path_and_query = prev_page_path_and_query
       AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 360
        THEN FALSE

      ELSE TRUE
    END AS new_session,

    CASE
      WHEN prev_occurred_at IS NULL
        THEN "First page of this user session"

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
       AND referer_path_and_query = prev_page_path_and_query
        THEN "Continued from previous user page: within 30 minutes and exact referrer match"

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
        THEN "Continued from previous user page: within 30 minutes"

      WHEN referer_path_and_query = prev_page_path_and_query
       AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 360
        THEN "Continued from previous user page: exact referrer match within 6 hours"

      ELSE "Started a new user session after a gap"
    END AS continuity_type_to_current_page,

    CASE
      WHEN prev_occurred_at IS NULL THEN NULL
      WHEN referer_path_and_query = prev_page_path_and_query THEN TRUE
      ELSE FALSE
    END AS has_exact_referrer_match_to_previous_page,

    CASE
      WHEN prev_occurred_at IS NULL THEN FALSE
      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) > 30
       AND referer_path_and_query = prev_page_path_and_query
       AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 360
        THEN TRUE
      ELSE FALSE
    END AS continued_after_30m_by_exact_referrer

  FROM signed_in_events_ordered
),

signed_in_events_with_session_number AS (
  SELECT
    *,
    COUNTIF(new_session) OVER (
      PARTITION BY user_id
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_number
  FROM signed_in_events_with_session_boundaries
),

signed_in_events_with_session_root AS (
  SELECT
    *,

    FIRST_VALUE(request_uuid) OVER (
      PARTITION BY user_id, session_number
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_request_uuid,

    FIRST_VALUE(occurred_at) OVER (
      PARTITION BY user_id, session_number
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_session_timestamp

  FROM signed_in_events_with_session_number
),

signed_in_events_with_session_id AS (
  SELECT
    *,
    TO_HEX(SHA256(CONCAT(
      "user_session",
      "|",
      user_id,
      "|",
      CAST(first_session_timestamp AS STRING),
      "|",
      COALESCE(first_request_uuid, "")
    ))) AS session_id
  FROM signed_in_events_with_session_root
),

signed_in_session_start_events AS (
  SELECT
    session_id,
    user_id,
    anonymised_user_agent_and_ip,
    occurred_at AS first_signed_in_event_at,
    page_path_and_query AS first_signed_in_page_path_and_query
  FROM signed_in_events_with_session_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY occurred_at, request_uuid
  ) = 1
),

/* --------------------------------------------------------------------------
   3. Pre-auth stitching

   Identity does not intentionally infer pre-anchor events. We attach eligible
   anonymous pre-auth pages to the first resolved user session when there is
   close same-AUID temporal continuity and the page is allow-listed.
-------------------------------------------------------------------------- */

pre_auth_pages AS (
  SELECT request_path
  FROM UNNEST(${sqlStringArray(preAuthPagePaths)}) AS request_path
),

stitched_pre_auth_pages AS (
  SELECT DISTINCT
    s.session_id,
    s.user_id,
    e.request_uuid,
    e.anonymised_user_agent_and_ip,
    e.occurred_at,
    e.page_path_and_query
  FROM signed_in_session_start_events s
  INNER JOIN anonymous_events e
    ON e.anonymised_user_agent_and_ip = s.anonymised_user_agent_and_ip
   AND e.occurred_at <= s.first_signed_in_event_at
   AND TIMESTAMP_DIFF(s.first_signed_in_event_at, e.occurred_at, MINUTE) <= 30
  INNER JOIN pre_auth_pages p
    ON e.request_path = p.request_path
  WHERE
    (
      COALESCE(e.auid_distinct_iuid_count, 0) <= 1
    )
    OR
    (
      COALESCE(e.auid_distinct_iuid_count, 2) > 1
      AND NOT EXISTS (
        SELECT 1
        FROM signed_in_session_start_events s2
        WHERE s2.anonymised_user_agent_and_ip = s.anonymised_user_agent_and_ip
          AND s2.first_signed_in_event_at > e.occurred_at
          AND s2.first_signed_in_event_at < s.first_signed_in_event_at
      )
    )
),

user_session_events AS (
  SELECT
    session_id,
    user_id,
    request_uuid,
    anonymised_user_agent_and_ip,
    occurred_at,
    namespace,
    request_path,
    request_query,
    page_path_and_query,
    request_referer_domain,
    referer_path_and_query,
    request_method,
    response_status,
    response_content_type,
    device_category,
    auid_distinct_iuid_count,
    auid_risk_classification,
    current_iuid_method,
    current_resolution_stage,
    current_iteration,
    is_currently_resolved,
    identity_resolution_priority,
    identity_resolution_locked,
    assigned_by_activity_window_fallback,
    window_assignment_reason,
    window_assignment_supporting_evidence,
    matched_windows_total,
    matched_clean_windows,
    matched_non_clean_windows,
    matched_overlapping_windows,
    matched_post_conflict_windows,
    matched_unknown_preexisting_activity_windows,
    parent_request_uuid_pass1,
    parent_match_confidence_pass1,
    parent_match_source_pass1,
    chain_id_pass1,
    likely_shunt_arrival,
    utm_source,
    utm_medium,
    utm_campaign,
    medium,
    continuity_type_to_current_page,
    has_exact_referrer_match_to_previous_page,
    continued_after_30m_by_exact_referrer,
    FALSE AS stitched_pre_auth_page
  FROM signed_in_events_with_session_id

  UNION ALL

  SELECT
    s.session_id,
    s.user_id,
    e.request_uuid,
    e.anonymised_user_agent_and_ip,
    e.occurred_at,
    e.namespace,
    e.request_path,
    e.request_query,
    e.page_path_and_query,
    e.request_referer_domain,
    e.referer_path_and_query,
    e.request_method,
    e.response_status,
    e.response_content_type,
    e.device_category,
    e.auid_distinct_iuid_count,
    e.auid_risk_classification,
    e.current_iuid_method,
    e.current_resolution_stage,
    e.current_iteration,
    e.is_currently_resolved,
    e.identity_resolution_priority,
    e.identity_resolution_locked,
    e.assigned_by_activity_window_fallback,
    e.window_assignment_reason,
    e.window_assignment_supporting_evidence,
    e.matched_windows_total,
    e.matched_clean_windows,
    e.matched_non_clean_windows,
    e.matched_overlapping_windows,
    e.matched_post_conflict_windows,
    e.matched_unknown_preexisting_activity_windows,
    e.parent_request_uuid_pass1,
    e.parent_match_confidence_pass1,
    e.parent_match_source_pass1,
    e.chain_id_pass1,
    e.likely_shunt_arrival,
    e.utm_source,
    e.utm_medium,
    e.utm_campaign,
    e.medium,
    "Stitched pre-auth page attached to following user session" AS continuity_type_to_current_page,
    CAST(NULL AS BOOL) AS has_exact_referrer_match_to_previous_page,
    CAST(NULL AS BOOL) AS continued_after_30m_by_exact_referrer,
    TRUE AS stitched_pre_auth_page
  FROM stitched_pre_auth_pages s
  INNER JOIN anonymous_events e
    ON e.request_uuid = s.request_uuid
   AND e.anonymised_user_agent_and_ip = s.anonymised_user_agent_and_ip
   AND e.occurred_at = s.occurred_at
   AND e.page_path_and_query = s.page_path_and_query
),

/* --------------------------------------------------------------------------
   4. Page timings for user sessions
-------------------------------------------------------------------------- */

user_session_page_times AS (
  SELECT
    session_id,
    user_id,
    request_uuid,
    anonymised_user_agent_and_ip,
    namespace,

    CAST(NULL AS STRING) AS page_domain,
    request_path AS page_path,
    request_query,
    page_path_and_query,

    utm_source,
    utm_medium,
    utm_campaign,
    medium,
    device_category,

    request_referer_domain AS previous_page_domain,
    REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
    referer_path_and_query AS previous_page_path_and_query,

    occurred_at AS page_entry_time,

    LEAD(occurred_at) OVER (
      PARTITION BY session_id
      ORDER BY occurred_at, request_uuid
    ) AS page_exit_time,

    TIMESTAMP_DIFF(
      LEAD(occurred_at) OVER (
        PARTITION BY session_id
        ORDER BY occurred_at, request_uuid
      ),
      occurred_at,
      SECOND
    ) AS page_duration_seconds,

    auid_distinct_iuid_count,
    auid_risk_classification,

    current_iuid_method,
    current_resolution_stage,
    current_iteration,
    is_currently_resolved,
    identity_resolution_priority,
    identity_resolution_locked,

    assigned_by_activity_window_fallback,
    window_assignment_reason,
    window_assignment_supporting_evidence,
    matched_windows_total,
    matched_clean_windows,
    matched_non_clean_windows,
    matched_overlapping_windows,
    matched_post_conflict_windows,
    matched_unknown_preexisting_activity_windows,

    parent_request_uuid_pass1,
    parent_match_confidence_pass1,
    parent_match_source_pass1,
    chain_id_pass1,
    likely_shunt_arrival,

    stitched_pre_auth_page,
    continuity_type_to_current_page,
    has_exact_referrer_match_to_previous_page,
    continued_after_30m_by_exact_referrer

  FROM user_session_events
),

user_session_metrics AS (
  SELECT
    session_id,
    user_id,

    MAX(COALESCE(auid_distinct_iuid_count, 0)) AS auid_distinct_iuid_count,

    LOGICAL_OR(
      user_id IS NOT NULL
      AND COALESCE(identity_resolution_priority, 0) < 70
    ) AS includes_low_confidence_identity_propagation,

    LOGICAL_OR(assigned_by_activity_window_fallback = TRUE)
      AS includes_activity_window_identity_assignment,

    LOGICAL_OR(current_resolution_stage LIKE "PART_4%")
      AS includes_repair_walk_identity_assignment,

    LOGICAL_OR(matched_overlapping_windows > 0)
      AS includes_overlapping_identity_window_context,

    LOGICAL_OR(matched_post_conflict_windows > 0)
      AS includes_post_conflict_identity_window_context,

    LOGICAL_OR(matched_unknown_preexisting_activity_windows > 0)
      AS includes_unknown_preexisting_activity_window_context,

    LOGICAL_OR(continued_after_30m_by_exact_referrer)
      AS includes_6h_referrer_continuity,

    LOGICAL_OR(stitched_pre_auth_page)
      AS includes_stitched_pre_auth_pages,

    COUNT(DISTINCT anonymised_user_agent_and_ip) > 1 AS multiple_auid_session,

    ARRAY_AGG(
      DISTINCT anonymised_user_agent_and_ip IGNORE NULLS
      ORDER BY anonymised_user_agent_and_ip
    ) AS session_auids,

    ARRAY_AGG(
      STRUCT(
        request_uuid,
        anonymised_user_agent_and_ip,
        page_domain,
        page_path,
        page_path_and_query,
        previous_page_domain,
        previous_page_path,
        previous_page_path_and_query,
        page_entry_time,
        page_exit_time,
        page_duration_seconds AS duration,
        continuity_type_to_current_page
      )
      ORDER BY page_entry_time, request_uuid
    ) AS pages_visited_details,

    ARRAY_AGG(
      STRUCT(
        namespace,
        utm_source,
        utm_medium,
        utm_campaign,
        medium,
        device_category
      )
      ORDER BY page_entry_time, request_uuid
    ) AS session_level_metrics,

    MIN(page_entry_time) AS session_start_timestamp,
    MAX(page_entry_time) AS final_session_page_timestamp,
    COUNT(*) AS count_pages_visited

  FROM user_session_page_times
  GROUP BY session_id, user_id
),

user_sessions_final AS (
  SELECT
    session_id,
    user_id,
    TRUE AS user_signed_in,
    "user_session" AS session_type,
    "known_user" AS behavioural_user_type,

    auid_distinct_iuid_count,

    multiple_auid_session,

    CASE
      WHEN multiple_auid_session
        THEN CONCAT("multi_auid_session_", TO_HEX(SHA256(session_id)))
      ELSE session_auids[OFFSET(0)]
    END AS device_id,

    session_auids,

    session_level_metrics[OFFSET(0)].namespace AS session_namespace,
    pages_visited_details[OFFSET(0)].previous_page_domain AS session_referer_domain,

    pages_visited_details[OFFSET(0)].page_path AS start_page,
    ARRAY_REVERSE(pages_visited_details)[OFFSET(0)].page_path AS exit_page,

    session_level_metrics[OFFSET(0)].utm_source AS utm_source,
    session_level_metrics[OFFSET(0)].utm_medium AS utm_medium,
    session_level_metrics[OFFSET(0)].utm_campaign AS utm_campaign,
    session_level_metrics[OFFSET(0)].medium AS medium,
    session_level_metrics[OFFSET(0)].device_category AS device_category,

    session_start_timestamp,
    final_session_page_timestamp,

    CASE
      WHEN final_session_page_timestamp = session_start_timestamp THEN NULL
      ELSE TIMESTAMP_DIFF(final_session_page_timestamp, session_start_timestamp, SECOND)
    END AS session_time_in_seconds,

    count_pages_visited,
    pages_visited_details

  FROM user_session_metrics
),` : ``}

/* --------------------------------------------------------------------------
   5. Anonymous device sessions from remaining anonymous events
-------------------------------------------------------------------------- */

/* This JS conditional selects either:
   - unresolved anonymous events after user-session stitching, when identity resolution is on
   - all anonymous/device events, when identity resolution is off
*/

${enableIdentityResolution ? `
remaining_anonymous_events AS (
  SELECT
    e.*
  FROM anonymous_events e
  LEFT JOIN user_session_events u
    ON e.request_uuid = u.request_uuid
   AND e.anonymised_user_agent_and_ip = u.anonymised_user_agent_and_ip
   AND e.occurred_at = u.occurred_at
   AND u.stitched_pre_auth_page = TRUE
  WHERE u.session_id IS NULL
),
` : `
remaining_anonymous_events AS (
  SELECT
    *
  FROM anonymous_events
),
`}

remaining_anonymous_events_ordered AS (
  SELECT
    *,
    LAG(occurred_at) OVER auid_window AS prev_occurred_at,
    LAG(page_path_and_query) OVER auid_window AS prev_page_path_and_query
  FROM remaining_anonymous_events
  WINDOW auid_window AS (
    PARTITION BY anonymised_user_agent_and_ip
    ORDER BY occurred_at, request_uuid
  )
),

remaining_anonymous_events_with_boundaries AS (
  SELECT
    *,

    CASE
      WHEN prev_occurred_at IS NULL THEN TRUE

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
        THEN FALSE

      WHEN referer_path_and_query = prev_page_path_and_query
        THEN FALSE

      ELSE TRUE
    END AS new_session,

    CASE
      WHEN prev_occurred_at IS NULL
        THEN "First page of this device session"

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
       AND referer_path_and_query = prev_page_path_and_query
        THEN "Continued from previous device page: within 30 minutes and exact referrer match"

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
        THEN "Continued from previous device page: within 30 minutes"

      WHEN referer_path_and_query = prev_page_path_and_query
        THEN "Continued from previous device page: exact referrer match after more than 30 minutes"

      ELSE "Started a new device session after a gap"
    END AS continuity_type_to_current_page

  FROM remaining_anonymous_events_ordered
),

remaining_anonymous_events_with_session_number AS (
  SELECT
    *,
    COUNTIF(new_session) OVER (
      PARTITION BY anonymised_user_agent_and_ip
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_number
  FROM remaining_anonymous_events_with_boundaries
),

remaining_anonymous_events_with_session_root AS (
  SELECT
    *,

    FIRST_VALUE(request_uuid) OVER (
      PARTITION BY anonymised_user_agent_and_ip, session_number
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_request_uuid,

    FIRST_VALUE(occurred_at) OVER (
      PARTITION BY anonymised_user_agent_and_ip, session_number
      ORDER BY occurred_at, request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_session_timestamp

  FROM remaining_anonymous_events_with_session_number
),

remaining_anonymous_events_with_session_id AS (
  SELECT
    *,
    TO_HEX(SHA256(CONCAT(
      "device_session",
      "|",
      anonymised_user_agent_and_ip,
      "|",
      CAST(first_session_timestamp AS STRING),
      "|",
      COALESCE(first_request_uuid, "")
    ))) AS session_id
  FROM remaining_anonymous_events_with_session_root
),

device_session_page_times AS (
  SELECT
    session_id,
    request_uuid,
    anonymised_user_agent_and_ip,
    namespace,

    CAST(NULL AS STRING) AS page_domain,
    request_path AS page_path,
    request_query,
    page_path_and_query,

    utm_source,
    utm_medium,
    utm_campaign,
    medium,
    device_category,

    request_referer_domain AS previous_page_domain,
    REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
    referer_path_and_query AS previous_page_path_and_query,

    occurred_at AS page_entry_time,

    LEAD(occurred_at) OVER (
      PARTITION BY session_id
      ORDER BY occurred_at, request_uuid
    ) AS page_exit_time,

    TIMESTAMP_DIFF(
      LEAD(occurred_at) OVER (
        PARTITION BY session_id
        ORDER BY occurred_at, request_uuid
      ),
      occurred_at,
      SECOND
    ) AS page_duration_seconds,

    continuity_type_to_current_page

    ${enableIdentityResolution ? `,
    auid_distinct_iuid_count,
    auid_risk_classification
    ` : ``}

  FROM remaining_anonymous_events_with_session_id
),

device_session_metrics AS (
  SELECT
    session_id,

    ${enableIdentityResolution ? `
    MAX(COALESCE(auid_distinct_iuid_count, 0)) AS auid_distinct_iuid_count,

    LOGICAL_OR(
      auid_risk_classification IN (
        "UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY",
        "HIGH_RISK_MULTI_IUID_AUID"
      )
    ) AS has_unresolved_user_evidence,
    ` : `
    FALSE AS has_unresolved_user_evidence,
    `}

    COUNT(DISTINCT anonymised_user_agent_and_ip) > 1 AS multiple_auid_session,

    ARRAY_AGG(
      DISTINCT anonymised_user_agent_and_ip IGNORE NULLS
      ORDER BY anonymised_user_agent_and_ip
    ) AS session_auids,

    ARRAY_AGG(
      STRUCT(
        request_uuid,
        anonymised_user_agent_and_ip,
        page_domain,
        page_path,
        page_path_and_query,
        previous_page_domain,
        previous_page_path,
        previous_page_path_and_query,
        page_entry_time,
        page_exit_time,
        page_duration_seconds AS duration,
        continuity_type_to_current_page
      )
      ORDER BY page_entry_time, request_uuid
    ) AS pages_visited_details,

    ARRAY_AGG(
      STRUCT(
        namespace,
        utm_source,
        utm_medium,
        utm_campaign,
        medium,
        device_category
      )
      ORDER BY page_entry_time, request_uuid
    ) AS session_level_metrics,

    MIN(page_entry_time) AS session_start_timestamp,
    MAX(page_entry_time) AS final_session_page_timestamp,
    COUNT(*) AS count_pages_visited

  FROM device_session_page_times
  GROUP BY session_id
),

device_sessions_final AS (
  SELECT
    session_id,
    CAST(NULL AS STRING) AS user_id,
    FALSE AS user_signed_in,
    "device_session" AS session_type,

    CASE
      WHEN has_unresolved_user_evidence THEN "unknown_user"
      ELSE "anonymous_only"
    END AS behavioural_user_type,

    ${enableIdentityResolution ? `
    auid_distinct_iuid_count,
    ` : ``}

    multiple_auid_session,

    CASE
      WHEN multiple_auid_session
        THEN CONCAT("multi_auid_session_", TO_HEX(SHA256(session_id)))
      ELSE session_auids[OFFSET(0)]
    END AS device_id,

    session_auids,

    session_level_metrics[OFFSET(0)].namespace AS session_namespace,
    pages_visited_details[OFFSET(0)].previous_page_domain AS session_referer_domain,

    pages_visited_details[OFFSET(0)].page_path AS start_page,
    ARRAY_REVERSE(pages_visited_details)[OFFSET(0)].page_path AS exit_page,

    session_level_metrics[OFFSET(0)].utm_source AS utm_source,
    session_level_metrics[OFFSET(0)].utm_medium AS utm_medium,
    session_level_metrics[OFFSET(0)].utm_campaign AS utm_campaign,
    session_level_metrics[OFFSET(0)].medium AS medium,
    session_level_metrics[OFFSET(0)].device_category AS device_category,

    session_start_timestamp,
    final_session_page_timestamp,

    CASE
      WHEN final_session_page_timestamp = session_start_timestamp THEN NULL
      ELSE TIMESTAMP_DIFF(final_session_page_timestamp, session_start_timestamp, SECOND)
    END AS session_time_in_seconds,

    count_pages_visited,
    pages_visited_details

  FROM device_session_metrics
),

${enableIdentityResolution ? `
/* --------------------------------------------------------------------------
   6. Combine user and device sessions
-------------------------------------------------------------------------- */

all_sessions AS (
  SELECT * FROM user_sessions_final
  UNION ALL
  SELECT * FROM device_sessions_final
),

/* --------------------------------------------------------------------------
   7. Journey stitching

   This is not identity propagation. It links an immediately preceding anonymous
   device session to a later user session where continuity is plausible.
-------------------------------------------------------------------------- */

user_session_first_pages AS (
  SELECT
    session_id AS user_session_id,
    user_id,
    anonymised_user_agent_and_ip AS first_page_auid,
    occurred_at AS first_page_at,
    referer_path_and_query AS first_page_referer_path_and_query,
    page_path_and_query AS first_page_path_and_query,
    auid_distinct_iuid_count
  FROM user_session_events
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY occurred_at, request_uuid
  ) = 1
),

user_session_last_pages AS (
  SELECT
    session_id,
    "user_session" AS session_type,
    anonymised_user_agent_and_ip AS session_ending_auid,
    occurred_at AS last_page_at,
    page_path_and_query AS last_page_path_and_query,
    auid_distinct_iuid_count
  FROM user_session_events
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY occurred_at DESC, request_uuid DESC
  ) = 1
),

device_session_last_pages AS (
  SELECT
    session_id,
    "device_session" AS session_type,
    anonymised_user_agent_and_ip AS session_ending_auid,
    occurred_at AS last_page_at,
    page_path_and_query AS last_page_path_and_query,
    auid_distinct_iuid_count
  FROM remaining_anonymous_events_with_session_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY occurred_at DESC, request_uuid DESC
  ) = 1
),

all_session_last_pages_by_auid AS (
  SELECT * FROM user_session_last_pages
  UNION ALL
  SELECT * FROM device_session_last_pages
),

immediately_preceding_session_on_user_first_page_auid AS (
  SELECT
    u.user_session_id,
    u.user_id,
    u.first_page_auid,
    u.first_page_at,
    u.first_page_referer_path_and_query,
    u.first_page_path_and_query,
    u.auid_distinct_iuid_count AS user_session_auid_distinct_iuid_count,

    p.session_id AS previous_session_id,
    p.session_type AS previous_session_type,
    p.last_page_at AS previous_session_last_page_at,
    p.last_page_path_and_query AS previous_session_last_page_path_and_query,
    p.auid_distinct_iuid_count AS previous_session_auid_distinct_iuid_count,

    TIMESTAMP_DIFF(u.first_page_at, p.last_page_at, MINUTE)
      AS stitch_gap_minutes,

    u.first_page_referer_path_and_query = p.last_page_path_and_query
      AS has_exact_referrer_match

  FROM user_session_first_pages u
  INNER JOIN all_session_last_pages_by_auid p
    ON p.session_ending_auid = u.first_page_auid
   AND p.last_page_at <= u.first_page_at
   AND TIMESTAMP_DIFF(u.first_page_at, p.last_page_at, MINUTE) <= 30
   AND p.session_id != u.user_session_id

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY u.user_session_id
    ORDER BY p.last_page_at DESC
  ) = 1
),

approved_device_to_user_journeys AS (
  SELECT
    user_session_id,
    previous_session_id AS device_session_id,

    TO_HEX(SHA256(CONCAT(
      previous_session_id,
      "--",
      user_session_id
    ))) AS journey_id

  FROM immediately_preceding_session_on_user_first_page_auid
  WHERE previous_session_type = "device_session"
    AND (
      GREATEST(
        COALESCE(user_session_auid_distinct_iuid_count, 0),
        COALESCE(previous_session_auid_distinct_iuid_count, 0)
      ) <= 1
      OR
      (
        GREATEST(
          COALESCE(user_session_auid_distinct_iuid_count, 0),
          COALESCE(previous_session_auid_distinct_iuid_count, 0)
        ) > 1
        AND stitch_gap_minutes <= 5
        AND has_exact_referrer_match
      )
    )
),

journey_session_map AS (
  SELECT
    device_session_id AS session_id,
    journey_id
  FROM approved_device_to_user_journeys

  UNION ALL

  SELECT
    user_session_id AS session_id,
    journey_id
  FROM approved_device_to_user_journeys
),

all_sessions_with_journey_id AS (
  SELECT
    s.*,

    CASE
      WHEN s.auid_distinct_iuid_count > 1 THEN TRUE
      ELSE FALSE
    END AS known_multi_user_device,

    COALESCE(j.journey_id, s.session_id) AS journey_id

  FROM all_sessions s
  LEFT JOIN journey_session_map j
    USING (session_id)
),

final AS (
  SELECT *
  FROM all_sessions_with_journey_id
  ${ctx.incremental()
    ? `WHERE session_start_timestamp >= session_replace_checkpoint`
    : ``}
)
` : `
/* --------------------------------------------------------------------------
   6. Final device-only sessions

   Identity resolution is disabled, so there are no user sessions and no
   device-to-user journey stitching.
-------------------------------------------------------------------------- */

final AS (
  SELECT
    *,
    FALSE AS known_multi_user_device,
    session_id AS journey_id
  FROM device_sessions_final
  ${ctx.incremental()
    ? `WHERE session_start_timestamp >= session_replace_checkpoint`
    : ``}
)
`}

SELECT *
FROM final
`).preOps(ctx => `
DECLARE session_replace_checkpoint TIMESTAMP DEFAULT (
  ${ctx.incremental()
    ? `SELECT TIMESTAMP_SUB(
          COALESCE(MAX(session_start_timestamp), TIMESTAMP(${sqlString(startDate)})),
          INTERVAL ${incrementalReplaceLookbackHours} HOUR
        )
        FROM ${ctx.self()}`
    : `SELECT TIMESTAMP(${sqlString(startDate)})`}
);

DECLARE event_read_checkpoint TIMESTAMP DEFAULT (
  ${ctx.incremental()
    ? `SELECT TIMESTAMP_SUB(
          session_replace_checkpoint,
          INTERVAL ${incrementalReadLookbackHours} HOUR
        )`
    : `SELECT TIMESTAMP(${sqlString(startDate)})`}
);

${ctx.incremental()
  ? `DELETE FROM ${ctx.self()}
     WHERE session_start_timestamp >= session_replace_checkpoint;`
  : ``}
`);
};