const identityConfig = require("../definitions/web_analytics_identity_inference_config_upd");
const parameter_functions = require("./parameter_functions");

module.exports = params => {
  if (!params.enableSessionDetailsTable) {
    return true;
  }

/* -------------------------------------------------------------------------- 
1. Select the session-construction mode 

Generate the session-details table only where it has been enabled for the current service. 
Where web-request identity resolution is enabled, build: 
 - resolved user sessions using the analytics safe identity produced by the identity resolution pipeline
 - anonymous device sessions from unresolved activity
 - journey links between eligible device and user sessions. 
 
Where identity resolution is disabled, build device sessions only. 
------------------------------------------------------------------------------ */

  const enableIdentityResolution =
    params.enableWebRequestIdentityResolution === true;

/* -------------------------------------------------------------------------- 
2. Load optional service specific configuration 

Session construction can operate without service specific identity rules. 
The identity resolution pipeline is responsible for validating its own mandatory configuration. 
Where service configuration is available, use: 
- the configured valid source-data start date
- configured public, sign-in, and sign-out paths. 

These paths help distinguish expected anonymous activity from unresolved identity 
evidence and support narrow pre-auth page stitching. 
------------------------------------------------------------------------------ */

  const webAnalytics = identityConfig[params.eventSourceName] || {};

  const paths = webAnalytics.paths || {};

  const startDate =
    webAnalytics.startDate ||
    "2025-06-01";

/* -------------------------------------------------------------------------- 
3. Define service-specific input and output table names 
Read from: 
- identity_solved_events_[event source] where identity resolution is on
- events_[event source] where identity resolution is off.

Publish one session_details_[event source] table.
------------------------------------------------------------------------------ */

  const finalName = 
    `session_details_${params.eventSourceName}`;

  const rawEventsName =
    `events_${params.eventSourceName}`;

  const identityEventsName =
    `identity_solved_events_${params.eventSourceName}`;

  const sessionInputEventsName = enableIdentityResolution
    ? identityEventsName
    : rawEventsName;

/* -------------------------------------------------------------------------- 
4. Configure paths and incremental overlap periods 
The session table is incremental. Each run: 
- replaces sessions with recent activity
- reads a wider event overlap so sessions crossing the replacement boundary 
can be reconstructed consistently. 

anonymousSafePagePaths identifies pages where anonymous activity is expected: 
- public pre-auth pages
- sign-in pages 
- sign-out pages 

These paths are used only for anonymous session classification and narrowly 
constrained pre auth stitching. 
------------------------------------------------------------------------------ */

  const unique = values => [...new Set(values)];

  const preAuthPagePaths = paths.preAuth || ["/", "/?"];

  const incrementalReadLookbackHours =
    params.sessionIncrementalReadLookbackHours || 12;

  const incrementalReplaceLookbackHours =
    params.sessionIncrementalReplaceLookbackHours || 6;

  const signInPagePaths = paths.signIn || ["/sign-in"];
  const signOutPaths = paths.signOut || ["/sign-out"];

  const anonymousSafePagePaths = unique([
    ...preAuthPagePaths,
    ...signInPagePaths,
    ...signOutPaths
  ]);

/* -------------------------------------------------------------------------- 
5. Define SQL-generation helpers 

These helpers: 
- safely interpolate configured paths into generated SQL
- create valid IN UNNEST conditions for dynamic path lists
- use shared parameter helpers for attribution fields and channel mapping where available
- fall back to a minimal UTM and referral implementation otherwise
------------------------------------------------------------------------------ */
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

/* --------------------------------------------------------------------------
   6. Publish the durable incremental session details table

   Persist one row per constructed session.

   The table is partitioned by session start date and incrementally merged using
   session_id as the stable key. A recent overlap is rebuilt on each run so
   sessions receiving new page activity near the processing boundary can be
   replaced consistently.

   The SQL generated below branches according to whether identity resolution is
   enabled:
     - identity aware mode creates resolved user and anonymous device sessions
     - device only mode creates anonymous device sessions only.
------------------------------------------------------------------------------ */

  return publish(finalName, {
    ...params.defaultConfig,
    type: "incremental",
    uniqueKey: ["session_id"],
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
      ? "This table contains session-level web analytics metrics. This service is configured to use server-side identity resolution. Each row represents one session. Sessions continue while successive observable page requests occur within 30 minutes. A request may also continue the preceding session after a longer gap of up to 3 hours where its referrer exactly matches the preceding page, providing a bounded allowance for unobserved page-reading time. Admin-exposed AUIDs use stable synthetic per-AUID identities for session construction. Bot traffic is excluded."
      : "This table contains session-level web analytics metrics. This service is not configured to infer user identities, so sessions are constructed from anonymous device identifiers only. Each row represents one session. Sessions continue while successive observable page requests occur within 30 minutes. A request may also continue the preceding session after a longer gap of up to 3 hours where its referrer exactly matches the preceding page, providing a bounded allowance for page activity time. Bot traffic is excluded.",
    dependencies: [...(params.dependencies || []), sessionInputEventsName],
    columns: {
    session_id: "Stable hashed session identifier.",
    user_id: "Analytics-safe sessionisation identity. Contains the inferred IUID for ordinary resolved-user sessions, a stable synthetic per-AUID identity for admin-exposed sessions, and NULL for anonymous device sessions.",
    user_signed_in: "True where the session was constructed from a non-null resolved or synthetic sessionisation identity. This is not direct evidence that every page request contains an observed sign-in event.",
    session_type: "Session category: user_session, admin_auid_session, or device_session.",
    admin_exposed_session: "True where the session contains activity on an AUID historically observed on a configured admin page.",
    includes_low_confidence_identity_propagation: "True where the session contains at least one lower-priority inferred identity assignment.",
    includes_stitched_pre_auth_pages: "True where eligible anonymous pre-auth pages were attached to the following resolved-user session.",
    known_multi_user_device: "True where an AUID associated with the session has historically been linked to more than one IUID.",
    multiple_auid_session: "True where a resolved-user session contains more than one AUID.",
    device_id: "Single AUID for a single-AUID session, or a synthetic device identifier for a session spanning multiple AUIDs.",
    journey_id: "Identifier linking an eligible anonymous device session to the immediately following resolved-user session. Defaults to session_id where no journey link is applied."
    }
  }).query(ctx => `

WITH

/* --------------------------------------------------------------------------
   1. Load page view events

   Select successful HTML web requests from the current incremental read scope.

   Exclude:
     - bot traffic;
     - redirects;
     - client-error responses;
     - non-HTML responses.

   The input depends on the configured mode.

   Identity-aware mode:
     - read the final solved-event identity table;
     - use admin_normalised_iuid as the downstream sessionisation identity;
     - retain AUID risk and identity-resolution audit fields for QA.

   admin_normalised_iuid is deliberately used instead of current_iuid.
   For ordinary users it contains the resolved IUID. For historically
   admin exposed AUIDs it contains a stable synthetic per AUID identity. This is necessary
   beacuse admin users can impersonate, edit, or act through privileged flows that do not 
   follow the same observable web request patterns as ordinary users. As a result, their 
   activity can appear to jump between user IDs or contaminate journeys in ways that the 
   propagation model was never designed to interpret. 

   Device only mode:
     - read raw events
     - set user_id to NULL;
     - construct sessions solely from the anonymous device identifier.
------------------------------------------------------------------------------ */

/* This JS function will create the base events table from either the identity resolved events (if enableWebRequestIdentityResolution = true) or the raw events table (if enableWebRequestIdentityResolution = false) */

${enableIdentityResolution ? `

/* --------------------------------------------------------------------------
   1. Base page-view style web events from final identity output

   Identity has already done the heavy attribution work. This layer consumes
   admin_normalised_current_iuid and accompanying resolution metadata.
-------------------------------------------------------------------------- */

events AS (
  SELECT
    request_uuid,
    auid AS anonymised_user_agent_and_ip,
    CAST(admin_normalised_iuid AS STRING) AS user_id,
    current_auid_is_admin_exposed,

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
    identity_resolution_priority,

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
   for device session construction.
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
   2. Construct resolved user sessions

   Run this branch only where identity resolution is enabled.

   Partition resolved events by user_id. In this context user_id is the
   analytics safe sessionisation identity:
     - inferred IUID for ordinary resolved users
     - synthetic per-AUID admin identity for admin-exposed activity

   This permits ordinary resolved sessions to continue across AUID changes while
   preventing unrelated admin devices from being collapsed into one session.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   2.1. Order resolved events and identify session boundaries

   Sequence requests chronologically within each resolved session identity.

   Start a new session where:
     - the event is the first observed event for the identity; or
     - the gap since the preceding event exceeds 30 minutes and there is no
       exact referrer continuation.

   Permit a bounded exception where:
     - the current request refers exactly to the preceding page; and
     - the gap is no more than 180 minutes.

   The exact referrer rule is a conservative server side approximation for cases
   where a user may have remained on a page without generating observable web
   requests. It is deliberately capped at three hours.

   Persist human readable continuity_type_to_current_page values and supporting
   booleans so the contribution of this exception can be audited.
------------------------------------------------------------------------------ */

signed_in_events_with_session_boundaries AS (
  SELECT
    *,
    CASE
      WHEN prev_occurred_at IS NULL THEN TRUE

      WHEN TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 30
        THEN FALSE

      WHEN referer_path_and_query = prev_page_path_and_query
       AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 180
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
       AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 180
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

/* --------------------------------------------------------------------------
   2.2. Assign stable resolveduser session identifiers

   Number sessions cumulatively within each resolved identity after applying the
   boundary rules.

   Identify the first request and first timestamp within each numbered session.

   Create a deterministic session_id from:
     - the session type
     - the resolved sessionisation identity
     - the first event timestamp
     - the first request UUID

   The stable identifier supports incremental MERGE updates where a recent
   session receives additional page activity.
------------------------------------------------------------------------------ */

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
    current_auid_is_admin_exposed,
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
   3. Attach eligible anonymous pre-auth pages to resolved-user sessions

   The identity-resolution pipeline intentionally avoids assigning an IUID to
   some pre-auth public activity. Attach a narrow set of those pages to the
   following resolved session where:
     - the page is an allow-listed pre-auth path
     - the anonymous event uses the same AUID as the first resolved page
     - the event occurred within the preceding 30 minutes

   Apply additional protection for AUIDs historically associated with multiple
   IUIDs: do not attach an anonymous page where a closer resolved session start
   exists on the same AUID.

   This is session enrichment, not identity propagation. The original anonymous
   event remains identifiable through stitched_pre_auth_page.
------------------------------------------------------------------------------ */

pre_auth_pages AS (
  SELECT request_path
  FROM UNNEST(${sqlStringArray(preAuthPagePaths)}) AS request_path
),

stitched_pre_auth_pages AS (
  SELECT DISTINCT
    s.session_id,
    s.user_id,
    s.current_auid_is_admin_exposed,
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
    current_auid_is_admin_exposed,
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
    identity_resolution_priority,
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
    s.current_auid_is_admin_exposed,
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
    e.identity_resolution_priority,
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
   4. Calculate page timings and aggregate resolved-user sessions

   Combine resolved events with eligible stitched pre-auth pages.

   For each session:
     - order page visits chronologically
     - calculate page exit time using the following request
     - calculate observable page duration
     - retain ordered page-level journey details
     - retain first-page acquisition attributes
     - calculate session start, final activity timestamp, and page count

   Also retain session level quality indicators:
     - whether the session uses an admin-exposed AUID
     - whether any assigned identity has lower confidence
     - whether anonymous pre-auth pages were stitched into the session
     - whether the session spans multiple AUIDs

   Label admin exposed sessions separately as admin_auid_session. This keeps
   their behaviour available for reporting while preventing them from being
   interpreted as standard resolved user sessions.
------------------------------------------------------------------------------ */

user_session_page_times AS (
  SELECT
    session_id,
    user_id,
    current_auid_is_admin_exposed,
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
    identity_resolution_priority,

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

    LOGICAL_OR(current_auid_is_admin_exposed) AS admin_exposed_session,

    MAX(COALESCE(auid_distinct_iuid_count, 0)) AS auid_distinct_iuid_count,

    LOGICAL_OR(
      user_id IS NOT NULL
      AND COALESCE(identity_resolution_priority, 0) < 70
    ) AS includes_low_confidence_identity_propagation,

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
    admin_exposed_session,
    includes_low_confidence_identity_propagation,
    includes_stitched_pre_auth_pages,
    TRUE AS user_signed_in,

    CASE
    WHEN admin_exposed_session
        THEN "admin_auid_session"
    ELSE "user_session"
    END AS session_type,

    CASE
    WHEN admin_exposed_session
        THEN "admin_exposed_auid"
    ELSE "known_user"
    END AS behavioural_user_type,

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
   5. Construct anonymous device sessions

   Select anonymous events that were not attached to a resolved-user session.

   In identity aware mode these are unresolved events remaining after pre-auth
   stitching. In devic only mode these are all eligible page-view-style events.

   Partition events by AUID because no resolved identity is available.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.1. Order anonymous events and identify device-session boundaries

   Sequence anonymous activity chronologically within each AUID.

   Start a new device session where:
     - the event is the first observed event for the AUID; or
     - the gap since the preceding event exceeds 30 minutes and there is no
       exact referrer continuation.

   As with resolved user sessions, permit a bounded three-hour continuation
   where the current request refers exactly to the preceding page.

   This exception helps represent long page-reading periods without allowing
   repeated path patterns to join activity indefinitely.
------------------------------------------------------------------------------ */

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
        AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 180
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
        AND TIMESTAMP_DIFF(occurred_at, prev_occurred_at, MINUTE) <= 180
        THEN "Continued from previous device page: exact referrer match within 3 hours"

      ELSE "Started a new device session after a gap"
    END AS continuity_type_to_current_page

  FROM remaining_anonymous_events_ordered
),

/* --------------------------------------------------------------------------
   5.2. Assign stable device-session identifiers and calculate page timings

   Number sessions cumulatively within each AUID after applying the boundary
   rules.

   Create a deterministic device-session identifier from:
     - the session type;
     - the AUID;
     - the first event timestamp;
     - the first request UUID.

   Calculate ordered page timings and retain first-page acquisition attributes
   using the same approach as resolved-user sessions.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.3. Classify anonymous device sessions

   Where identity resolution is enabled, use AUID risk evidence to distinguish:
     - anonymous_only:
         no indication that the session should resolve to a known user;

     - expected_anonymous_pre_auth_only:
         unresolved activity exists but is restricted to expected public or
         authentication-transition pages;

     - unknown_user:
         unresolved evidence exists on meaningful service pages;

     - identity_resolution_anomaly:
         the unresolved session has an unexpected upstream identity state.

   These categories support quality inference (and QA) and downstream interpretation 
   without forcing an identity assignment where the evidence is insufficient.
------------------------------------------------------------------------------ */

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
    ) AS has_any_unresolved_user_evidence,

    LOGICAL_OR(
      auid_risk_classification IN (
        "UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY",
        "HIGH_RISK_MULTI_IUID_AUID"
      )
      AND NOT (${sqlInList("page_path", anonymousSafePagePaths)})
    ) AS has_meaningful_unresolved_user_evidence,

    LOGICAL_AND(
      ${sqlInList("page_path", anonymousSafePagePaths)}
    ) AS contains_only_anonymous_safe_pages,

    LOGICAL_OR(
      auid_risk_classification IS NULL
      OR auid_risk_classification = "UNKNOWN"
      OR auid_risk_classification = "LOW_RISK_EXCLUSIVE_ANCHORED_AUID"
    ) AS has_identity_resolution_anomaly,
    ` : `
    FALSE AS has_any_unresolved_user_evidence,
    FALSE AS has_meaningful_unresolved_user_evidence,
    FALSE AS contains_only_anonymous_safe_pages,
    FALSE AS has_identity_resolution_anomaly,
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
    FALSE AS admin_exposed_session,
    FALSE AS includes_low_confidence_identity_propagation,
    FALSE AS includes_stitched_pre_auth_pages,
    FALSE AS user_signed_in,
    "device_session" AS session_type,

    CASE
        WHEN has_identity_resolution_anomaly
            THEN "identity_resolution_anomaly"

        WHEN has_meaningful_unresolved_user_evidence
            THEN "unknown_user"

        WHEN has_any_unresolved_user_evidence
        AND contains_only_anonymous_safe_pages
            THEN "expected_anonymous_pre_auth_only"

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
   6. Combine resolved-user and anonymous-device sessions

   Union the two session types into one common output schema.

   Retain:
     - sessionisation identity where available;
     - session category;
     - admin-exposure indicator;
     - identity-resolution QA fields;
     - session-level dimensions and metrics;
     - ordered page-level details.
------------------------------------------------------------------------------ */

all_sessions AS (
  SELECT * FROM user_sessions_final
  UNION ALL
  SELECT * FROM device_sessions_final
),

/* --------------------------------------------------------------------------
   7. Link eligible device and user sessions into journeys

   Journey stitching is separate from identity inference.

   Link an immediately preceding anonymous device session to a following
   resolved-user session only where:
     - both sessions meet on the AUID used by the first resolved page;
     - the anonymous session ended no more than 30 minutes earlier;
     - the resolved session is not admin-exposed.

   For AUIDs associated with at most one IUID, allow the short temporal link.

   For AUIDs historically associated with multiple IUIDs, require stricter
   evidence:
     - a gap of no more than five minutes; and
     - an exact referrer match.

   Where one anonymous device session could link to multiple following user
   sessions, retain only the earliest eligible following user session. This
   prevents a single device session from being duplicated across journeys.
------------------------------------------------------------------------------ */

user_session_first_pages AS (
  SELECT
    session_id AS user_session_id,
    user_id,
    current_auid_is_admin_exposed,
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
    u.current_auid_is_admin_exposed,
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

approved_device_to_user_journey_candidates AS (
  SELECT
    user_session_id,
    previous_session_id AS device_session_id,
    first_page_at,

    TO_HEX(SHA256(CONCAT(
      previous_session_id,
      "--",
      user_session_id
    ))) AS journey_id

  FROM immediately_preceding_session_on_user_first_page_auid
  WHERE previous_session_type = "device_session"
    AND current_auid_is_admin_exposed = FALSE
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

approved_device_to_user_journeys AS (
  SELECT * EXCEPT (first_page_at)
  FROM approved_device_to_user_journey_candidates
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY device_session_id
    ORDER BY first_page_at, user_session_id
  ) = 1
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

/* --------------------------------------------------------------------------
   8. Add journey identifiers and select the incremental replacement scope

   Assign:
     - a shared journey_id to approved device-to-user session pairs
     - the original session_id as journey_id where no journey link exists.

   Flag known_multi_user_device where the session contains activity from an
   AUID historically associated with more than one IUID.

   On incremental runs, emit sessions with recent final page activity rather
   than filtering only by session start. This ensures sessions that began before
   the replacement checkpoint but received newer activity are reconstructed and
   merged correctly.
------------------------------------------------------------------------------ */

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
    ? `WHERE final_session_page_timestamp >= session_replace_checkpoint`
    : ``}
)
` : `


final AS (
  SELECT
    *,
    FALSE AS known_multi_user_device,
    session_id AS journey_id
  FROM device_sessions_final
  ${ctx.incremental()
    ? `WHERE final_session_page_timestamp >= session_replace_checkpoint`
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
     WHERE final_session_page_timestamp >= session_replace_checkpoint;`
  : ``}
`);
};