const identityConfig = require("../definitions/web_analytics_identity_inference_config_upd");
const parameter_functions = require("./parameter_functions");

module.exports = params => {
  if (!params.enableWebRequestIdentityResolution) {
    return true;
  }

  /* --------------------------------------------------------------------------
     1. Load service-specific config
  -------------------------------------------------------------------------- */

  const webAnalytics = identityConfig[params.eventSourceName];

  if (!webAnalytics) {
    throw new Error(
      `enableWebRequestIdentityResolution is true but no web analytics config was found for eventSourceName '${params.eventSourceName}'.`
    );
  }

  const identity = webAnalytics.identity || {};
  const paths = webAnalytics.paths || {};
  const startDate =
    webAnalytics.startDate ||
    "2025-06-01"; /* Anonymised user agent and IP was incorrectly captured before this date */

  /* --------------------------------------------------------------------------
     2. Identity config
  -------------------------------------------------------------------------- */

  const anchorSources = identity.anchorSources || [];

  /* --------------------------------------------------------------------------
     3. Path config
  -------------------------------------------------------------------------- */

  const unique = values => [...new Set(values)];

  const preAuthPagePaths = paths.preAuth || ["/", "/?"];
  const signInPagePaths = paths.signIn || ["/sign-in"];
  const signOutPaths = paths.signOut || ["/sign-out"];

  const authPrefixes = paths.authPrefixes || [];
  const adminPagePatterns = paths.adminPatterns || [];

  const preAuthAndSignInPagePaths = unique([
    ...preAuthPagePaths,
    ...signInPagePaths
  ]);

  const publicAndAuthPagePaths = unique([
    ...preAuthPagePaths,
    ...signInPagePaths,
    ...signOutPaths
  ]);

  const excludedPathsForActivity = publicAndAuthPagePaths;

  /* --------------------------------------------------------------------------
     4. Dynamic table names
  -------------------------------------------------------------------------- */

  const stagingSchema = params.stagingDataset || "web_analytics_staging_tables";

  const eventsTableName = `events_${params.eventSourceName}`;

  const classifyEventsName =
    `identity_classify_events_cfg_${params.eventSourceName}`;

  const conservativeWalkName =
    `identity_conservative_recursive_walk_cfg_${params.eventSourceName}`;

  const activityWindowsName =
    `identity_activity_windows_cfg_${params.eventSourceName}`;

  const secondWalkName =
    `identity_second_recursive_walk_cfg_${params.eventSourceName}`;

  const resolvedAdminName =
    `identity_solved_events_cfg_${params.eventSourceName}`;

  /* --------------------------------------------------------------------------
     5. SQL helper functions
  -------------------------------------------------------------------------- */

  function sqlString(value) {
    return `'${String(value).replace(/'/g, "\\'")}'`;
  }

  function sqlStringArray(values = []) {
    return `[${values.map(sqlString).join(", ")}]`;
  }

  function sqlFieldRef(alias, fieldName) {
    return `${alias}.${fieldName}`;
  }

  function sqlInList(field, values = []) {
    if (!values.length) {
      return "FALSE";
    }
    return `${field} IN UNNEST(${sqlStringArray(values)})`;
  }

  function sqlNotInList(field, values = []) {
    if (!values.length) {
      return "TRUE";
    }
    return `${field} NOT IN UNNEST(${sqlStringArray(values)})`;
  }

  function sqlStartsWithAny(field, prefixes = []) {
    if (!prefixes.length) {
      return "FALSE";
    }
    return prefixes
      .map(prefix => `STARTS_WITH(${field}, ${sqlString(prefix)})`)
      .join(" OR ");
  }

  function sqlNotStartsWithAny(field, prefixes = []) {
    if (!prefixes.length) {
      return "TRUE";
    }
    return prefixes
      .map(prefix => `NOT STARTS_WITH(${field}, ${sqlString(prefix)})`)
      .join(" AND ");
  }

  function sqlRegexpContainsAny(field, patterns = []) {
    if (!patterns.length) {
      return "FALSE";
    }
    return patterns
      .map(pattern => `REGEXP_CONTAINS(${field}, ${sqlString(pattern)})`)
      .join(" OR ");
  }

  function sqlPathMatchesAny(field, paths = []) {
    if (!paths.length) {
      return "FALSE";
    }

    return paths
      .map(path => {
        if (path.includes("*")) {
          return `STARTS_WITH(${field}, ${sqlString(path.replace("*", ""))})`;
        }

        return `${field} = ${sqlString(path)}`;
      })
      .join(" OR ");
  }

  function sqlAnchorSourceQueries(alias, sources = []) {
    const validSources = sources.filter(source =>
      source.entityTableName &&
      source.requestPaths?.length &&
      source.dataField &&
      source.userIdKey
    );

    if (!validSources.length) {
      return `
        SELECT
          NULL AS request_uuid,
          NULL AS occurred_at,
          NULL AS event_date,
          NULL AS auid,
          NULL AS iuid
        WHERE FALSE
      `;
    }

    return validSources
      .map(source => `
        SELECT DISTINCT
          ${alias}.request_uuid,
          ${alias}.occurred_at,
          DATE(${alias}.occurred_at) AS event_date,
          ${alias}.anonymised_user_agent_and_ip AS auid,
          d.value[SAFE_OFFSET(0)] AS iuid
        FROM events_base ${alias}
        JOIN UNNEST(${sqlFieldRef(alias, source.dataField)}) AS d
        WHERE ${alias}.entity_table_name = ${sqlString(source.entityTableName)}
          AND (${sqlPathMatchesAny(`${alias}.request_path`, source.requestPaths)})
          AND ${sqlInList(`${alias}.request_method`, source.requestMethods || ["GET"])}
          AND d.key = ${sqlString(source.userIdKey)}
          AND d.value[SAFE_OFFSET(0)] IS NOT NULL
      `)
      .join("\nUNION ALL\n");
  }

  function stagingConfig(description, extra = {}) {
    return {
      ...params.defaultConfig,
      schema: stagingSchema,
      type: "table",
      tags: [params.eventSourceName.toLowerCase(), "identity-staging"],
      description,
      dependencies: params.dependencies,
      ...extra
    };
  }

/* --------------------------------------------------------------------------
   6. Component 1: classify events
-------------------------------------------------------------------------- */

publish(classifyEventsName, stagingConfig("Classify events for identity resolution")).query(ctx => `

WITH

events_base AS (
  SELECT *
  FROM ${ctx.ref(`events_${params.eventSourceName}`)}
  WHERE occurred_at >= TIMESTAMP(${sqlString(startDate)})
    AND anonymised_user_agent_and_ip IS NOT NULL
),

web_events_base AS (
  SELECT *
  FROM events_base
  WHERE event_type = 'web_request'
),

raw_anchors AS (
  ${sqlAnchorSourceQueries("e", anchorSources)}
),

/* De-duplicate anchors as the duplicates will cause an explosion of rows in the self-joins used in the recursive walks */

anchors AS (
  SELECT
    request_uuid,
    MIN(auid) AS auid,
    MIN(event_date) AS event_date,
    MIN(occurred_at) AS occurred_at,
    MIN(iuid) AS iuid
  FROM raw_anchors
  WHERE iuid IS NOT NULL
  GROUP BY request_uuid
),

auid_events_by_date AS (
  SELECT
    anonymised_user_agent_and_ip AS auid,
    DATE(occurred_at) AS event_date,
    COUNT(*) AS event_count
  FROM web_events_base
  GROUP BY 1, 2
),

auid_anchors_by_date AS (
  SELECT
    auid,
    event_date,
    COUNT(*) AS sign_in_event_count,
    COUNT(DISTINCT iuid) AS distinct_iuid_count_on_date,
    ARRAY_AGG(DISTINCT iuid IGNORE NULLS ORDER BY iuid) AS iuids_on_date
  FROM anchors
  GROUP BY 1, 2
),

auid_anchor_summary AS (
  SELECT
    auid,
    COUNT(*) AS total_anchor_event_count,
    COUNT(DISTINCT event_date) AS anchor_date_count,
    COUNT(DISTINCT iuid) AS distinct_iuid_count_ever,
    ARRAY_AGG(DISTINCT iuid IGNORE NULLS ORDER BY iuid) AS iuids_ever,
    MIN(iuid) AS single_known_iuid
  FROM anchors
  GROUP BY auid
),

auid_summary AS (
  SELECT
    e.auid,

    COUNT(DISTINCT e.event_date) AS active_date_count,
    SUM(e.event_count) AS total_event_count,

    IFNULL(MAX(s.total_anchor_event_count), 0) AS total_anchor_event_count,

    SUM(e.event_count) - IFNULL(MAX(s.total_anchor_event_count), 0)
      AS total_non_anchor_event_count,

    IFNULL(MAX(s.anchor_date_count), 0) AS anchor_date_count,
    IFNULL(MAX(s.distinct_iuid_count_ever), 0) AS distinct_iuid_count_ever,
    IFNULL(ANY_VALUE(s.iuids_ever), []) AS iuids_ever,
    ANY_VALUE(s.single_known_iuid) AS single_known_iuid,

    COUNTIF(a.auid IS NULL) AS active_dates_without_sign_in_anchor,

    COUNTIF(
      a.auid IS NOT NULL
      AND a.distinct_iuid_count_on_date = 1
    ) AS active_dates_with_single_iuid_anchor,

    COUNTIF(
      a.auid IS NOT NULL
      AND a.distinct_iuid_count_on_date > 1
    ) AS active_dates_with_multiple_iuid_anchors

  FROM auid_events_by_date e
  LEFT JOIN auid_anchors_by_date a
    ON e.auid = a.auid
   AND e.event_date = a.event_date
  LEFT JOIN auid_anchor_summary s
    ON e.auid = s.auid
  GROUP BY e.auid
),

classified_auids AS (
  SELECT
    *,

    CASE
      WHEN distinct_iuid_count_ever = 0
        THEN 'NO_KNOWN_IUID_ANONYMOUS_ONLY'

      WHEN distinct_iuid_count_ever = 1
        AND active_dates_without_sign_in_anchor = 0
        THEN 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'

      WHEN distinct_iuid_count_ever = 1
        AND active_dates_without_sign_in_anchor > 0
        THEN 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'

      WHEN distinct_iuid_count_ever > 1
        THEN 'HIGH_RISK_MULTI_IUID_AUID'

      ELSE 'UNKNOWN'
    END AS auid_risk_classification,

    CASE
      WHEN distinct_iuid_count_ever = 0
        THEN FALSE

      WHEN distinct_iuid_count_ever = 1
        AND active_dates_without_sign_in_anchor = 0
        THEN FALSE

      ELSE TRUE
    END AS requires_walk

  FROM auid_summary
),

events_with_current_identity_state AS (
  SELECT
    e.request_uuid,
    e.occurred_at,
    DATE(e.occurred_at) AS event_date,
    e.event_type,
    e.anonymised_user_agent_and_ip AS auid,
    e.request_user_id,

    e.request_path,
    e.request_query,
    e.request_path_and_query,
    e.request_referer_path_and_query,
    e.request_referer_domain,
    e.request_method,
    SAFE_CAST(e.response_status AS STRING) AS response_status,
    e.response_content_type,

    e.entity_table_name,
    e.namespace,
    e.device_category,

    IF(a.request_uuid IS NOT NULL, TRUE, FALSE) AS is_sign_in_anchor,

    a.iuid AS known_anchor_iuid,

    c.auid_risk_classification,
    c.requires_walk,

    c.distinct_iuid_count_ever,
    c.iuids_ever,
    c.active_date_count,
    c.anchor_date_count,
    c.active_dates_without_sign_in_anchor,
    c.active_dates_with_single_iuid_anchor,
    c.active_dates_with_multiple_iuid_anchors,
    c.total_event_count AS auid_total_event_count,
    c.total_anchor_event_count AS auid_total_anchor_event_count,
    c.total_non_anchor_event_count AS auid_total_non_anchor_event_count,

    CASE
      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        AND a.iuid IS NULL
        THEN c.single_known_iuid

      ELSE NULL
    END AS initial_inferred_iuid,

    CASE
      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        AND a.iuid IS NULL
        THEN 'INFERRED_FROM_EXCLUSIVE_SINGLE_IUID_AUID_ACTIVITY_ONLY_ON_ANCHORED_DATES'

      WHEN a.iuid IS NOT NULL
        THEN 'NOT_INFERRED_KNOWN_SIGN_IN_ANCHOR_EVENT'

      WHEN c.auid_risk_classification = 'NO_KNOWN_IUID_ANONYMOUS_ONLY'
        THEN 'NOT_INFERRED_NO_SIGN_IN_ANCHOR_AVAILABLE'

      WHEN c.auid_risk_classification = 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'
        THEN 'NOT_INFERRED_REQUIRES_WALK_SINGLE_IUID_WITH_UNANCHORED_ACTIVITY'

      WHEN c.auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        THEN 'NOT_INFERRED_REQUIRES_WALK_MULTI_IUID_AUID'

      ELSE 'NOT_INFERRED_UNKNOWN_RISK_CLASSIFICATION'
    END AS initial_inferred_iuid_method,

    CASE
      WHEN a.iuid IS NOT NULL
        THEN a.iuid

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN c.single_known_iuid

      ELSE NULL
    END AS current_iuid,

    CASE
      WHEN a.iuid IS NOT NULL
        THEN 'KNOWN_SIGN_IN_ANCHOR_EVENT'

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 'NO_WALK_INFERRED_FROM_EXCLUSIVE_SINGLE_IUID_AUID'

      WHEN c.auid_risk_classification = 'NO_KNOWN_IUID_ANONYMOUS_ONLY'
        THEN 'UNRESOLVED_ANONYMOUS_ONLY_AUID'

      WHEN c.auid_risk_classification = 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'
        THEN 'UNRESOLVED_PENDING_WALK_SINGLE_IUID_UNANCHORED_ACTIVITY'

      WHEN c.auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        THEN 'UNRESOLVED_PENDING_WALK_MULTI_IUID_AUID'

      ELSE 'UNRESOLVED_UNKNOWN_RISK_CLASSIFICATION'
    END AS current_iuid_method,

    CASE
      WHEN a.iuid IS NOT NULL
        THEN 'PART_1_KNOWN_ANCHOR'

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 'PART_1_NO_WALK_ASSIGNMENT'

      ELSE 'PART_1_UNRESOLVED'
    END AS current_resolution_stage,

    1 AS current_iteration,

    CASE
      WHEN a.iuid IS NOT NULL
        OR c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN TRUE
      ELSE FALSE
    END AS is_currently_resolved,

    CASE
      WHEN a.iuid IS NOT NULL
        THEN 100

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 80

      ELSE 0
    END AS identity_resolution_priority,

    CASE
      WHEN a.iuid IS NOT NULL
        THEN TRUE
      ELSE FALSE
    END AS identity_resolution_locked

  FROM web_events_base e
  LEFT JOIN anchors a
    ON e.request_uuid = a.request_uuid
  LEFT JOIN classified_auids c
    ON e.anonymised_user_agent_and_ip = c.auid
)

SELECT *
FROM events_with_current_identity_state

`);

/* --------------------------------------------------------------------------
   7. Component 2: conservative recursive walk
-------------------------------------------------------------------------- */

publish(conservativeWalkName, stagingConfig("Run conservative recursive identity walk")).query(ctx => `

WITH RECURSIVE

events_with_part_1_identity AS (
  SELECT *
  FROM ${ctx.ref(classifyEventsName)}
),

anchors_all AS (
  SELECT DISTINCT
    request_uuid,
    known_anchor_iuid AS inferred_user_id,
    auid,
    TRUE AS is_anchor,
    'SIGN_IN_ANCHOR' AS walk_anchor_type
  FROM events_with_part_1_identity
  WHERE is_sign_in_anchor = TRUE
    AND known_anchor_iuid IS NOT NULL
    AND auid IS NOT NULL
),

auid_group_confident_map AS (
  SELECT
    auid,
    MIN(inferred_user_id) AS auid_group_confident,
    COUNT(DISTINCT inferred_user_id) AS auid_distinct_iuid_count
  FROM anchors_all
  GROUP BY auid
),

walk_target_group_times AS (
  SELECT DISTINCT
    c.auid_group_confident,
    e.occurred_at AS target_occurred_at
  FROM events_with_part_1_identity e
  LEFT JOIN auid_group_confident_map c
    ON e.auid = c.auid
  WHERE e.requires_walk = TRUE
    AND c.auid_group_confident IS NOT NULL
),

/*
  Performance optimisation: merged target-window islands.

  Original logic:
    Include all events requiring a walk, plus resolved GET/sign-in events
    in the same confident AUID group that occurred within 2 hours of any
    walk-requiring event.

  Problem:
    Checking every possible supporting event against every individual
    walk-requiring event can create a many-to-many join explosion:
      support events × walk-requiring events

  New logic:
    1. Turn each walk-requiring event into a ±120 minute window.
    2. Merge overlapping windows within each confident group into islands.
    3. Check supporting events against those islands instead of every
       individual target timestamp.

  This preserves the intended "within 2 hours of walk-requiring activity"
  rule while reducing timestamp comparisons.
*/

target_ranges AS (
  SELECT
    auid_group_confident,
    TIMESTAMP_SUB(target_occurred_at, INTERVAL 120 MINUTE) AS range_start,
    TIMESTAMP_ADD(target_occurred_at, INTERVAL 120 MINUTE) AS range_end
  FROM walk_target_group_times
),

ordered_ranges AS (
  SELECT
    *,
    LAG(range_end) OVER (
      PARTITION BY auid_group_confident
      ORDER BY range_start, range_end
    ) AS prev_range_end
  FROM target_ranges
),

range_breaks AS (
  SELECT
    *,
    CASE
      WHEN prev_range_end IS NULL THEN 1
      WHEN range_start > prev_range_end THEN 1
      ELSE 0
    END AS new_island_flag
  FROM ordered_ranges
),

range_islands AS (
  SELECT
    *,
    SUM(new_island_flag) OVER (
      PARTITION BY auid_group_confident
      ORDER BY range_start, range_end
      ROWS UNBOUNDED PRECEDING
    ) AS island_id
  FROM range_breaks
),

target_window_islands AS (
  SELECT
    auid_group_confident,
    island_id,
    MIN(range_start) AS window_start,
    MAX(range_end) AS window_end,
    COUNT(*) AS merged_target_count
  FROM range_islands
  GROUP BY
    auid_group_confident,
    island_id
),

events_base AS (
  SELECT
    e.*,

    CASE
      WHEN e.requires_walk = TRUE
        THEN 'TARGET_REQUIRES_WALK'

      WHEN e.current_iuid IS NOT NULL
        AND (
          e.request_method = 'GET'
          OR e.is_sign_in_anchor = TRUE
        )
        AND EXISTS (
          SELECT 1
          FROM target_window_islands w
          WHERE w.auid_group_confident = c.auid_group_confident
            AND e.occurred_at BETWEEN w.window_start AND w.window_end
        )
        THEN 'SUPPORTING_RESOLVED_PARENT_SAME_GROUP_WITHIN_2H'

      ELSE 'EXCLUDED'
    END AS part_2_inclusion_reason

  FROM events_with_part_1_identity e
  LEFT JOIN auid_group_confident_map c
    ON e.auid = c.auid

  WHERE e.requires_walk = TRUE
     OR (
       e.current_iuid IS NOT NULL
       AND (
         e.request_method = 'GET'
         OR e.is_sign_in_anchor = TRUE
       )
       /* Bring in pre-assigned events that could be chained to */
       AND EXISTS (
         SELECT 1
         FROM target_window_islands w
         WHERE w.auid_group_confident = c.auid_group_confident
           AND e.occurred_at BETWEEN w.window_start AND w.window_end
       )
     )
),

auid_seen_in_anchor AS (
  SELECT DISTINCT
    auid,
    TRUE AS auid_has_anchor
  FROM anchors_all
),

events_supp AS (
  SELECT
    e.*,

CASE
  WHEN a.is_anchor = TRUE
    THEN TRUE

  WHEN e.part_2_inclusion_reason = 'SUPPORTING_RESOLVED_PARENT_SAME_GROUP_WITHIN_2H'
    AND e.current_iuid IS NOT NULL
    THEN TRUE

  ELSE FALSE
END AS walk_is_anchor,

CASE
  WHEN a.is_anchor = TRUE
    THEN a.inferred_user_id

  WHEN e.part_2_inclusion_reason = 'SUPPORTING_RESOLVED_PARENT_SAME_GROUP_WITHIN_2H'
    AND e.current_iuid IS NOT NULL
    THEN e.current_iuid

  ELSE NULL
END AS walk_anchor_iuid,

CASE
  WHEN a.is_anchor = TRUE
    THEN 'SIGN_IN_ANCHOR'

  WHEN e.part_2_inclusion_reason = 'SUPPORTING_RESOLVED_PARENT_SAME_GROUP_WITHIN_2H'
    AND e.current_iuid IS NOT NULL
    THEN 'LOCAL_PART_1_RESOLVED_ANCHOR'

  ELSE NULL
END AS walk_anchor_type,

    c.auid_group_confident,
    c.auid_distinct_iuid_count,

    IFNULL(sa.auid_has_anchor, FALSE) AS auid_has_anchor,

    (
      e.request_method = 'GET'
      AND SAFE_CAST(e.response_status AS STRING) NOT IN ('301', '302', '303', '307', '308')
    ) AS is_navigable_parent,

    TIMESTAMP_TRUNC(e.occurred_at, HOUR) AS hour_bucket

  FROM events_base e
  LEFT JOIN anchors_all a
    ON e.request_uuid = a.request_uuid
  LEFT JOIN auid_group_confident_map c
    ON e.auid = c.auid
  LEFT JOIN auid_seen_in_anchor sa
    ON e.auid = sa.auid
),

base_seq AS (
  SELECT
    e.*,

    ROW_NUMBER() OVER (
      PARTITION BY e.auid
      ORDER BY e.occurred_at, e.request_uuid
    ) AS seq

  FROM events_supp e
  WHERE e.auid IS NOT NULL
),

base_seq_with_prev AS (
  SELECT
    b.*,

    MAX(
      IF((b.is_navigable_parent OR b.walk_is_anchor), b.seq, NULL)
    ) OVER (
      PARTITION BY b.auid
      ORDER BY b.occurred_at, b.request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_parent_seq_auid

  FROM base_seq b
),

prev_by_auid AS (
  SELECT
    c.*,

    p.request_uuid AS prev_parent_request_uuid_auid,
    p.occurred_at AS prev_parent_occurred_at_auid,
    p.request_path AS prev_parent_request_path_auid

  FROM base_seq_with_prev c
  LEFT JOIN base_seq p
    ON p.auid = c.auid
   AND p.seq = c.prev_parent_seq_auid
),

parent_lookup AS (
  SELECT
    request_uuid,
    occurred_at,
    request_path,
    request_path_and_query,
    auid,
    auid_group_confident,
    hour_bucket
  FROM events_supp
  WHERE is_navigable_parent
     OR walk_is_anchor
),

referrer_children AS (
  SELECT
    child.request_uuid,
    child.occurred_at,
    child.request_path,
    child.request_referer_path_and_query,
    child.auid,
    child.auid_group_confident,
    child.hour_bucket
  FROM events_supp child
  WHERE child.request_referer_path_and_query IS NOT NULL
    AND NOT (
      ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
      AND ${sqlNotInList("child.request_path", signInPagePaths)}
    )
),

referrer_child_buckets AS (
  SELECT
    child.*,
    parent_hour_bucket
  FROM referrer_children child
  CROSS JOIN UNNEST([
    child.hour_bucket,
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 1 HOUR),
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 2 HOUR)
  ]) AS parent_hour_bucket
),

p1_referrer_candidates AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    CASE
      WHEN child.auid = parent.auid THEN 'HIGH'
      WHEN child.auid_group_confident IS NOT NULL
        AND child.auid_group_confident = parent.auid_group_confident THEN 'MEDIUM'
      ELSE 'LOW'
    END AS match_confidence,

    CASE
      WHEN child.auid = parent.auid THEN 'P2_REFERRER_SAME_AUID'
      WHEN child.auid_group_confident IS NOT NULL
        AND child.auid_group_confident = parent.auid_group_confident THEN 'P2_REFERRER_SAME_CONFIDENT_AUID_GROUP'
      ELSE 'P2_REFERRER_WEAK'
    END AS match_source

  FROM referrer_child_buckets child
  JOIN parent_lookup parent
    ON child.request_referer_path_and_query = parent.request_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.request_uuid != child.request_uuid
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND (
     child.auid = parent.auid
     OR (
       child.auid_group_confident IS NOT NULL
       AND child.auid_group_confident = parent.auid_group_confident
     )
   )
),

p1_anchor_next_callback AS (
  SELECT
    request_uuid AS anchor_request_uuid,
    auid,
    occurred_at AS anchor_at,

    LEAD(occurred_at) OVER (
      PARTITION BY auid
      ORDER BY occurred_at, request_uuid
    ) AS next_callback_at

  FROM events_supp
  WHERE walk_is_anchor = TRUE
    AND walk_anchor_type = 'SIGN_IN_ANCHOR'
    AND request_method = 'GET'
    AND walk_anchor_iuid IS NOT NULL
    AND auid IS NOT NULL
),

p1_bootstrap_first_child_after_callback AS (
  SELECT
    anc.anchor_request_uuid,
    anc.auid,
    anc.anchor_at,
    anc.next_callback_at,

    child.request_uuid AS child_request_uuid,
    child.occurred_at AS child_at

  FROM p1_anchor_next_callback anc
  JOIN events_supp child
    ON child.auid = anc.auid
   AND child.occurred_at > anc.anchor_at
   AND child.occurred_at <= TIMESTAMP_ADD(anc.anchor_at, INTERVAL 30 MINUTE)
   AND (anc.next_callback_at IS NULL OR child.occurred_at < anc.next_callback_at)
   AND child.walk_is_anchor = FALSE
   AND child.is_navigable_parent = TRUE
  WHERE child.request_referer_path_and_query IS NULL
     OR ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY anc.anchor_request_uuid
    ORDER BY child.occurred_at ASC, child.request_uuid ASC
  ) = 1
),

p1_bootstrap_candidates AS (
  SELECT
    child_request_uuid,
    anchor_request_uuid AS parent_request_uuid,
    child_at,
    anchor_at AS parent_at,
    'MEDIUM' AS match_confidence,
    'P2_BOOTSTRAP_FIRST_EVENT_AFTER_CALLBACK_SAME_AUID_30M' AS match_source
  FROM p1_bootstrap_first_child_after_callback
),

p1_slash_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'LOW' AS match_confidence,
    'P2_HOME_REFERRER_PREVIOUS_PARENT_BY_AUID_10M_SINGLE_IUID_ONLY' AS match_source

  FROM prev_by_auid child
  WHERE ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
    AND child.prev_parent_request_uuid_auid != child.request_uuid
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND ${sqlNotInList("child.prev_parent_request_path_auid", preAuthPagePaths)}
    AND child.auid_distinct_iuid_count = 1
),

p1_null_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'MEDIUM' AS match_confidence,
    'P2_NULL_REFERRER_PREVIOUS_PARENT_BY_AUID_10M_SINGLE_IUID_ONLY' AS match_source

  FROM prev_by_auid child
  WHERE child.request_referer_path_and_query IS NULL
    AND child.auid_distinct_iuid_count = 1
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_request_uuid_auid != child.request_uuid
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND ${sqlNotInList("child.prev_parent_request_path_auid", preAuthAndSignInPagePaths)}
),

p1_parent_candidates AS (
  SELECT * FROM p1_referrer_candidates
  UNION ALL
  SELECT * FROM p1_bootstrap_candidates
  UNION ALL
  SELECT * FROM p1_slash_prev_by_auid_single_user
  UNION ALL
  SELECT * FROM p1_null_prev_by_auid_single_user
),

p1_best_parent AS (
  SELECT
    child_request_uuid,
    best.parent_request_uuid,
    best.match_confidence,
    best.match_source
  FROM (
    SELECT
      child_request_uuid,

      ARRAY_AGG(
        STRUCT(parent_request_uuid, match_confidence, match_source, parent_at)
        ORDER BY
          CASE match_confidence
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            ELSE 1
          END DESC,
          parent_at DESC,
          parent_request_uuid
        LIMIT 1
      )[OFFSET(0)] AS best

    FROM p1_parent_candidates
    GROUP BY child_request_uuid
  )
),

events_with_parent_pass1 AS (
  SELECT
    e.*,

    CASE
      /*
        Exception: allow a POST anchor to attach to its exact same-AUID referrer parent.

        This supports flows where the anchor is created by submitting a page:

          GET  /candidate/sign-in/confirm?token=...
          POST /candidate/sign-in/confirm        -- anchor
          GET  /candidate/application/choices

        The POST is the identity-bearing action caused by the previous GET page,
        so it is valid for the POST anchor to share that strict chain.

        GET anchors remain chain roots because they are more likely to be arrival /
        callback events where identity is already present. Allowing GET anchors to
        point backwards can make anchors bridge separate journeys and create very
        long recursive chains.
      */
      WHEN e.walk_is_anchor
        AND e.request_method = 'POST'
        AND bp.match_source = 'P2_REFERRER_SAME_AUID'
        THEN bp.parent_request_uuid

      WHEN e.walk_is_anchor
        THEN NULL

      ELSE bp.parent_request_uuid
    END AS parent_request_uuid_pass1,

    bp.match_confidence AS parent_match_confidence_pass1,
    bp.match_source AS parent_match_source_pass1

  FROM events_supp e
  LEFT JOIN p1_best_parent bp
    ON e.request_uuid = bp.child_request_uuid
),

walk_pass1 AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.request_uuid AS current_request_uuid,
    e.parent_request_uuid_pass1 AS parent_request_uuid,
    e.walk_is_anchor,
    e.walk_anchor_iuid,
    0 AS depth,

    IF(e.walk_is_anchor, e.walk_anchor_iuid, NULL) AS nearest_anchor_user_id

  FROM events_with_parent_pass1 e

  UNION ALL

  SELECT
    w.start_request_uuid,
    p.request_uuid AS current_request_uuid,
    p.parent_request_uuid_pass1 AS parent_request_uuid,
    p.walk_is_anchor,
    p.walk_anchor_iuid,
    w.depth + 1 AS depth,

    COALESCE(
      w.nearest_anchor_user_id,
      IF(p.walk_is_anchor, p.walk_anchor_iuid, NULL)
    ) AS nearest_anchor_user_id

  FROM walk_pass1 w
  JOIN events_with_parent_pass1 p
    ON w.parent_request_uuid = p.request_uuid
  WHERE w.parent_request_uuid IS NOT NULL
    AND w.parent_request_uuid != w.current_request_uuid
    AND w.depth < 100
),

collapsed_pass1 AS (
  SELECT
    start_request_uuid AS request_uuid,

    MAX_BY(
      current_request_uuid,
      IF(parent_request_uuid IS NULL, 1, 0)
    ) AS chain_id_pass1,

    MIN_BY(
      nearest_anchor_user_id,
      IF(nearest_anchor_user_id IS NOT NULL, depth, 999999)
    ) AS propagated_user_id_pass1

  FROM walk_pass1
  GROUP BY start_request_uuid
),

chain_anchor_summary_pass1 AS (
  SELECT
    c.chain_id_pass1,

    COUNT(DISTINCT e.walk_anchor_iuid) AS chain_distinct_anchor_iuid_count,

    ARRAY_AGG(
      DISTINCT e.walk_anchor_iuid IGNORE NULLS
      ORDER BY e.walk_anchor_iuid
    ) AS chain_anchor_iuids,

    MIN(e.walk_anchor_iuid) AS chain_single_anchor_iuid

  FROM collapsed_pass1 c
  JOIN events_with_parent_pass1 e
    ON c.request_uuid = e.request_uuid
  WHERE e.walk_is_anchor = TRUE
    AND e.walk_anchor_iuid IS NOT NULL
  GROUP BY c.chain_id_pass1
),

shunt_parents AS (
  SELECT
    e.request_uuid,
    e.occurred_at,
    e.request_path,
    e.request_path_and_query,
    e.auid,
    e.hour_bucket

  FROM events_supp e
  WHERE (e.is_navigable_parent OR e.walk_is_anchor)
    AND ${sqlNotInList("e.request_path", preAuthAndSignInPagePaths)}
    AND ${sqlNotStartsWithAny("e.request_path", authPrefixes)}
    AND e.auid IS NOT NULL
),

shunt_children AS (
  SELECT
    e.request_uuid,
    e.occurred_at,
    e.request_referer_path_and_query,
    e.auid,
    e.hour_bucket

  FROM events_supp e
  WHERE e.request_referer_path_and_query IS NOT NULL
    AND ${sqlNotInList("e.request_referer_path_and_query", preAuthPagePaths)}
    AND e.auid IS NOT NULL
),

shunt_child_buckets AS (
  SELECT
    child.*,
    parent_hour_bucket

  FROM shunt_children child
  CROSS JOIN UNNEST([
    child.hour_bucket,
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 1 HOUR),
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 2 HOUR),
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 3 HOUR)
  ]) AS parent_hour_bucket
),

likely_shunt_arrival_candidates AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.auid AS child_auid,
    child.occurred_at AS child_at,

    parent.request_uuid AS shunt_parent_request_uuid,
    parent.auid AS shunt_parent_auid,
    parent.occurred_at AS shunt_parent_at,

    COUNT(*) OVER (
      PARTITION BY child.request_uuid
    ) AS candidate_count_last_3h

  FROM shunt_child_buckets child
  JOIN shunt_parents parent
    ON parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 3 HOUR)
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 2 MINUTE)
   AND child.auid != parent.auid
),

likely_shunt_arrivals AS (
  SELECT
    child_request_uuid,
    TRUE AS likely_shunt_arrival,
    shunt_parent_request_uuid,
    shunt_parent_auid,
    shunt_parent_at
  FROM likely_shunt_arrival_candidates
  WHERE candidate_count_last_3h = 1
),

walked_events AS (
  SELECT
    e.request_uuid,

    e.parent_request_uuid_pass1,
    e.parent_match_confidence_pass1,
    e.parent_match_source_pass1,

    e.auid_group_confident,
    e.auid_distinct_iuid_count,
    e.auid_has_anchor,

    c.chain_id_pass1,
    c.propagated_user_id_pass1,

    cas.chain_distinct_anchor_iuid_count,
    cas.chain_anchor_iuids,
    cas.chain_single_anchor_iuid,

    IFNULL(s.likely_shunt_arrival, FALSE) AS likely_shunt_arrival,
    s.shunt_parent_request_uuid,
    s.shunt_parent_auid,
    s.shunt_parent_at

  FROM events_with_parent_pass1 e
  LEFT JOIN collapsed_pass1 c
    ON e.request_uuid = c.request_uuid
  LEFT JOIN chain_anchor_summary_pass1 cas
    ON c.chain_id_pass1 = cas.chain_id_pass1
  LEFT JOIN likely_shunt_arrivals s
    ON e.request_uuid = s.child_request_uuid
),

final AS (
  SELECT
    p1.* EXCEPT (
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
      current_iteration,
      is_currently_resolved,
      identity_resolution_priority
    ),

    w.parent_request_uuid_pass1,
    w.parent_match_confidence_pass1,
    w.parent_match_source_pass1,

    w.auid_group_confident,
    w.auid_distinct_iuid_count,
    w.auid_has_anchor,

    w.chain_id_pass1,
    w.propagated_user_id_pass1,

    w.likely_shunt_arrival,
    w.shunt_parent_request_uuid,
    w.shunt_parent_auid,
    w.shunt_parent_at,
    w.chain_distinct_anchor_iuid_count,
    w.chain_anchor_iuids,
    w.chain_single_anchor_iuid,

    CASE
      WHEN p1.current_iuid IS NOT NULL
        THEN p1.current_iuid

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN w.chain_single_anchor_iuid

      ELSE NULL
    END AS current_iuid,

    CASE
      WHEN p1.current_iuid IS NOT NULL
        THEN p1.current_iuid_method

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 'PART_2_STRICT_CHAIN_SINGLE_ANCHOR_IUID'

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count > 1
        THEN 'UNRESOLVED_PART_2_STRICT_CHAIN_CONFLICTING_ANCHOR_IUIDS'

      WHEN p1.requires_walk = TRUE
        THEN 'UNRESOLVED_AFTER_PART_2_STRICT_CHAIN_NO_ANCHOR'

      WHEN p1.auid_risk_classification = 'NO_KNOWN_IUID_ANONYMOUS_ONLY'
        THEN 'UNRESOLVED_ANONYMOUS_ONLY_AUID_NOT_WALKED'

      ELSE 'UNRESOLVED_NOT_WALKED'
    END AS current_iuid_method,

    CASE
      WHEN p1.current_iuid IS NOT NULL
        THEN p1.current_resolution_stage

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 'PART_2_CONSERVATIVE_STRICT_CHAIN_SINGLE_ANCHOR'

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count > 1
        THEN 'PART_2_CONFLICT'

      ELSE 'PART_2_UNRESOLVED'
    END AS current_resolution_stage,

    2 AS current_iteration,

    (
      p1.current_iuid IS NOT NULL
      OR (
        p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
      )
    ) AS is_currently_resolved,

    CASE
      WHEN p1.identity_resolution_locked = TRUE
        THEN 100

      WHEN p1.current_iuid IS NOT NULL
        THEN p1.identity_resolution_priority

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 70

      ELSE 0
    END AS identity_resolution_priority,

    CASE
      WHEN p1.current_iuid IS NOT NULL
        THEN p1.current_iuid

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN w.chain_single_anchor_iuid

      ELSE NULL
    END AS final_iuid_after_part_2,

    CASE
      WHEN p1.current_iuid IS NOT NULL
        THEN TRUE

      WHEN p1.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN TRUE

      ELSE FALSE
    END AS is_resolved_after_part_2

  FROM events_with_part_1_identity p1
  LEFT JOIN walked_events w
    ON p1.request_uuid = w.request_uuid
)

SELECT *
FROM final

`);


/* --------------------------------------------------------------------------
   8. Component 3: build and apply activity windows
-------------------------------------------------------------------------- */

publish(activityWindowsName, stagingConfig("Build and apply activity-window identity fallback")).query(ctx => `

WITH

current_identity_resolution_state AS (
  SELECT *
  FROM ${ctx.ref(conservativeWalkName)}
),

events_base AS (
  SELECT
    e.*,

    (
      e.event_type = 'web_request'
      AND e.request_method = 'GET'
      AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X')
        NOT IN ('301','302','303','307','308')
    ) AS is_navigable_activity_event,

    (
      e.current_iuid IS NULL
      AND e.requires_walk = TRUE
      AND e.auid_risk_classification IN (
        'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY',
        'HIGH_RISK_MULTI_IUID_AUID'
      )
    ) AS is_activity_window_candidate

  FROM current_identity_resolution_state e
  WHERE e.occurred_at >= TIMESTAMP(${sqlString(startDate)})
),

/*
  Resolved activity is grouped to minute level before constructing activity
  islands and identity windows.

  Grouping at minute level also reduces the amount of data that needs processing
  as part of the analytic functions, allowing this code to run on larger datasets.

  The downstream islanding logic only needs to identify continuous periods of
  resolved activity for a given:
    - AUID
    - IUID
    - approximate point in time

  Multiple resolved events for the same AUID/IUID occurring within the same
  minute are therefore treated as a single activity point.

  Real event timestamps are still preserved using:
    - first_occurred_at
    - last_occurred_at

  so that final window boundaries continue to reflect actual activity times
  rather than rounded minute values.

*/

resolved_activity_points AS (
  SELECT
    auid,
    current_iuid,
    TIMESTAMP_TRUNC(occurred_at, MINUTE) AS activity_minute,

    MIN(occurred_at) AS first_occurred_at,
    MAX(occurred_at) AS last_occurred_at,

    ARRAY_AGG(
      request_uuid
      ORDER BY occurred_at, request_uuid
      LIMIT 1
    )[OFFSET(0)] AS first_request_uuid,

    ANY_VALUE(distinct_iuid_count_ever) AS distinct_iuid_count_ever

  FROM events_base
  WHERE auid IS NOT NULL
    AND current_iuid IS NOT NULL
    AND (
      is_sign_in_anchor = TRUE
      OR is_navigable_activity_event = TRUE
      OR identity_resolution_priority >= 70
    )
  GROUP BY
    auid,
    current_iuid,
    activity_minute
),

resolved_activity_events AS (
  SELECT
    p.*,

    LAG(p.current_iuid) OVER (
      PARTITION BY p.auid
      ORDER BY p.activity_minute, p.first_occurred_at, p.first_request_uuid
    ) AS previous_resolved_iuid,

    LAG(p.last_occurred_at) OVER (
      PARTITION BY p.auid
      ORDER BY p.activity_minute, p.first_occurred_at, p.first_request_uuid
    ) AS previous_resolved_at

  FROM resolved_activity_points p
),

resolved_activity_events_with_island_flags AS (
  SELECT
    *,

    CASE
      WHEN previous_resolved_at IS NULL
        THEN 1

      WHEN previous_resolved_iuid != current_iuid
        THEN 1

      WHEN TIMESTAMP_DIFF(first_occurred_at, previous_resolved_at, MINUTE)
        > IF(distinct_iuid_count_ever = 1, 180, 10)
        THEN 1

      ELSE 0
    END AS is_new_activity_island

  FROM resolved_activity_events
),

resolved_activity_events_with_island_id AS (
  SELECT
    *,

    SUM(is_new_activity_island) OVER (
      PARTITION BY auid
      ORDER BY activity_minute, first_occurred_at, first_request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS activity_island_id

  FROM resolved_activity_events_with_island_flags
),

identity_anchors AS (
  SELECT
    auid,
    current_iuid AS iuid,

    MIN(first_occurred_at) AS window_start,
    MAX(last_occurred_at) AS island_last_high_conf_activity_at,

    ARRAY_AGG(
      first_request_uuid
      ORDER BY first_occurred_at, first_request_uuid
      LIMIT 1
    )[OFFSET(0)] AS anchor_request_uuid,

    ANY_VALUE(distinct_iuid_count_ever) AS auid_distinct_iuid_count

  FROM resolved_activity_events_with_island_id
  GROUP BY
    auid,
    current_iuid,
    activity_island_id
),

attributed_signouts AS (
  SELECT
    auid,
    current_iuid AS iuid,
    TIMESTAMP_ADD(occurred_at, INTERVAL 1 MICROSECOND) AS signout_at,
    request_uuid AS signout_request_uuid
  FROM events_base
  WHERE ${sqlInList("request_path", signOutPaths)}
    AND current_iuid IS NOT NULL
    AND auid IS NOT NULL
),

anchor_boundaries AS (
  SELECT
    a.*,

    (
      SELECT MIN(a2.window_start)
      FROM identity_anchors a2
      WHERE a2.auid = a.auid
        AND a2.window_start > a.window_start
        AND a2.iuid != a.iuid
    ) AS next_diff_signin_at,

    (
      SELECT MIN(e.occurred_at)
      FROM events_base e
      WHERE e.auid = a.auid
        AND e.occurred_at > a.window_start
        AND e.likely_shunt_arrival = TRUE
        AND (
          e.parent_request_uuid_pass1 IS NULL
          OR e.parent_match_confidence_pass1 NOT IN ('HIGH', 'MEDIUM')
        )
    ) AS next_shunt_arrival_at,

    (
      SELECT MIN(s.signout_at)
      FROM attributed_signouts s
      WHERE s.auid = a.auid
        AND s.iuid = a.iuid
        AND s.signout_at > a.window_start
    ) AS first_attributed_signout_at,

    (
      SELECT
        COUNT(*) > 0
        AND COUNTIF(e.current_iuid IS NOT NULL) = 0
      FROM events_base e
      WHERE e.auid = a.auid
        AND e.occurred_at < a.window_start
        AND e.occurred_at >= TIMESTAMP_SUB(a.window_start, INTERVAL 10 MINUTE)
        AND e.is_navigable_activity_event = TRUE
        AND ${sqlNotStartsWithAny("e.request_path", authPrefixes)}
        AND ${sqlNotInList("e.request_path", excludedPathsForActivity)}
    ) AS preexisting_unattributable_activity_10m,

    TIMESTAMP_ADD(a.window_start, INTERVAL 24 HOUR) AS max_cap_at

  FROM identity_anchors a
),

anchor_last_activity AS (
  SELECT
    b.*,

    b.island_last_high_conf_activity_at AS last_high_conf_activity_at

  FROM anchor_boundaries b
),

window_end_candidates AS (
  SELECT
    a.*,

    TIMESTAMP_ADD(
      a.last_high_conf_activity_at,
      INTERVAL IF(a.auid_distinct_iuid_count = 1, 180, 10) MINUTE
    ) AS inactivity_tail_end_at,

    IF(a.auid_distinct_iuid_count = 1, 180, 10) AS inactivity_tail_minutes

  FROM anchor_last_activity a
),

windows_base AS (
  SELECT
    *,

    LEAST(
      IFNULL(first_attributed_signout_at, max_cap_at),
      IFNULL(inactivity_tail_end_at, max_cap_at),
      max_cap_at
    ) AS chain_end_base,

    LEAST(
      IFNULL(first_attributed_signout_at, max_cap_at),
      max_cap_at
    ) AS hard_end,

    LEAST(
      IFNULL(next_diff_signin_at, max_cap_at),
      IF(
        auid_distinct_iuid_count > 1,
        IFNULL(next_shunt_arrival_at, max_cap_at),
        max_cap_at
      ),
      IFNULL(first_attributed_signout_at, max_cap_at),
      IFNULL(inactivity_tail_end_at, max_cap_at),
      max_cap_at
    ) AS clean_end,

    CASE
      WHEN first_attributed_signout_at IS NOT NULL
        AND first_attributed_signout_at <= IFNULL(inactivity_tail_end_at, max_cap_at)
        AND first_attributed_signout_at <= max_cap_at
        THEN 'ATTRIBUTED_SIGNOUT'

      WHEN inactivity_tail_end_at IS NOT NULL
        AND inactivity_tail_end_at < IFNULL(first_attributed_signout_at, max_cap_at)
        AND inactivity_tail_end_at < max_cap_at
        THEN 'INACTIVITY_TAIL'

      ELSE 'MAX_24H_CAP'
    END AS chain_end_reason

  FROM window_end_candidates
),

overlap_seed AS (
  SELECT auid, window_start AS interval_timestamp FROM windows_base
  UNION DISTINCT
  SELECT auid, chain_end_base AS interval_timestamp FROM windows_base
),

overlap_atomic_intervals AS (
  SELECT
    auid,
    interval_timestamp AS interval_start,
    LEAD(interval_timestamp) OVER (
      PARTITION BY auid
      ORDER BY interval_timestamp
    ) AS interval_end
  FROM overlap_seed
  QUALIFY interval_end IS NOT NULL
    AND interval_start < interval_end
),

overlap_interval_user_count AS (
  SELECT
    ai.auid,
    ai.interval_start,
    ai.interval_end,
    COUNT(DISTINCT w.iuid) AS active_iuid_count
  FROM overlap_atomic_intervals ai
  JOIN windows_base w
    ON w.auid = ai.auid
   AND w.window_start < ai.interval_end
   AND w.chain_end_base > ai.interval_start
  GROUP BY ai.auid, ai.interval_start, ai.interval_end
),

overlap_intervals AS (
  SELECT
    auid,
    interval_start,
    interval_end,
    IF(
      LAG(interval_end) OVER (
        PARTITION BY auid
        ORDER BY interval_start
      ) = interval_start,
      0,
      1
    ) AS is_new_component
  FROM overlap_interval_user_count
  WHERE active_iuid_count > 1
),

overlap_components AS (
  SELECT
    auid,
    interval_start,
    interval_end,
    SUM(is_new_component) OVER (
      PARTITION BY auid
      ORDER BY interval_start
    ) AS overlap_component_id
  FROM overlap_intervals
),

overlap_component_bounds AS (
  SELECT
    auid,
    overlap_component_id,
    MIN(interval_start) AS overlap_start,
    MAX(interval_end) AS overlap_end_seed
  FROM overlap_components
  GROUP BY auid, overlap_component_id
),

overlap_component_windows AS (
  SELECT
    oc.auid,
    oc.overlap_component_id,
    oc.overlap_start,
    oc.overlap_end_seed,
    w.iuid,
    w.window_start,
    w.chain_end_base,
    w.hard_end
  FROM overlap_component_bounds oc
  JOIN windows_base w
    ON w.auid = oc.auid
   AND w.window_start < oc.overlap_end_seed
   AND w.chain_end_base > oc.overlap_start
),

overlap_component_hard_bounds AS (
  SELECT
    auid,
    overlap_component_id,
    MIN(overlap_start) AS overlap_start,
    MAX(hard_end) AS overlap_end_hard
  FROM overlap_component_windows
  GROUP BY auid, overlap_component_id
),

overlap_component_last_device_activity AS (
  SELECT
    oc.auid,
    oc.overlap_component_id,
    MAX(e.occurred_at) AS overlap_last_device_activity_at
  FROM overlap_component_hard_bounds oc
  JOIN events_base e
    ON e.auid = oc.auid
   AND e.occurred_at >= oc.overlap_start
   AND e.occurred_at < oc.overlap_end_hard
   AND e.is_navigable_activity_event = TRUE
  WHERE ${sqlNotStartsWithAny("e.request_path", authPrefixes)}
    AND ${sqlNotInList("e.request_path", signInPagePaths)}
  GROUP BY oc.auid, oc.overlap_component_id
),

windows_overlap_extended AS (
  SELECT
    w.*,

    COALESCE(
      MAX(
        TIMESTAMP_ADD(
          d.overlap_last_device_activity_at,
          INTERVAL w.inactivity_tail_minutes MINUTE
        )
      ),
      TIMESTAMP '1970-01-01 00:00:00+00'
    ) AS overlap_extended_end_candidate

  FROM windows_base w
  LEFT JOIN overlap_component_hard_bounds oc
    ON oc.auid = w.auid
   AND w.window_start < oc.overlap_end_hard
   AND w.hard_end > oc.overlap_start
  LEFT JOIN overlap_component_last_device_activity d
    ON d.auid = oc.auid
   AND d.overlap_component_id = oc.overlap_component_id
  GROUP BY ALL
),

windows_base_final AS (
  SELECT
    w.*,

    LEAST(
      w.max_cap_at,
      w.hard_end,
      GREATEST(w.chain_end_base, w.overlap_extended_end_candidate)
    ) AS chain_end

  FROM windows_overlap_extended w
),

segment_boundaries AS (
  SELECT auid, window_start AS interval_timestamp FROM windows_base_final
  UNION DISTINCT
  SELECT auid, chain_end AS interval_timestamp FROM windows_base_final
  UNION DISTINCT
  SELECT auid, clean_end AS interval_timestamp FROM windows_base_final
),

atomic_intervals AS (
  SELECT
    auid,
    interval_timestamp AS interval_start,
    LEAD(interval_timestamp) OVER (
      PARTITION BY auid
      ORDER BY interval_timestamp
    ) AS interval_end
  FROM segment_boundaries
  QUALIFY interval_end IS NOT NULL
    AND interval_start < interval_end
),

interval_concurrency AS (
  SELECT
    ai.auid,
    ai.interval_start,
    ai.interval_end,
    COUNT(DISTINCT w.iuid) AS active_iuid_count
  FROM atomic_intervals ai
  JOIN windows_base_final w
    ON w.auid = ai.auid
   AND w.window_start < ai.interval_end
   AND w.chain_end > ai.interval_start
  GROUP BY ai.auid, ai.interval_start, ai.interval_end
),

activity_window_segments AS (
  SELECT
    w.auid,
    w.iuid,
    w.anchor_request_uuid,

    GREATEST(w.window_start, c.interval_start) AS window_start,
    LEAST(w.chain_end, c.interval_end) AS window_end,

    c.active_iuid_count,
    w.clean_end,
    w.chain_end,
    w.chain_end_reason,
    w.last_high_conf_activity_at,
    w.inactivity_tail_end_at,
    w.inactivity_tail_minutes,
    w.next_diff_signin_at,
    w.next_shunt_arrival_at,
    w.first_attributed_signout_at,
    w.max_cap_at,

    CASE
      WHEN (
        c.active_iuid_count > 1
        OR (
          w.preexisting_unattributable_activity_10m = TRUE
          AND w.auid_distinct_iuid_count > 1
        )
      )
        THEN 'OVERLAPPING'

      WHEN LEAST(w.chain_end, c.interval_end) <= w.clean_end
        THEN 'CLEAN'

      ELSE 'POST_IDENTITY_CONFLICT'
    END AS window_quality,

    (
      c.active_iuid_count > 1
      OR (
        w.preexisting_unattributable_activity_10m = TRUE
        AND w.auid_distinct_iuid_count > 1
      )
    ) AS is_overlapping,

    (
      w.preexisting_unattributable_activity_10m = TRUE
      AND w.auid_distinct_iuid_count > 1
    ) AS has_unknown_preexisting_activity

  FROM windows_base_final w
  JOIN interval_concurrency c
    ON c.auid = w.auid
   AND w.window_start < c.interval_end
   AND w.chain_end > c.interval_start
  WHERE GREATEST(w.window_start, c.interval_start)
      < LEAST(w.chain_end, c.interval_end)
),

current_activity_windows AS (
  SELECT
    auid,
    iuid,

    window_start,
    window_end,
    window_quality,
    is_overlapping,
    active_iuid_count,
    has_unknown_preexisting_activity,

    anchor_request_uuid,

    clean_end,
    chain_end,
    chain_end_reason,
    last_high_conf_activity_at,
    inactivity_tail_end_at,
    inactivity_tail_minutes,
    next_diff_signin_at,
    next_shunt_arrival_at,
    first_attributed_signout_at,
    max_cap_at,

    (
      window_quality = 'CLEAN'
      AND is_overlapping = FALSE
      AND active_iuid_count = 1
    ) AS is_clean_non_overlapping_window,

    CURRENT_TIMESTAMP() AS activity_windows_rebuilt_at

  FROM activity_window_segments
),

/*
  Performance optimisation: bucketed window matching.

  Original logic:
    Match each candidate event against all activity windows for the same
    AUID using timestamp range predicates:

      e.occurred_at >= w.window_start
      AND e.occurred_at < w.window_end

  Problem:
    For high-activity or high-conflict AUIDs with many overlapping windows,
    this creates an expensive range join explosion:

      candidate events × possible windows

    Even relatively small datasets can therefore become very slow if a small
    number of AUIDs contain many activity windows.

  Optimisation:
    1. Assign candidate events to 15-minute timestamp buckets.
    2. Expand activity windows into the same bucket space.
    3. First join on:
         - AUID
         - bucket
    4. Then apply the exact timestamp predicates afterwards.

  This dramatically reduces the number of candidate window comparisons while
  fully preserving the original matching semantics and exact timestamp logic.
*/

candidate_events_for_window_match AS (
  SELECT
    *,
    TIMESTAMP_BUCKET(occurred_at, INTERVAL 15 MINUTE) AS match_bucket
  FROM events_base
  WHERE is_activity_window_candidate = TRUE
),

current_activity_windows_bucketed AS (
  SELECT
    w.*,
    bucket AS match_bucket
  FROM current_activity_windows w,
  UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_BUCKET(w.window_start, INTERVAL 15 MINUTE),
      TIMESTAMP_BUCKET(TIMESTAMP_SUB(w.window_end, INTERVAL 1 MICROSECOND), INTERVAL 15 MINUTE),
      INTERVAL 15 MINUTE
    )
  ) AS bucket
),


window_matches AS (
  SELECT
    e.request_uuid,

    COUNT(w.iuid) AS matched_windows_total,

    COUNTIF(w.window_quality = 'CLEAN') AS matched_clean_windows,
    COUNTIF(w.window_quality != 'CLEAN') AS matched_non_clean_windows,
    COUNTIF(w.window_quality = 'OVERLAPPING') AS matched_overlapping_windows,
    COUNTIF(w.window_quality = 'POST_IDENTITY_CONFLICT') AS matched_post_conflict_windows,

    COUNTIF(w.has_unknown_preexisting_activity = TRUE)
      AS matched_unknown_preexisting_activity_windows,

    COUNT(DISTINCT w.iuid) AS matched_any_distinct_iuid,

    COUNT(DISTINCT IF(w.window_quality = 'CLEAN', w.iuid, NULL))
      AS matched_clean_distinct_iuid,

    ARRAY_AGG(
      DISTINCT IF(w.window_quality = 'CLEAN', w.iuid, NULL)
      IGNORE NULLS
      ORDER BY IF(w.window_quality = 'CLEAN', w.iuid, NULL)
    ) AS matched_clean_iuids,

    MAX(IF(w.window_quality = 'CLEAN', w.iuid, NULL))
      AS eligible_window_iuid,

    MIN(IF(w.window_quality = 'CLEAN', w.window_start, NULL))
      AS eligible_window_start,

    MAX(IF(w.window_quality = 'CLEAN', w.window_end, NULL))
      AS eligible_window_end,

    MAX(IF(w.window_quality = 'CLEAN', w.window_quality, NULL))
      AS eligible_window_quality,

    MAX(IF(w.window_quality = 'CLEAN', w.active_iuid_count, NULL))
      AS eligible_window_active_iuid_count,

    MAX(IF(w.window_quality = 'CLEAN', w.has_unknown_preexisting_activity, NULL))
      AS eligible_window_has_unknown_preexisting_activity

    FROM candidate_events_for_window_match e
    LEFT JOIN current_activity_windows_bucketed w
      ON e.auid = w.auid
    AND e.match_bucket = w.match_bucket
    AND e.occurred_at >= w.window_start
    AND e.occurred_at < w.window_end
    GROUP BY e.request_uuid
),

window_gate AS (
  SELECT
    *,

    CASE
      WHEN matched_windows_total = 0
        THEN 'NO_WINDOW_MATCH'

      WHEN matched_overlapping_windows > 0
        THEN 'MATCHED_OVERLAPPING_WINDOW'

      WHEN matched_post_conflict_windows > 0
        THEN 'MATCHED_POST_IDENTITY_CONFLICT_WINDOW'

      WHEN matched_unknown_preexisting_activity_windows > 0
        THEN 'MATCHED_UNKNOWN_PREEXISTING_ACTIVITY_WINDOW'

      WHEN matched_clean_windows = 0
        THEN 'MATCHED_WINDOWS_BUT_NO_CLEAN_WINDOW'

      WHEN matched_non_clean_windows > 0
        THEN 'MATCHED_CLEAN_AND_NON_CLEAN_WINDOWS'

      WHEN matched_clean_distinct_iuid > 1
        THEN 'MULTIPLE_CLEAN_IUID_WINDOWS_MATCH'

      WHEN matched_clean_distinct_iuid = 1
        THEN 'ELIGIBLE_SINGLE_CLEAN_IUID_WINDOW'

      ELSE 'WINDOW_MATCH_UNCLASSIFIED'
    END AS window_gate_reason,

    (
      matched_clean_distinct_iuid = 1
      AND matched_clean_windows > 0
      AND matched_non_clean_windows = 0
      AND matched_overlapping_windows = 0
      AND matched_post_conflict_windows = 0
      AND matched_unknown_preexisting_activity_windows = 0
    ) AS has_single_unambiguous_clean_window

  FROM window_matches
),

events_with_window_context AS (
  SELECT
    e.*,

    wg.matched_windows_total,
    wg.matched_clean_windows,
    wg.matched_non_clean_windows,
    wg.matched_overlapping_windows,
    wg.matched_post_conflict_windows,
    wg.matched_unknown_preexisting_activity_windows,
    wg.matched_any_distinct_iuid,
    wg.matched_clean_distinct_iuid,
    wg.matched_clean_iuids,
    wg.eligible_window_iuid,
    wg.eligible_window_start,
    wg.eligible_window_end,
    wg.eligible_window_quality,
    wg.eligible_window_active_iuid_count,
    wg.eligible_window_has_unknown_preexisting_activity,
    wg.window_gate_reason,
    wg.has_single_unambiguous_clean_window,

    (
      ${sqlNotInList("e.request_path", publicAndAuthPagePaths)}
      AND ${sqlNotStartsWithAny("e.request_path", authPrefixes)}
    ) AS is_not_obvious_public_or_auth_event,

    FALSE AS has_previous_resolved_same_iuid_30m,
    FALSE AS has_next_resolved_same_iuid_30m,
    FALSE AS has_referrer_to_previous_resolved_same_iuid,

    (
      wg.eligible_window_start IS NOT NULL
      AND e.occurred_at >= wg.eligible_window_start
      AND e.occurred_at <= TIMESTAMP_ADD(wg.eligible_window_start, INTERVAL 30 MINUTE)
      AND ${sqlNotInList("e.request_path", preAuthPagePaths)}
      AND ${sqlNotInList("e.request_path", signInPagePaths)}
    ) AS is_near_window_start_post_auth_activity

  FROM events_base e
  LEFT JOIN window_gate wg
    ON e.request_uuid = wg.request_uuid
),

window_assignment_decision AS (
  SELECT
    *,

    CASE
      WHEN is_activity_window_candidate = FALSE
        THEN FALSE

      WHEN has_single_unambiguous_clean_window IS NOT TRUE
        THEN FALSE

      WHEN auid_risk_classification = 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'
        THEN TRUE

      WHEN auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        AND eligible_window_active_iuid_count = 1
        AND COALESCE(eligible_window_has_unknown_preexisting_activity, FALSE) = FALSE
        THEN TRUE

      ELSE FALSE
    END AS should_apply_window_iuid,

    CASE
      WHEN is_activity_window_candidate = FALSE
        THEN 'NOT_ACTIVITY_WINDOW_CANDIDATE'

      WHEN has_single_unambiguous_clean_window IS NOT TRUE
        THEN COALESCE(window_gate_reason, 'NO_WINDOW_MATCH')

      WHEN auid_risk_classification = 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'
        THEN 'SINGLE_IUID_UNANCHORED_AUID_SINGLE_CLEAN_WINDOW'

      WHEN auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        AND eligible_window_active_iuid_count = 1
        AND COALESCE(eligible_window_has_unknown_preexisting_activity, FALSE) = FALSE
        THEN 'MULTI_IUID_AUID_SINGLE_CLEAN_NON_AMBIGUOUS_WINDOW'

      WHEN auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        THEN 'MULTI_IUID_AUID_CLEAN_WINDOW_BUT_FAILED_SAFETY_GATE'

      ELSE 'ACTIVITY_WINDOW_ASSIGNMENT_UNCLASSIFIED'
    END AS window_assignment_reason,

    CASE
      WHEN is_near_window_start_post_auth_activity = TRUE
        THEN 'NEAR_WINDOW_START_POST_AUTH_ACTIVITY'

      WHEN has_single_unambiguous_clean_window = TRUE
        THEN 'CLEAN_WINDOW_ONLY'

      ELSE 'NO_SUPPORTING_JOURNEY_EVIDENCE'
    END AS window_assignment_supporting_evidence

  FROM events_with_window_context
),

window_assignments_to_apply AS (
  SELECT
    request_uuid,

    eligible_window_iuid AS applied_iuid,

    CASE
      WHEN auid_risk_classification = 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'
        THEN 'ACTIVITY_WINDOW_CLEAN_FALLBACK_SINGLE_IUID_AUID'

      WHEN auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        THEN 'ACTIVITY_WINDOW_CLEAN_FALLBACK_MULTI_IUID_AUID'

      ELSE 'ACTIVITY_WINDOW_CLEAN_FALLBACK'
    END AS applied_iuid_method,

    'PART_3_ACTIVITY_WINDOW_FALLBACK' AS applied_resolution_stage,

    window_assignment_reason,
    window_assignment_supporting_evidence,

    matched_windows_total,
    matched_clean_windows,
    matched_non_clean_windows,
    matched_overlapping_windows,
    matched_post_conflict_windows,
    matched_unknown_preexisting_activity_windows,
    matched_any_distinct_iuid,
    matched_clean_distinct_iuid,
    matched_clean_iuids,

    eligible_window_start,
    eligible_window_end,
    eligible_window_quality,
    eligible_window_active_iuid_count,
    eligible_window_has_unknown_preexisting_activity,

    window_gate_reason,
    has_single_unambiguous_clean_window,

    has_previous_resolved_same_iuid_30m,
    has_next_resolved_same_iuid_30m,
    has_referrer_to_previous_resolved_same_iuid,
    is_near_window_start_post_auth_activity,

    CURRENT_TIMESTAMP() AS window_assignment_applied_at

  FROM window_assignment_decision
  WHERE should_apply_window_iuid = TRUE
),

current_state_after_window_assignments AS (
  SELECT
    e.* EXCEPT (
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
      current_iteration,
      is_currently_resolved,
      identity_resolution_priority
    ),

    COALESCE(e.current_iuid, a.applied_iuid) AS current_iuid,

    CASE
      WHEN e.current_iuid IS NOT NULL
        THEN e.current_iuid_method

      WHEN a.applied_iuid IS NOT NULL
        THEN a.applied_iuid_method

      ELSE e.current_iuid_method
    END AS current_iuid_method,

    CASE
      WHEN e.current_iuid IS NOT NULL
        THEN e.current_resolution_stage

      WHEN a.applied_iuid IS NOT NULL
        THEN a.applied_resolution_stage

      ELSE e.current_resolution_stage
    END AS current_resolution_stage,

    3 AS current_iteration,

    COALESCE(e.current_iuid, a.applied_iuid) IS NOT NULL
      AS is_currently_resolved,

    CASE
      WHEN e.identity_resolution_locked = TRUE
        THEN 100

      WHEN e.current_iuid IS NOT NULL
        THEN e.identity_resolution_priority

      WHEN a.applied_iuid IS NOT NULL
        THEN 60

      ELSE 0
    END AS identity_resolution_priority,

    a.window_assignment_reason,
    a.window_assignment_supporting_evidence,
    COALESCE(a.matched_windows_total, 0) AS matched_windows_total,
    COALESCE(a.matched_clean_windows, 0) AS matched_clean_windows,
    COALESCE(a.matched_non_clean_windows, 0) AS matched_non_clean_windows,
    COALESCE(a.matched_overlapping_windows, 0) AS matched_overlapping_windows,
    COALESCE(a.matched_post_conflict_windows, 0) AS matched_post_conflict_windows,
    COALESCE(a.matched_unknown_preexisting_activity_windows, 0) AS matched_unknown_preexisting_activity_windows,

    a.matched_any_distinct_iuid,
    a.matched_clean_distinct_iuid,
    a.matched_clean_iuids,
    a.applied_iuid AS eligible_window_iuid,
    a.eligible_window_start,
    a.eligible_window_end,
    a.eligible_window_quality,
    a.eligible_window_active_iuid_count,
    a.eligible_window_has_unknown_preexisting_activity,
    a.window_gate_reason,
    a.has_single_unambiguous_clean_window,
    a.has_previous_resolved_same_iuid_30m,
    a.has_next_resolved_same_iuid_30m,
    a.has_referrer_to_previous_resolved_same_iuid,
    a.is_near_window_start_post_auth_activity,
    a.window_assignment_applied_at,

    a.applied_iuid IS NOT NULL AS assigned_by_activity_window_fallback

  FROM current_identity_resolution_state e
  LEFT JOIN window_assignments_to_apply a
    ON e.request_uuid = a.request_uuid
)

SELECT *
FROM current_state_after_window_assignments

`);

/* --------------------------------------------------------------------------
9. Component 4: second recursive walk / post-window repair attribution


publish(
  secondWalkName,
  stagingConfig(
    "Run post-window repair walk from unresolved events to resolved neighbours"
  )
).query(ctx => `

WITH RECURSIVE

current_state AS (
  SELECT *
  FROM ${ctx.ref(activityWindowsName)}
),

events_base AS (
  SELECT
    e.*,

    (
      e.event_type = 'web_request'
      AND e.request_method = 'GET'
      AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X')
        NOT IN ('301','302','303','307','308')
    ) AS is_navigable_parent,

    e.current_iuid IS NOT NULL AS has_current_identity,

    (
      e.current_iuid IS NULL
      AND e.requires_walk = TRUE
      AND e.identity_resolution_locked = FALSE
      AND e.auid_risk_classification IN (
        'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY',
        'HIGH_RISK_MULTI_IUID_AUID'
      )
    ) AS is_repair_walk_candidate,

    (
      COALESCE(e.matched_overlapping_windows, 0) > 0
      OR COALESCE(e.matched_post_conflict_windows, 0) > 0
    ) AS has_negative_window_context,

    (
      COALESCE(e.matched_unknown_preexisting_activity_windows, 0) > 0
      OR COALESCE(
        e.eligible_window_has_unknown_preexisting_activity,
        FALSE
      ) = TRUE
    ) AS has_unknown_preexisting_activity_risk,

    TIMESTAMP_TRUNC(e.occurred_at, HOUR) AS hour_bucket

  FROM current_state e
  WHERE e.occurred_at >= TIMESTAMP(${sqlString(startDate)})
    AND e.auid IS NOT NULL
),

candidate_auids AS (
  SELECT DISTINCT auid
  FROM events_base
  WHERE is_repair_walk_candidate = TRUE
),

events_scope AS (
  /*
    Starts are unresolved repair candidates.

    Support/intermediate events are deliberately broader than the starts so
    unresolved chains can traverse through intermediate requests and terminate
    at already-resolved events.
 

  SELECT e.*
  FROM events_base e
  JOIN candidate_auids c
    ON e.auid = c.auid
  WHERE
    e.is_repair_walk_candidate = TRUE
    OR e.is_navigable_parent = TRUE
    OR e.has_current_identity = TRUE
    OR (
      e.event_type = 'web_request'
      AND e.request_referer_path_and_query IS NOT NULL
    )
),

base_seq AS (
  SELECT
    e.*,

    ROW_NUMBER() OVER (
      PARTITION BY e.auid
      ORDER BY e.occurred_at, e.request_uuid
    ) AS seq

  FROM events_scope e
),

base_seq_with_prev AS (
  SELECT
    b.*,

    MAX(
      IF(
        b.is_navigable_parent OR b.has_current_identity,
        b.seq,
        NULL
      )
    ) OVER (
      PARTITION BY b.auid
      ORDER BY b.occurred_at, b.request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_parent_seq_auid

  FROM base_seq b
),

prev_by_auid AS (
  SELECT
    c.*,

    p.request_uuid AS prev_parent_request_uuid_auid,
    p.occurred_at AS prev_parent_occurred_at_auid,
    p.request_path AS prev_parent_request_path_auid,
    p.current_iuid AS prev_parent_current_iuid,
    p.has_current_identity AS prev_parent_has_current_identity

  FROM base_seq_with_prev c

  LEFT JOIN base_seq p
    ON p.auid = c.auid
    AND p.seq = c.prev_parent_seq_auid
),

parent_lookup AS (
  SELECT
    request_uuid,
    occurred_at,
    request_path,
    request_path_and_query,
    auid,
    current_iuid,
    has_current_identity,
    is_navigable_parent,
    hour_bucket

  FROM events_scope

  WHERE
    is_navigable_parent = TRUE
    OR has_current_identity = TRUE
),

children_to_repair AS (
  SELECT *
  FROM events_scope
  WHERE is_repair_walk_candidate = TRUE
),

/* --------------------------------------------------------------------------
Rule 1: explicit referrer to resolved or unresolved same-AUID parent
-------------------------------------------------------------------------- 

referrer_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE
    child.request_referer_path_and_query IS NOT NULL
    AND NOT (
      ${sqlInList(
        "child.request_referer_path_and_query",
        preAuthPagePaths
      )}
      AND ${sqlNotInList("child.request_path", signInPagePaths)}
    )
),

referrer_child_buckets AS (
  SELECT
    child.*,
    parent_hour_bucket

  FROM referrer_children child

  CROSS JOIN UNNEST([
    child.hour_bucket,
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 1 HOUR),
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 2 HOUR)
  ]) AS parent_hour_bucket
),

repair_referrer_candidates AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    CASE
      WHEN parent.has_current_identity = TRUE THEN 'HIGH'
      ELSE 'MEDIUM'
    END AS match_confidence,

    CASE
      WHEN parent.has_current_identity = TRUE
        THEN 'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
      ELSE 'PART_4_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
    END AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM referrer_child_buckets child

  JOIN parent_lookup parent
    ON parent.auid = child.auid
    AND parent.request_path_and_query =
      child.request_referer_path_and_query
    AND parent.hour_bucket = child.parent_hour_bucket
    AND parent.occurred_at < child.occurred_at
    AND parent.occurred_at >= TIMESTAMP_SUB(
      child.occurred_at,
      INTERVAL 120 MINUTE
    )
),

/* --------------------------------------------------------------------------
Rule 2: short-gap previous parent for null/home referrer
-------------------------------------------------------------------------- 

repair_home_or_null_prev_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,

    CASE
      WHEN child.prev_parent_current_iuid IS NOT NULL THEN 'MEDIUM'
      ELSE 'LOW'
    END AS match_confidence,

    CASE
      WHEN child.request_referer_path_and_query IS NULL
        AND child.prev_parent_current_iuid IS NOT NULL
        THEN 'PART_4_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      WHEN child.request_referer_path_and_query IS NULL
        THEN 'PART_4_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'

      WHEN child.prev_parent_current_iuid IS NOT NULL
        THEN 'PART_4_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      ELSE 'PART_4_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
    END AS match_source,

    TRUE AS is_weak_previous_parent_rule

  FROM prev_by_auid child

  WHERE
    child.is_repair_walk_candidate = TRUE
    AND (
      child.request_referer_path_and_query IS NULL
      OR ${sqlInList(
        "child.request_referer_path_and_query",
        preAuthPagePaths
      )}
    )
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(
      child.occurred_at,
      INTERVAL 30 MINUTE
    )
    AND ${sqlNotInList(
      "child.prev_parent_request_path_auid",
      preAuthPagePaths
    )}
),

/* --------------------------------------------------------------------------
Rule 3: near-window-start previous resolved parent

This is restored because it was likely recovering valid post-window-start
activity not captured by the main clean-window fallback.
-------------------------------------------------------------------------- 

repair_near_window_start_resolved_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    'MEDIUM' AS match_confidence,

    'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'
      AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM children_to_repair child

  JOIN parent_lookup parent
    ON parent.auid = child.auid
    AND parent.has_current_identity = TRUE
    AND parent.occurred_at < child.occurred_at
    AND parent.occurred_at >= TIMESTAMP_SUB(
      child.occurred_at,
      INTERVAL 30 MINUTE
    )

  WHERE
    child.eligible_window_start IS NOT NULL
    AND child.occurred_at >= child.eligible_window_start
    AND child.occurred_at <= TIMESTAMP_ADD(
      child.eligible_window_start,
      INTERVAL 30 MINUTE
    )
    AND ${sqlNotInList("child.request_path", preAuthPagePaths)}
    AND ${sqlNotInList("child.request_path", signInPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY parent.occurred_at DESC, parent.request_uuid
  ) = 1
),

parent_candidates AS (
  SELECT * FROM repair_referrer_candidates

  UNION ALL

  SELECT * FROM repair_home_or_null_prev_parent

  UNION ALL

  SELECT * FROM repair_near_window_start_resolved_parent
),

best_parent AS (
  SELECT
    child_request_uuid,

    best.parent_request_uuid,
    best.match_confidence,
    best.match_source,
    best.is_weak_previous_parent_rule

  FROM (
    SELECT
      child_request_uuid,

      ARRAY_AGG(
        STRUCT(
          parent_request_uuid,
          match_confidence,
          match_source,
          parent_at,
          is_weak_previous_parent_rule
        )
        ORDER BY
          CASE match_confidence
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            WHEN 'LOW' THEN 1
            ELSE 0
          END DESC,
          parent_at DESC,
          parent_request_uuid
        LIMIT 1
      )[OFFSET(0)] AS best

    FROM parent_candidates
    GROUP BY child_request_uuid
  )
),

events_with_parent AS (
  SELECT
    e.*,

    CASE
      WHEN e.has_current_identity = TRUE THEN NULL
      ELSE bp.parent_request_uuid
    END AS parent_request_uuid_repair,

    bp.match_confidence AS parent_match_confidence_repair,
    bp.match_source AS parent_match_source_repair,
    bp.is_weak_previous_parent_rule
      AS parent_is_weak_previous_parent_rule_repair

  FROM events_scope e

  LEFT JOIN best_parent bp
    ON e.request_uuid = bp.child_request_uuid
),

repair_walk AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.request_uuid AS current_request_uuid,
    e.occurred_at AS current_occurred_at,
    e.parent_request_uuid_repair AS parent_request_uuid,
    e.has_current_identity,
    e.current_iuid,
    0 AS depth,

    IF(
      e.has_current_identity,
      e.current_iuid,
      NULL
    ) AS nearest_current_iuid

  FROM events_with_parent e

  WHERE e.is_repair_walk_candidate = TRUE

  UNION ALL

  SELECT
    w.start_request_uuid,
    p.request_uuid AS current_request_uuid,
    p.occurred_at AS current_occurred_at,
    p.parent_request_uuid_repair AS parent_request_uuid,
    p.has_current_identity,
    p.current_iuid,
    w.depth + 1 AS depth,

    COALESCE(
      w.nearest_current_iuid,
      IF(p.has_current_identity, p.current_iuid, NULL)
    ) AS nearest_current_iuid

  FROM repair_walk w

  JOIN events_with_parent p
    ON w.parent_request_uuid = p.request_uuid

  WHERE
    w.parent_request_uuid IS NOT NULL
    AND w.nearest_current_iuid IS NULL
    AND w.depth < 25
),

collapsed_repair_walk AS (
  SELECT
    start_request_uuid AS request_uuid,

    ARRAY_AGG(
      STRUCT(
        current_request_uuid,
        current_occurred_at,
        depth,
        nearest_current_iuid
      )
      ORDER BY
        IF(nearest_current_iuid IS NOT NULL, 0, 1),
        depth ASC
      LIMIT 1
    )[OFFSET(0)] AS best_repair

  FROM repair_walk
  GROUP BY start_request_uuid
),

repair_assignments AS (
  SELECT
    c.request_uuid,

    c.best_repair.current_request_uuid
      AS repair_source_request_uuid,

    c.best_repair.current_occurred_at
      AS repair_source_occurred_at,

    c.best_repair.depth AS repair_depth,

    c.best_repair.nearest_current_iuid
      AS repaired_iuid

  FROM collapsed_repair_walk c

  WHERE c.best_repair.nearest_current_iuid IS NOT NULL
),

repair_walk_proposals AS (
  SELECT
    e.request_uuid,

    r.repaired_iuid AS proposed_iuid,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair =
          'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair =
          'PART_4_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_REFERRER_CHAIN_TO_RESOLVED_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'PART_4_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M',
          'PART_4_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'PART_4_PREVIOUS_RESOLVED_PARENT_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'PART_4_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M',
          'PART_4_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'PART_4_PREVIOUS_PARENT_CHAIN_TO_RESOLVED_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair =
          'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        THEN 'PART_4_REPAIR_WALK_TO_NEAREST_CURRENT_IDENTITY'

      ELSE 'UNRESOLVED_AFTER_PART_4_REPAIR_WALK'
    END AS proposed_iuid_method,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        THEN 'PART_4_SECOND_RECURSIVE_WALK'
      ELSE NULL
    END AS proposed_resolution_stage,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_confidence_repair = 'HIGH'
        THEN 55

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_confidence_repair = 'MEDIUM'
        THEN 50

      WHEN r.repaired_iuid IS NOT NULL
        THEN 45

      ELSE 0
    END AS proposed_identity_resolution_priority,

    r.repair_source_request_uuid,
    r.repair_source_occurred_at,
    r.repair_depth,

    p.parent_request_uuid_repair,
    p.parent_match_confidence_repair,
    p.parent_match_source_repair,
    p.parent_is_weak_previous_parent_rule_repair,

    (
      r.repaired_iuid IS NOT NULL

      /*
        If Part 3 produced an eligible clean-window identity for the event,
        Part 4 must not contradict it.
      
      AND (
        e.eligible_window_iuid IS NULL
        OR e.eligible_window_iuid = r.repaired_iuid
      )

      /*
        For high-risk shared AUIDs, do not apply weak previous-parent repairs
        where there is explicit unknown-preexisting-activity risk.
        Strong referrer-based repairs are still allowed because they are direct
        deterministic navigation evidence.
    
      AND (
        e.auid_risk_classification != 'HIGH_RISK_MULTI_IUID_AUID'
        OR (
          COALESCE(
            p.parent_is_weak_previous_parent_rule_repair,
            FALSE
          ) = FALSE
          OR e.has_unknown_preexisting_activity_risk = FALSE
        )
      )

      AND NOT EXISTS (
        SELECT 1
        FROM events_base s
        WHERE s.auid = e.auid
          AND ${sqlInList("s.request_path", signOutPaths)}
          AND s.current_iuid = r.repaired_iuid
          AND s.occurred_at > LEAST(
            e.occurred_at,
            r.repair_source_occurred_at
          )
          AND s.occurred_at < GREATEST(
            e.occurred_at,
            r.repair_source_occurred_at
          )
      )
    ) AS should_apply_proposed_iuid,

    CASE
      WHEN r.repaired_iuid IS NULL
        THEN 'NO_REPAIRED_IUID_FOUND'

      WHEN e.eligible_window_iuid IS NOT NULL
        AND e.eligible_window_iuid != r.repaired_iuid
        THEN 'BLOCKED_PROPOSED_IUID_CONFLICTS_WITH_ELIGIBLE_WINDOW_IUID'

      WHEN e.auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        AND COALESCE(
          p.parent_is_weak_previous_parent_rule_repair,
          FALSE
        ) = TRUE
        AND e.has_unknown_preexisting_activity_risk = TRUE
        THEN 'BLOCKED_HIGH_RISK_WEAK_PARENT_WITH_UNKNOWN_PREEXISTING_ACTIVITY'

      WHEN EXISTS (
        SELECT 1
        FROM events_base s
        WHERE s.auid = e.auid
          AND ${sqlInList("s.request_path", signOutPaths)}
          AND s.current_iuid = r.repaired_iuid
          AND s.occurred_at > LEAST(
            e.occurred_at,
            r.repair_source_occurred_at
          )
          AND s.occurred_at < GREATEST(
            e.occurred_at,
            r.repair_source_occurred_at
          )
      )
        THEN 'BLOCKED_SIGNOUT_BETWEEN_EVENT_AND_REPAIR_SOURCE'

      ELSE 'ELIGIBLE_REPAIR_WALK_ASSIGNMENT'
    END AS repair_walk_gate_reason,

    CURRENT_TIMESTAMP() AS proposed_assignment_created_at

  FROM events_scope e

  LEFT JOIN events_with_parent p
    ON e.request_uuid = p.request_uuid

  LEFT JOIN repair_assignments r
    ON e.request_uuid = r.request_uuid

  WHERE e.is_repair_walk_candidate = TRUE
),

deduplicated_repair_walk_proposals AS (
  SELECT
    request_uuid,

    ARRAY_AGG(
      STRUCT(
        proposed_iuid,
        proposed_iuid_method,
        proposed_resolution_stage,
        proposed_identity_resolution_priority,
        repair_source_request_uuid,
        repair_source_occurred_at,
        repair_depth,
        parent_request_uuid_repair,
        parent_match_confidence_repair,
        parent_match_source_repair,
        parent_is_weak_previous_parent_rule_repair,
        repair_walk_gate_reason,
        proposed_assignment_created_at
      )
      ORDER BY
        proposed_identity_resolution_priority DESC,
        repair_depth ASC,
        proposed_assignment_created_at ASC,
        proposed_iuid
      LIMIT 1
    )[OFFSET(0)] AS best_proposal

  FROM repair_walk_proposals

  WHERE
    should_apply_proposed_iuid = TRUE
    AND proposed_iuid IS NOT NULL

  GROUP BY request_uuid
),

updated_current_state AS (
  SELECT
    c.* EXCEPT (
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
      current_iteration,
      is_currently_resolved,
      identity_resolution_priority
    ),

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_iuid
      ELSE c.current_iuid
    END AS current_iuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_iuid_method
      ELSE c.current_iuid_method
    END AS current_iuid_method,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_resolution_stage
      ELSE c.current_resolution_stage
    END AS current_resolution_stage,

    4 AS current_iteration,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN TRUE
      ELSE c.is_currently_resolved
    END AS is_currently_resolved,

    CASE
      WHEN c.identity_resolution_locked = TRUE
        THEN 100

      WHEN c.current_iuid IS NOT NULL
        THEN c.identity_resolution_priority

      WHEN p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_identity_resolution_priority

      ELSE c.identity_resolution_priority
    END AS identity_resolution_priority,

    p.best_proposal.proposed_assignment_created_at
      AS repair_walk_assignment_created_at,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN TRUE
      ELSE FALSE
    END AS repair_walk_assignment_applied,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_source_request_uuid
      ELSE NULL
    END AS repair_walk_source_request_uuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_source_occurred_at
      ELSE NULL
    END AS repair_walk_source_occurred_at,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_depth
      ELSE NULL
    END AS repair_walk_depth,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_request_uuid_repair
      ELSE NULL
    END AS repair_walk_parent_request_uuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_match_confidence_repair
      ELSE NULL
    END AS repair_walk_parent_match_confidence,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_match_source_repair
      ELSE NULL
    END AS repair_walk_parent_match_source,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_is_weak_previous_parent_rule_repair
      ELSE NULL
    END AS repair_walk_parent_is_weak_previous_parent_rule,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_walk_gate_reason
      ELSE NULL
    END AS repair_walk_gate_reason,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN CURRENT_TIMESTAMP()
      ELSE NULL
    END AS repair_walk_assignment_applied_at

  FROM current_state c

  LEFT JOIN deduplicated_repair_walk_proposals p
    ON c.request_uuid = p.request_uuid
)

SELECT *
FROM updated_current_state

`);
-------------------------------------------------------------------------- */
/* --------------------------------------------------------------------------
   9. Component 4: second recursive walk / post-window repair attribution
-------------------------------------------------------------------------- */

publish(secondWalkName, stagingConfig("Run post-window repair walk from unresolved events to resolved neighbours")).query(ctx => `

WITH RECURSIVE

current_state AS (
  SELECT *
  FROM ${ctx.ref(activityWindowsName)}
),

events_base AS (
  SELECT
    e.*,

    (
      e.event_type = 'web_request'
      AND e.request_method = 'GET'
      AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X')
        NOT IN ('301','302','303','307','308')
    ) AS is_navigable_parent,

    e.current_iuid IS NOT NULL AS has_current_identity,

    (
      e.current_iuid IS NULL
      AND e.requires_walk = TRUE
      AND e.identity_resolution_locked = FALSE
      AND e.auid_risk_classification IN (
        'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY',
        'HIGH_RISK_MULTI_IUID_AUID'
      )
    ) AS is_repair_walk_candidate,

    (
      COALESCE(e.matched_overlapping_windows, 0) > 0
      OR COALESCE(e.matched_post_conflict_windows, 0) > 0
    ) AS has_negative_window_context,

    (
      COALESCE(e.matched_unknown_preexisting_activity_windows, 0) > 0
      OR COALESCE(e.eligible_window_has_unknown_preexisting_activity, FALSE) = TRUE
    ) AS has_unknown_preexisting_activity_risk,

    TIMESTAMP_TRUNC(e.occurred_at, HOUR) AS hour_bucket,
    TIMESTAMP_TRUNC(e.occurred_at, MINUTE) AS minute_bucket

  FROM current_state e
  WHERE e.occurred_at >= TIMESTAMP(${sqlString(startDate)})
    AND e.auid IS NOT NULL
),

candidate_events AS (
  SELECT *
  FROM events_base
  WHERE is_repair_walk_candidate = TRUE
),

candidate_auid_time_bounds AS (
  SELECT
    auid,

    /*
      PERFORMANCE STRATEGY 1:
      Bound the local AUID scope around repair candidates.

      The original Part 4 could pull a candidate AUID's whole history into
      events_scope. That made later joins and windows operate over far more
      rows than the repair rules could actually use.

      These bounds preserve the rule lookbacks:
        - explicit referrer: 120 minutes
        - previous parent: 30 minutes
        - near-window-start parent: 30 minutes

      Resolved events are still included as anchors, but only when they are
      close enough to candidate events to be reachable by the repair rules.
    */
    TIMESTAMP_SUB(MIN(occurred_at), INTERVAL 2 HOUR) AS min_needed_at,
    TIMESTAMP_ADD(MAX(occurred_at), INTERVAL 30 MINUTE) AS max_needed_at

  FROM candidate_events
  GROUP BY auid
),

events_scope AS (
  SELECT e.*
  FROM events_base e
  JOIN candidate_auid_time_bounds b
    ON e.auid = b.auid
   AND e.occurred_at >= b.min_needed_at
   AND e.occurred_at <= b.max_needed_at
  WHERE e.is_repair_walk_candidate = TRUE
     OR e.is_navigable_parent = TRUE
     OR e.has_current_identity = TRUE
     OR (
       e.event_type = 'web_request'
       AND e.request_referer_path_and_query IS NOT NULL
     )
),

children_to_repair AS (
  SELECT *
  FROM events_scope
  WHERE is_repair_walk_candidate = TRUE
),

/* --------------------------------------------------------------------------
   Parent lookup tables

   PERFORMANCE STRATEGY 2:
   Use pre-filtered parent tables so each rule joins against the smallest
   relevant parent population.

   This does not change rule logic. It only prevents each rule from repeatedly
   joining to a broader parent_lookup than it needs.
-------------------------------------------------------------------------- */

parent_lookup AS (
  SELECT
    request_uuid,
    occurred_at,
    request_path,
    request_path_and_query,
    auid,
    current_iuid,
    has_current_identity,
    is_navigable_parent,
    hour_bucket,
    minute_bucket
  FROM events_scope
  WHERE is_navigable_parent = TRUE
     OR has_current_identity = TRUE
),

referrer_parent_lookup AS (
  /*
    Rule 1 needs parents that can be matched by explicit referrer path/query.
  */
  SELECT *
  FROM parent_lookup
  WHERE request_path_and_query IS NOT NULL
),

resolved_parent_lookup AS (
  /*
    Used by resolved fallback candidates and Rule 3.
  */
  SELECT *
  FROM parent_lookup
  WHERE has_current_identity = TRUE
),

previous_parent_lookup AS (
  /*
    Rule 2 needs previous navigable or resolved parent rows.
    This currently matches parent_lookup, but is kept separate so future
    rule-specific filtering can be added without touching Rule 1 or Rule 3.
  */
  SELECT *
  FROM parent_lookup
),

/* --------------------------------------------------------------------------
   Rule 1: explicit referrer to resolved or unresolved same-AUID parent

   Bounded multi-candidate version.

   For each child, keep:
     1. closest valid explicit-referrer parent
     2. closest resolved explicit-referrer parent

   PERFORMANCE STRATEGY 3A:
   Use hour buckets as equality join keys for the 120-minute referrer lookback.

   The exact timestamp filters remain for correctness. The hour bucket only
   narrows the join candidate set before the range filter is applied.
-------------------------------------------------------------------------- */

referrer_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE child.request_referer_path_and_query IS NOT NULL
    AND NOT (
      ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
      AND ${sqlNotInList("child.request_path", signInPagePaths)}
    )
),

referrer_child_buckets AS (
  SELECT
    child.*,
    parent_hour_bucket
  FROM referrer_children child
  CROSS JOIN UNNEST([
    child.hour_bucket,
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 1 HOUR),
    TIMESTAMP_SUB(child.hour_bucket, INTERVAL 2 HOUR)
  ]) AS parent_hour_bucket
),

repair_referrer_closest_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    CASE
      WHEN parent.has_current_identity = TRUE THEN 'HIGH'
      ELSE 'MEDIUM'
    END AS match_confidence,

    CASE
      WHEN parent.has_current_identity = TRUE
        THEN 'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
      ELSE 'PART_4_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
    END AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM referrer_child_buckets child
  JOIN referrer_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND parent.request_uuid != child.request_uuid

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY
      parent.occurred_at DESC,
      parent.has_current_identity DESC,
      parent.request_uuid
  ) = 1
),

repair_referrer_closest_resolved_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    'HIGH' AS match_confidence,
    'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID' AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM referrer_child_buckets child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND parent.request_uuid != child.request_uuid

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY
      parent.occurred_at DESC,
      parent.request_uuid
  ) = 1
),

repair_referrer_candidates AS (
  SELECT * FROM repair_referrer_closest_parent
  UNION DISTINCT
  SELECT * FROM repair_referrer_closest_resolved_parent
),

/* --------------------------------------------------------------------------
   Rule 2: short-gap previous parent for null/home referrer

   Bounded multi-candidate version.

   For each child, keep:
     1. closest valid previous parent
     2. closest resolved previous parent

   PERFORMANCE STRATEGY 3B:
   Use generated minute buckets as equality join keys for the 30-minute
   previous-parent lookback.

   The exact timestamp filters remain for correctness. The generated minute
   buckets prevent BigQuery from joining every child to all same-AUID parent
   rows before applying the time range.
-------------------------------------------------------------------------- */

home_or_null_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE (
      child.request_referer_path_and_query IS NULL
      OR ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
    )
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
),

home_or_null_child_parent_minutes AS (
  SELECT
    child.*,
    parent_minute_bucket
  FROM home_or_null_children child
  CROSS JOIN UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE), MINUTE),
      TIMESTAMP_TRUNC(child.occurred_at, MINUTE),
      INTERVAL 1 MINUTE
    )
  ) AS parent_minute_bucket
),

repair_home_or_null_closest_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    CASE
      WHEN parent.current_iuid IS NOT NULL THEN 'MEDIUM'
      ELSE 'LOW'
    END AS match_confidence,

    CASE
      WHEN child.request_referer_path_and_query IS NULL
        AND parent.current_iuid IS NOT NULL
        THEN 'PART_4_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      WHEN child.request_referer_path_and_query IS NULL
        THEN 'PART_4_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'

      WHEN parent.current_iuid IS NOT NULL
        THEN 'PART_4_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      ELSE 'PART_4_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
    END AS match_source,

    TRUE AS is_weak_previous_parent_rule

  FROM home_or_null_child_parent_minutes child
  JOIN previous_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND parent.request_uuid != child.request_uuid

  WHERE ${sqlNotInList("parent.request_path", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY
      parent.occurred_at DESC,
      parent.request_uuid DESC
  ) = 1
),

repair_home_or_null_closest_resolved_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    'MEDIUM' AS match_confidence,

    CASE
      WHEN child.request_referer_path_and_query IS NULL
        THEN 'PART_4_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
      ELSE 'PART_4_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
    END AS match_source,

    TRUE AS is_weak_previous_parent_rule

  FROM home_or_null_child_parent_minutes child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND parent.request_uuid != child.request_uuid

  WHERE ${sqlNotInList("parent.request_path", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY
      parent.occurred_at DESC,
      parent.request_uuid DESC
  ) = 1
),

repair_home_or_null_prev_parent AS (
  SELECT * FROM repair_home_or_null_closest_parent
  UNION DISTINCT
  SELECT * FROM repair_home_or_null_closest_resolved_parent
),

/* --------------------------------------------------------------------------
   Rule 3: near-window-start previous resolved parent

   Already resolved-only, so keep one closest rule-valid resolved parent.

   PERFORMANCE STRATEGY 3C:
   Reuse minute-bucket equality joins for this 30-minute resolved-parent search.
-------------------------------------------------------------------------- */

near_window_start_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE child.eligible_window_start IS NOT NULL
    AND child.occurred_at >= child.eligible_window_start
    AND child.occurred_at <= TIMESTAMP_ADD(child.eligible_window_start, INTERVAL 30 MINUTE)
    AND ${sqlNotInList("child.request_path", preAuthPagePaths)}
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
),

near_window_start_child_parent_minutes AS (
  SELECT
    child.*,
    parent_minute_bucket
  FROM near_window_start_children child
  CROSS JOIN UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE), MINUTE),
      TIMESTAMP_TRUNC(child.occurred_at, MINUTE),
      INTERVAL 1 MINUTE
    )
  ) AS parent_minute_bucket
),

repair_near_window_start_resolved_parent AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,

    'MEDIUM' AS match_confidence,
    'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID' AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM near_window_start_child_parent_minutes child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND parent.request_uuid != child.request_uuid

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid
    ORDER BY
      parent.occurred_at DESC,
      parent.request_uuid
  ) = 1
),

/* --------------------------------------------------------------------------
   Choose the best rule-derived parent per child.
-------------------------------------------------------------------------- */

parent_candidates AS (
  SELECT * FROM repair_referrer_candidates
  UNION ALL
  SELECT * FROM repair_home_or_null_prev_parent
  UNION ALL
  SELECT * FROM repair_near_window_start_resolved_parent
),

best_parent AS (
  SELECT
    child_request_uuid,
    best.parent_request_uuid,
    best.match_confidence,
    best.match_source,
    best.is_weak_previous_parent_rule
  FROM (
    SELECT
      child_request_uuid,
      ARRAY_AGG(
        STRUCT(
          parent_request_uuid,
          match_confidence,
          match_source,
          parent_at,
          is_weak_previous_parent_rule
        )
        ORDER BY
          CASE match_confidence
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            WHEN 'LOW' THEN 1
            ELSE 0
          END DESC,
          parent_at DESC,
          parent_request_uuid
        LIMIT 1
      )[OFFSET(0)] AS best
    FROM parent_candidates
    GROUP BY child_request_uuid
  )
),

events_with_parent AS (
  SELECT
    e.*,

    CASE
      WHEN e.has_current_identity = TRUE THEN NULL
      ELSE bp.parent_request_uuid
    END AS parent_request_uuid_repair,

    bp.match_confidence AS parent_match_confidence_repair,
    bp.match_source AS parent_match_source_repair,
    bp.is_weak_previous_parent_rule AS parent_is_weak_previous_parent_rule_repair

  FROM events_scope e
  LEFT JOIN best_parent bp
    ON e.request_uuid = bp.child_request_uuid
),

repair_walk AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.request_uuid AS current_request_uuid,
    e.occurred_at AS current_occurred_at,
    e.parent_request_uuid_repair AS parent_request_uuid,
    e.has_current_identity,
    e.current_iuid,
    0 AS depth,

    [e.request_uuid] AS visited_request_uuids,

    IF(e.has_current_identity, e.current_iuid, NULL) AS nearest_current_iuid

  FROM events_with_parent e
  WHERE e.is_repair_walk_candidate = TRUE

  UNION ALL

  SELECT
    w.start_request_uuid,
    p.request_uuid AS current_request_uuid,
    p.occurred_at AS current_occurred_at,
    p.parent_request_uuid_repair AS parent_request_uuid,
    p.has_current_identity,
    p.current_iuid,
    w.depth + 1 AS depth,

    ARRAY_CONCAT(w.visited_request_uuids, [p.request_uuid])
      AS visited_request_uuids,

    COALESCE(
      w.nearest_current_iuid,
      IF(p.has_current_identity, p.current_iuid, NULL)
    ) AS nearest_current_iuid

  FROM repair_walk w
  JOIN events_with_parent p
    ON w.parent_request_uuid = p.request_uuid
  WHERE w.parent_request_uuid IS NOT NULL
    AND w.parent_request_uuid != w.current_request_uuid
    AND w.nearest_current_iuid IS NULL
    AND w.depth < 25
    AND NOT p.request_uuid IN UNNEST(w.visited_request_uuids)
),

collapsed_repair_walk AS (
  SELECT
    start_request_uuid AS request_uuid,

    ARRAY_AGG(
      STRUCT(
        current_request_uuid,
        current_occurred_at,
        depth,
        nearest_current_iuid
      )
      ORDER BY
        IF(nearest_current_iuid IS NOT NULL, 0, 1),
        depth ASC
      LIMIT 1
    )[OFFSET(0)] AS best_repair

  FROM repair_walk
  GROUP BY start_request_uuid
),

repair_assignments AS (
  SELECT
    c.request_uuid,
    c.best_repair.current_request_uuid AS repair_source_request_uuid,
    c.best_repair.current_occurred_at AS repair_source_occurred_at,
    c.best_repair.depth AS repair_depth,
    c.best_repair.nearest_current_iuid AS repaired_iuid
  FROM collapsed_repair_walk c
  WHERE c.best_repair.nearest_current_iuid IS NOT NULL
),

repair_walk_proposals AS (
  SELECT
    e.request_uuid,

    r.repaired_iuid AS proposed_iuid,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'PART_4_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_REFERRER_CHAIN_TO_RESOLVED_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'PART_4_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M',
          'PART_4_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'PART_4_PREVIOUS_RESOLVED_PARENT_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'PART_4_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M',
          'PART_4_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'PART_4_PREVIOUS_PARENT_CHAIN_TO_RESOLVED_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'
        THEN 'PART_4_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        THEN 'PART_4_REPAIR_WALK_TO_NEAREST_CURRENT_IDENTITY'

      ELSE 'UNRESOLVED_AFTER_PART_4_REPAIR_WALK'
    END AS proposed_iuid_method,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        THEN 'PART_4_SECOND_RECURSIVE_WALK'
      ELSE NULL
    END AS proposed_resolution_stage,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_confidence_repair = 'HIGH'
        THEN 55

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_confidence_repair = 'MEDIUM'
        THEN 50

      WHEN r.repaired_iuid IS NOT NULL
        THEN 45

      ELSE 0
    END AS proposed_identity_resolution_priority,

    r.repair_source_request_uuid,
    r.repair_source_occurred_at,
    r.repair_depth,

    p.parent_request_uuid_repair,
    p.parent_match_confidence_repair,
    p.parent_match_source_repair,
    p.parent_is_weak_previous_parent_rule_repair,

    (
      r.repaired_iuid IS NOT NULL

      AND (
        e.eligible_window_iuid IS NULL
        OR e.eligible_window_iuid = r.repaired_iuid
      )

      AND (
        e.auid_risk_classification != 'HIGH_RISK_MULTI_IUID_AUID'
        OR (
          COALESCE(p.parent_is_weak_previous_parent_rule_repair, FALSE) = FALSE
          OR e.has_unknown_preexisting_activity_risk = FALSE
        )
      )

      AND NOT EXISTS (
        SELECT 1
        FROM events_base s
        WHERE s.auid = e.auid
          AND ${sqlInList("s.request_path", signOutPaths)}
          AND s.current_iuid = r.repaired_iuid
          AND s.occurred_at > LEAST(e.occurred_at, r.repair_source_occurred_at)
          AND s.occurred_at < GREATEST(e.occurred_at, r.repair_source_occurred_at)
      )
    ) AS should_apply_proposed_iuid,

    CASE
      WHEN r.repaired_iuid IS NULL
        THEN 'NO_REPAIRED_IUID_FOUND'

      WHEN e.eligible_window_iuid IS NOT NULL
        AND e.eligible_window_iuid != r.repaired_iuid
        THEN 'BLOCKED_PROPOSED_IUID_CONFLICTS_WITH_ELIGIBLE_WINDOW_IUID'

      WHEN e.auid_risk_classification = 'HIGH_RISK_MULTI_IUID_AUID'
        AND COALESCE(p.parent_is_weak_previous_parent_rule_repair, FALSE) = TRUE
        AND e.has_unknown_preexisting_activity_risk = TRUE
        THEN 'BLOCKED_HIGH_RISK_WEAK_PARENT_WITH_UNKNOWN_PREEXISTING_ACTIVITY'

      WHEN EXISTS (
        SELECT 1
        FROM events_base s
        WHERE s.auid = e.auid
          AND ${sqlInList("s.request_path", signOutPaths)}
          AND s.current_iuid = r.repaired_iuid
          AND s.occurred_at > LEAST(e.occurred_at, r.repair_source_occurred_at)
          AND s.occurred_at < GREATEST(e.occurred_at, r.repair_source_occurred_at)
      )
        THEN 'BLOCKED_SIGNOUT_BETWEEN_EVENT_AND_REPAIR_SOURCE'

      ELSE 'ELIGIBLE_REPAIR_WALK_ASSIGNMENT'
    END AS repair_walk_gate_reason,

    CURRENT_TIMESTAMP() AS proposed_assignment_created_at

  FROM events_scope e
  LEFT JOIN events_with_parent p
    ON e.request_uuid = p.request_uuid
  LEFT JOIN repair_assignments r
    ON e.request_uuid = r.request_uuid
  WHERE e.is_repair_walk_candidate = TRUE
),

deduplicated_repair_walk_proposals AS (
  SELECT
    request_uuid,

    ARRAY_AGG(
      STRUCT(
        proposed_iuid,
        proposed_iuid_method,
        proposed_resolution_stage,
        proposed_identity_resolution_priority,
        repair_source_request_uuid,
        repair_source_occurred_at,
        repair_depth,
        parent_request_uuid_repair,
        parent_match_confidence_repair,
        parent_match_source_repair,
        parent_is_weak_previous_parent_rule_repair,
        repair_walk_gate_reason,
        proposed_assignment_created_at
      )
      ORDER BY
        proposed_identity_resolution_priority DESC,
        repair_depth ASC,
        proposed_assignment_created_at ASC,
        proposed_iuid
      LIMIT 1
    )[OFFSET(0)] AS best_proposal

  FROM repair_walk_proposals
  WHERE should_apply_proposed_iuid = TRUE
    AND proposed_iuid IS NOT NULL
  GROUP BY request_uuid
),

updated_current_state AS (
  SELECT
    c.* EXCEPT (
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
      current_iteration,
      is_currently_resolved,
      identity_resolution_priority
    ),

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_iuid
      ELSE c.current_iuid
    END AS current_iuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_iuid_method
      ELSE c.current_iuid_method
    END AS current_iuid_method,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_resolution_stage
      ELSE c.current_resolution_stage
    END AS current_resolution_stage,

    4 AS current_iteration,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN TRUE
      ELSE c.is_currently_resolved
    END AS is_currently_resolved,

    CASE
      WHEN c.identity_resolution_locked = TRUE
        THEN 100

      WHEN c.current_iuid IS NOT NULL
        THEN c.identity_resolution_priority

      WHEN p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_identity_resolution_priority

      ELSE c.identity_resolution_priority
    END AS identity_resolution_priority,

    p.best_proposal.proposed_assignment_created_at AS repair_walk_assignment_created_at,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN TRUE
      ELSE FALSE
    END AS repair_walk_assignment_applied,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_source_request_uuid
      ELSE NULL
    END AS repair_walk_source_request_uuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_source_occurred_at
      ELSE NULL
    END AS repair_walk_source_occurred_at,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_depth
      ELSE NULL
    END AS repair_walk_depth,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_request_uuid_repair
      ELSE NULL
    END AS repair_walk_parent_request_uuid,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_match_confidence_repair
      ELSE NULL
    END AS repair_walk_parent_match_confidence,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_match_source_repair
      ELSE NULL
    END AS repair_walk_parent_match_source,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.parent_is_weak_previous_parent_rule_repair
      ELSE NULL
    END AS repair_walk_parent_is_weak_previous_parent_rule,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.repair_walk_gate_reason
      ELSE NULL
    END AS repair_walk_gate_reason,

    CASE
      WHEN c.current_iuid IS NULL
        AND p.best_proposal.proposed_iuid IS NOT NULL
        THEN CURRENT_TIMESTAMP()
      ELSE NULL
    END AS repair_walk_assignment_applied_at

  FROM current_state c
  LEFT JOIN deduplicated_repair_walk_proposals p
    ON c.request_uuid = p.request_uuid
)

SELECT *
FROM updated_current_state

`);

  /* --------------------------------------------------------------------------
     10. Component 5: resolve admin identities
  -------------------------------------------------------------------------- */

publish(resolvedAdminName, stagingConfig("Flag and normalise admin identities for final identity-solved events")).query(ctx => `

WITH

current_state AS (
  SELECT *
  FROM ${ctx.ref(secondWalkName)}
),

admin_page_events AS (
  SELECT *
  FROM current_state
  WHERE request_method = 'GET'
    AND COALESCE(SAFE_CAST(response_status AS STRING), 'X')
      NOT IN ('301','302','303','307','308')
    AND ${sqlRegexpContainsAny("request_path", adminPagePatterns)}
),

admin_iuids AS (
  SELECT DISTINCT current_iuid AS iuid
  FROM admin_page_events
  WHERE current_iuid IS NOT NULL

  UNION DISTINCT

  SELECT DISTINCT CAST(request_user_id AS STRING) AS iuid
  FROM admin_page_events
  WHERE request_user_id IS NOT NULL
),

admin_group AS (
  SELECT
    CONCAT(
      'ADMIN_GROUP:',
      TO_HEX(SHA256(STRING_AGG(iuid, '|' ORDER BY iuid)))
    ) AS admin_group_id
  FROM admin_iuids
),

final AS (
  SELECT
    e.*,

    e.current_iuid IN (
      SELECT iuid FROM admin_iuids
    ) AS current_iuid_is_admin_identity,

    CASE
      WHEN e.current_iuid IN (SELECT iuid FROM admin_iuids)
        THEN g.admin_group_id
      ELSE e.current_iuid
    END AS admin_normalised_iuid

  FROM current_state e
  CROSS JOIN admin_group g
)

SELECT *
FROM final

`);
};