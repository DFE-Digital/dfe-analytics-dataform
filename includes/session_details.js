const parameter_functions = require("./parameter_functions");

module.exports = params => {
  if (!params.enableSessionDetailsTable) {
    return true;
  }

  const stagingSchema = params.stagingDataset || "web_analytics_staging_tables";
  const eventsTableName = `events_${params.eventSourceName}`;
  const pass1Name = `identity_propagation_pass_1_${params.eventSourceName}`;
  const pass2Name = `user_activity_windows_${params.eventSourceName}`;
  const pass3Name = `identity_propagation_master_${params.eventSourceName}`;
  const finalName = `session_details_v2_${params.eventSourceName}`;

  function requestToMedium(ctx) {
    return `
    CASE
        WHEN REGEXP_CONTAINS(request_referer_domain, "${params.urlRegex}") THEN NULL
        ${ctx.when(params.attributionParameters.includes("utm_medium"), `WHEN utm_medium = "cpc" THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("gclid"), `WHEN gclid IS NOT NULL THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("dclid"), `WHEN dclid IS NOT NULL THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("msclkid"), `WHEN msclkid IS NOT NULL THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("gbraid"), `WHEN gbraid IS NOT NULL THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("fbclid"), `WHEN fbclid IS NOT NULL THEN "PPC"`, ``)}
        ${ctx.when(params.attributionParameters.includes("utm_medium"), `WHEN REGEXP_CONTAINS(utm_medium, "(?i)(email)") THEN "Email"`, ``)}
        WHEN REGEXP_CONTAINS(request_referer_domain, "${params.socialRefererDomainRegex}") THEN "Social"
        ${ctx.when(params.attributionParameters.includes("utm_medium"), `WHEN REGEXP_CONTAINS(utm_medium, "(?i)(social)") THEN "Social"`, ``)}
        WHEN REGEXP_CONTAINS(request_referer_domain, "${params.searchEngineRefererDomainRegex}") THEN "Organic"
        ${ctx.when(params.attributionParameters.includes("utm_medium"), `WHEN REGEXP_CONTAINS(utm_medium, "(?i)(organic)") THEN "Organic"`, ``)}
        WHEN REGEXP_CONTAINS(request_referer_domain, "${params.attributionDomainExclusionRegex}") THEN "Direct or unknown"
        WHEN request_referer_domain IS NOT NULL THEN "Referral"
        ${ctx.when(params.attributionParameters.includes("utm_medium"), `WHEN REGEXP_CONTAINS(utm_medium, "(?i)(referral)") THEN "Referral"`, ``)}
        ELSE "Direct or unknown"
    END
    `;
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

  publish(
    pass1Name,
    stagingConfig("Pass 1: high-confidence chaining and conservative user identity propagation.")
  ).query(
    ctx => `
WITH RECURSIVE

anchors AS (
  SELECT DISTINCT
    e.request_uuid,
    d.value[SAFE_OFFSET(0)] AS inferred_user_id,
    e.anonymised_user_agent_and_ip AS auid,
    TRUE AS is_anchor
  FROM ${ctx.ref(eventsTableName)} e
  JOIN UNNEST(e.data) AS d
  WHERE e.entity_table_name = 'users'
    AND e.request_path = '/auth/dfe/callback'
    AND e.request_method = 'GET'
    AND d.key = 'id'
),

auid_seen_in_anchor AS (
  SELECT DISTINCT auid, TRUE AS auid_has_anchor
  FROM anchors
),

auid_group_confident_map AS (
  SELECT
    auid,
    MIN(inferred_user_id) AS auid_group_confident,
    COUNT(DISTINCT inferred_user_id) AS auid_distinct_iuid_count
  FROM anchors
  GROUP BY auid
),

events_supp AS (
  SELECT
    e.*,
    IFNULL(a.is_anchor, FALSE) AS is_anchor,
    a.inferred_user_id,
    c.auid_group_confident,
    c.auid_distinct_iuid_count,
    IFNULL(sa.auid_has_anchor, FALSE) AS auid_has_anchor,
    (
      e.request_method = 'GET'
      AND SAFE_CAST(e.response_status AS STRING) NOT IN ('301','302','303','307','308')
    ) AS is_navigable_parent
  FROM ${ctx.ref(eventsTableName)} e
  LEFT JOIN anchors a
    ON e.request_uuid = a.request_uuid
  LEFT JOIN auid_group_confident_map c
    ON e.anonymised_user_agent_and_ip = c.auid
  LEFT JOIN auid_seen_in_anchor sa
    ON e.anonymised_user_agent_and_ip = sa.auid
  WHERE e.event_type = 'web_request'
),

base_seq AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY e.anonymised_user_agent_and_ip
      ORDER BY e.occurred_at, e.request_uuid
    ) AS seq
  FROM events_supp e
  WHERE e.anonymised_user_agent_and_ip IS NOT NULL
),

base_seq_with_prev AS (
  SELECT
    b.*,
    LAG(b.request_uuid) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
    ) AS prev_request_uuid_auid,
    LAG(b.occurred_at) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
    ) AS prev_occurred_at_auid,
    LAG(b.request_path) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
    ) AS prev_request_path_auid,
    LAG(b.request_method) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
    ) AS prev_request_method_auid,
    MAX(
      IF((b.is_navigable_parent OR b.is_anchor), b.seq, NULL)
    ) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_parent_seq_auid
  FROM base_seq b
),

prev_by_auid AS (
  SELECT
    c.*,
    p.request_uuid AS prev_parent_request_uuid_auid,
    p.occurred_at  AS prev_parent_occurred_at_auid,
    p.request_path AS prev_parent_request_path_auid
  FROM base_seq_with_prev c
  LEFT JOIN base_seq p
    ON p.anonymised_user_agent_and_ip = c.anonymised_user_agent_and_ip
   AND p.seq = c.prev_parent_seq_auid
),

p1_referrer_candidates AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,
    CASE
      WHEN child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip THEN 'high'
      WHEN child.auid_group_confident IS NOT NULL
        AND child.auid_group_confident = parent.auid_group_confident THEN 'medium'
      ELSE 'low'
    END AS match_confidence,
    CASE
      WHEN child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip THEN 'P1_referrer+auid'
      WHEN child.auid_group_confident IS NOT NULL
        AND child.auid_group_confident = parent.auid_group_confident THEN 'P1_referrer+auid_group_confident'
      ELSE 'P1_referrer+weak'
    END AS match_source
  FROM events_supp child
  JOIN events_supp parent
    ON child.request_referer_path_and_query = parent.request_path_and_query
   AND (parent.is_navigable_parent OR parent.is_anchor)
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND (
     child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip
     OR (
       child.auid_group_confident IS NOT NULL
       AND child.auid_group_confident = parent.auid_group_confident
     )
   )
  WHERE NOT (
    child.request_referer_path_and_query IN ('/', '/?')
    AND child.request_path != '/sign-in'
  )
),

p1_anchor_next_callback AS (
  SELECT
    a.request_uuid AS anchor_request_uuid,
    a.anonymised_user_agent_and_ip AS auid,
    a.occurred_at AS anchor_at,
    (SELECT MIN(a2.occurred_at)
     FROM events_supp a2
     WHERE a2.request_path = '/auth/dfe/callback'
       AND a2.is_anchor = TRUE
       AND a2.request_method = 'GET'
       AND a2.anonymised_user_agent_and_ip = a.anonymised_user_agent_and_ip
       AND a2.occurred_at > a.occurred_at
    ) AS next_callback_at
  FROM events_supp a
  WHERE a.is_anchor = TRUE
    AND a.request_path = '/auth/dfe/callback'
    AND a.request_method = 'GET'
    AND a.inferred_user_id IS NOT NULL
    AND a.anonymised_user_agent_and_ip IS NOT NULL
),

p1_bootstrap_first_child_after_callback AS (
  SELECT
    anc.anchor_request_uuid,
    anc.auid,
    anc.anchor_at,
    anc.next_callback_at,
    child.request_uuid AS child_request_uuid,
    child.occurred_at  AS child_at
  FROM p1_anchor_next_callback anc
  JOIN events_supp child
    ON child.anonymised_user_agent_and_ip = anc.auid
   AND child.occurred_at > anc.anchor_at
   AND child.occurred_at <= TIMESTAMP_ADD(anc.anchor_at, INTERVAL 30 MINUTE)
   AND (anc.next_callback_at IS NULL OR child.occurred_at < anc.next_callback_at)
   AND child.is_anchor = FALSE
   AND (child.request_referer_path_and_query IS NULL OR child.request_referer_path_and_query IN ('/', '/?'))
   AND child.is_navigable_parent = TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY anc.anchor_request_uuid
    ORDER BY child.occurred_at ASC, child.request_uuid ASC
  ) = 1
),

p1_bootstrap_candidates AS (
  SELECT
    child_request_uuid AS child_request_uuid,
    anchor_request_uuid AS parent_request_uuid,
    child_at AS child_at,
    anchor_at AS parent_at,
    'medium' AS match_confidence,
    'P1_bootstrap_first_event_after_callback_same_auid_30m' AS match_source
  FROM p1_bootstrap_first_child_after_callback
),

p1_slash_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'low' AS match_confidence,
    'P1_referrer_slash+prev_parent_by_auid_10m_single_user_only' AS match_source
  FROM prev_by_auid child
  WHERE child.request_referer_path_and_query IN ('/', '/?')
    AND child.request_path != '/sign-in'
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND child.prev_parent_request_path_auid NOT IN ('/', '/?')
    AND child.auid_distinct_iuid_count = 1
),

p1_null_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'medium' AS match_confidence,
    'P1_null_referrer+prev_parent_by_auid_single_user_10m' AS match_source
  FROM prev_by_auid child
  WHERE child.request_referer_path_and_query IS NULL
    AND child.auid_distinct_iuid_count = 1
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND child.prev_parent_request_path_auid NOT IN ('/', '/sign-in')
),

p1_parent_candidates AS (
  SELECT * FROM p1_referrer_candidates
  UNION ALL SELECT * FROM p1_bootstrap_candidates
  UNION ALL SELECT * FROM p1_slash_prev_by_auid_single_user
  UNION ALL SELECT * FROM p1_null_prev_by_auid_single_user
),

p1_best_parent AS (
  SELECT
    child_request_uuid,
    parent_request_uuid,
    match_confidence,
    match_source
  FROM p1_parent_candidates
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child_request_uuid
    ORDER BY
      CASE match_confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC,
      parent_at DESC
  ) = 1
),

events_with_parent_pass1 AS (
  SELECT
    e.*,
    CASE WHEN e.is_anchor THEN NULL ELSE bp.parent_request_uuid END AS parent_request_uuid_pass1,
    bp.match_confidence AS parent_match_confidence_pass1,
    bp.match_source     AS parent_match_source_pass1
  FROM events_supp e
  LEFT JOIN p1_best_parent bp
    ON e.request_uuid = bp.child_request_uuid
),

walk_pass1 AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.request_uuid AS current_request_uuid,
    e.parent_request_uuid_pass1 AS parent_request_uuid,
    e.is_anchor,
    e.inferred_user_id,
    0 AS depth,
    IF(e.is_anchor, e.inferred_user_id, NULL) AS nearest_anchor_user_id
  FROM events_with_parent_pass1 e

  UNION ALL

  SELECT
    w.start_request_uuid,
    p.request_uuid AS current_request_uuid,
    p.parent_request_uuid_pass1 AS parent_request_uuid,
    p.is_anchor,
    p.inferred_user_id,
    w.depth + 1 AS depth,
    COALESCE(w.nearest_anchor_user_id, IF(p.is_anchor, p.inferred_user_id, NULL)) AS nearest_anchor_user_id
  FROM walk_pass1 w
  JOIN events_with_parent_pass1 p
    ON w.parent_request_uuid = p.request_uuid
  WHERE w.parent_request_uuid IS NOT NULL
    AND w.depth < 400
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

likely_shunt_arrival_candidates AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.anonymised_user_agent_and_ip AS child_auid,
    child.occurred_at AS child_at,
    parent.request_uuid AS shunt_parent_request_uuid,
    parent.anonymised_user_agent_and_ip AS shunt_parent_auid,
    parent.occurred_at AS shunt_parent_at,
    COUNT(*) OVER (PARTITION BY child.request_uuid) AS candidate_count_last_3h
  FROM events_supp child
  JOIN events_supp parent
    ON parent.request_path_and_query = child.request_referer_path_and_query
   AND (parent.is_navigable_parent OR parent.is_anchor)
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 3 HOUR)
  WHERE child.request_referer_path_and_query IS NOT NULL
    AND child.request_referer_path_and_query NOT IN ('/', '/?')
    AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 2 MINUTE)
    AND parent.request_path NOT IN ('/', '/sign-in')
    AND NOT STARTS_WITH(parent.request_path, '/auth/')
    AND child.anonymised_user_agent_and_ip IS NOT NULL
    AND parent.anonymised_user_agent_and_ip IS NOT NULL
    AND child.anonymised_user_agent_and_ip != parent.anonymised_user_agent_and_ip
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
)

SELECT
  e.*,
  c.chain_id_pass1,
  c.propagated_user_id_pass1,
  IFNULL(s.likely_shunt_arrival, FALSE) AS likely_shunt_arrival,
  s.shunt_parent_request_uuid,
  s.shunt_parent_auid,
  s.shunt_parent_at
FROM events_with_parent_pass1 e
LEFT JOIN collapsed_pass1 c
  ON e.request_uuid = c.request_uuid
LEFT JOIN likely_shunt_arrivals s
  ON e.request_uuid = s.child_request_uuid`
  );

  publish(
    pass2Name,
    stagingConfig("Pass 2: user activity windows from pass 1 anchors and sign-outs.", {
      dependencies: [...(params.dependencies || []), pass1Name]
    })
  ).query(
    ctx => `
WITH

params AS (
  SELECT
    '/auth/dfe/callback' AS callback_path,
    '/auth/dfe/sign-out' AS signout_path,
    '/auth/' AS auth_prefix,
    ['/sign-in', '/sign-out', '/'] AS explicit_excluded_paths,
    ['301','302','303','307','308'] AS redirect_statuses,
    10 AS pre_window_minutes,
    10 AS unknown_presence_minutes
),

pass1_anchors AS (
  SELECT
    p.anonymised_user_agent_and_ip AS auid,
    p.inferred_user_id AS iuid,
    p.occurred_at AS window_start,
    p.request_uuid AS anchor_request_uuid,
    p.auid_distinct_iuid_count
  FROM ${ctx.ref(pass1Name)} p
  CROSS JOIN params prm
  WHERE p.is_anchor = TRUE
    AND p.request_path = prm.callback_path
    AND p.inferred_user_id IS NOT NULL
),

pass1_signouts_attributed AS (
  SELECT
    p.anonymised_user_agent_and_ip AS auid,
    p.propagated_user_id_pass1 AS iuid,
    TIMESTAMP_ADD(p.occurred_at, INTERVAL 1 MICROSECOND) AS signout_at,
    p.request_uuid AS signout_request_uuid
  FROM ${ctx.ref(pass1Name)} p
  CROSS JOIN params prm
  WHERE p.request_path = prm.signout_path
    AND p.propagated_user_id_pass1 IS NOT NULL
),

anchor_boundaries AS (
  SELECT
    a.auid,
    a.iuid,
    a.window_start,
    a.anchor_request_uuid,
    a.auid_distinct_iuid_count,

    (SELECT MIN(a2.window_start)
     FROM pass1_anchors a2
     WHERE a2.auid = a.auid
       AND a2.window_start > a.window_start
       AND a2.iuid != a.iuid) AS next_diff_signin_at,

    (SELECT MIN(e.occurred_at)
     FROM ${ctx.ref(pass1Name)} e
     WHERE e.anonymised_user_agent_and_ip = a.auid
       AND e.occurred_at > a.window_start
       AND e.likely_shunt_arrival = TRUE
       AND (
          e.parent_request_uuid_pass1 IS NULL
          OR e.parent_match_confidence_pass1 NOT IN ('high','medium')
        )
    ) AS next_shunt_arrival_at,

    (SELECT MIN(s.signout_at)
     FROM pass1_signouts_attributed s
     WHERE s.auid = a.auid
       AND s.iuid = a.iuid
       AND s.signout_at > a.window_start) AS first_attributed_signout_at,

    (
      SELECT
        COUNT(*) > 0
        AND COUNTIF(e.propagated_user_id_pass1 IS NOT NULL) = 0
      FROM ${ctx.ref(pass1Name)} e
      CROSS JOIN params prm
      WHERE e.anonymised_user_agent_and_ip = a.auid
        AND e.occurred_at < a.window_start
        AND e.occurred_at >= TIMESTAMP_SUB(a.window_start, INTERVAL prm.pre_window_minutes MINUTE)
        AND e.event_type = 'web_request'
        AND e.request_method = 'GET'
        AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X') NOT IN UNNEST(prm.redirect_statuses)
        AND NOT STARTS_WITH(e.request_path, prm.auth_prefix)
        AND e.request_path NOT IN UNNEST(prm.explicit_excluded_paths)
    ) AS preexisting_unattributable_activity_10m,

    TIMESTAMP_ADD(a.window_start, INTERVAL 24 HOUR) AS max_cap_at
  FROM pass1_anchors a
),

anchor_last_activity AS (
  SELECT
    b.*,
    IFNULL((
      SELECT MAX(e.occurred_at)
      FROM ${ctx.ref(pass1Name)} e
      WHERE e.anonymised_user_agent_and_ip = b.auid
        AND e.occurred_at >= b.window_start
        AND e.occurred_at <= LEAST(IFNULL(b.first_attributed_signout_at, b.max_cap_at), b.max_cap_at)
        AND e.propagated_user_id_pass1 = b.iuid
        AND e.parent_match_confidence_pass1 IN ('high','medium')
    ), b.window_start) AS last_high_conf_activity_at
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
    auid,
    iuid,
    window_start,
    anchor_request_uuid,
    auid_distinct_iuid_count,
    next_diff_signin_at,
    next_shunt_arrival_at,
    first_attributed_signout_at,
    last_high_conf_activity_at,
    inactivity_tail_end_at,
    max_cap_at,
    inactivity_tail_minutes,
    preexisting_unattributable_activity_10m,
    LEAST(
      IFNULL(first_attributed_signout_at, max_cap_at),
      IFNULL(inactivity_tail_end_at,      max_cap_at),
      max_cap_at
    ) AS chain_end_base,
    LEAST(
      IFNULL(first_attributed_signout_at, max_cap_at),
      max_cap_at
    ) AS hard_end,
    LEAST(
      IFNULL(next_diff_signin_at, max_cap_at),
      IF(auid_distinct_iuid_count > 1, IFNULL(next_shunt_arrival_at, max_cap_at), max_cap_at),
      IFNULL(first_attributed_signout_at, max_cap_at),
      IFNULL(inactivity_tail_end_at, max_cap_at),
      max_cap_at
    ) AS clean_end,
    CASE
      WHEN first_attributed_signout_at IS NOT NULL
        AND first_attributed_signout_at <= IFNULL(inactivity_tail_end_at, max_cap_at)
        AND first_attributed_signout_at <= max_cap_at
        THEN 'attributed_signout'
      WHEN inactivity_tail_end_at IS NOT NULL
        AND inactivity_tail_end_at < IFNULL(first_attributed_signout_at, max_cap_at)
        AND inactivity_tail_end_at < max_cap_at
        THEN 'inactivity_tail'
      ELSE 'max_24h_cap'
    END AS chain_end_reason
  FROM window_end_candidates
),

boundaries_overlap_seed AS (
  SELECT auid, window_start AS interval_timestamp FROM windows_base
  UNION DISTINCT
  SELECT auid, chain_end_base AS interval_timestamp FROM windows_base
),

atomic_intervals_overlap_seed AS (
  SELECT
    auid,
    interval_timestamp AS interval_start,
    LEAD(interval_timestamp) OVER (PARTITION BY auid ORDER BY interval_timestamp) AS interval_end
  FROM boundaries_overlap_seed
  QUALIFY interval_end IS NOT NULL AND interval_start < interval_end
),

interval_overlap_user_count AS (
  SELECT
    ai.auid,
    ai.interval_start,
    ai.interval_end,
    COUNT(DISTINCT w.iuid) AS active_iuid_count
  FROM atomic_intervals_overlap_seed ai
  JOIN windows_base w
    ON w.auid = ai.auid
   AND w.window_start < ai.interval_end
   AND w.chain_end_base > ai.interval_start
  GROUP BY ALL
),

overlap_intervals AS (
  SELECT
    auid,
    interval_start,
    interval_end,
    IF(LAG(interval_end) OVER (PARTITION BY auid ORDER BY interval_start) = interval_start, 0, 1) AS is_new_component
  FROM interval_overlap_user_count
  WHERE active_iuid_count > 1
),

overlap_components AS (
  SELECT
    auid,
    interval_start,
    interval_end,
    SUM(is_new_component) OVER (PARTITION BY auid ORDER BY interval_start) AS overlap_component_id
  FROM overlap_intervals
),

overlap_component_bounds_seed AS (
  SELECT
    auid,
    overlap_component_id,
    MIN(interval_start) AS overlap_start,
    MAX(interval_end)   AS overlap_end_seed
  FROM overlap_components
  GROUP BY ALL
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
    w.hard_end,
    w.max_cap_at,
    w.inactivity_tail_minutes
  FROM overlap_component_bounds_seed oc
  JOIN windows_base w
    ON w.auid = oc.auid
   AND w.window_start   < oc.overlap_end_seed
   AND w.chain_end_base > oc.overlap_start
),

overlap_component_bounds AS (
  SELECT
    auid,
    overlap_component_id,
    MIN(overlap_start) AS overlap_start,
    MAX(hard_end)      AS overlap_end_hard
  FROM overlap_component_windows
  GROUP BY auid, overlap_component_id
),

overlap_component_last_device_activity AS (
  SELECT
    oc.auid,
    oc.overlap_component_id,
    oc.overlap_start,
    oc.overlap_end_hard,
    MAX(e.occurred_at) AS overlap_last_device_activity_at
  FROM overlap_component_bounds oc
  JOIN ${ctx.ref(pass1Name)} e
    ON e.anonymised_user_agent_and_ip = oc.auid
   AND e.occurred_at >= oc.overlap_start
   AND e.occurred_at <  oc.overlap_end_hard
   AND e.event_type = 'web_request'
   AND e.request_method = 'GET'
   AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X')
       NOT IN ('301','302','303','307','308')
   AND NOT STARTS_WITH(e.request_path, '/auth/')
   AND e.request_path NOT IN ('/sign-in')
  GROUP BY oc.auid, oc.overlap_component_id, oc.overlap_start, oc.overlap_end_hard
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
  LEFT JOIN overlap_component_bounds oc
    ON oc.auid = w.auid
   AND w.window_start < oc.overlap_end_hard
   AND w.hard_end     > oc.overlap_start
  LEFT JOIN overlap_component_last_device_activity d
    ON d.auid = oc.auid
   AND d.overlap_component_id = oc.overlap_component_id
  GROUP BY
    w.auid, w.iuid, w.window_start, w.anchor_request_uuid, w.auid_distinct_iuid_count,
    w.next_diff_signin_at, w.next_shunt_arrival_at, w.first_attributed_signout_at,
    w.last_high_conf_activity_at, w.inactivity_tail_end_at, w.inactivity_tail_minutes,
    w.max_cap_at, w.chain_end_base, w.hard_end, w.clean_end, w.chain_end_reason,
    w.preexisting_unattributable_activity_10m
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

boundaries AS (
  SELECT auid, window_start AS interval_timestamp FROM windows_base_final
  UNION DISTINCT
  SELECT auid, chain_end    AS interval_timestamp FROM windows_base_final
  UNION DISTINCT
  SELECT auid, clean_end    AS interval_timestamp FROM windows_base_final
),

atomic_intervals AS (
  SELECT
    auid,
    interval_timestamp AS interval_start,
    LEAD(interval_timestamp) OVER (PARTITION BY auid ORDER BY interval_timestamp) AS interval_end
  FROM boundaries
  QUALIFY interval_end IS NOT NULL AND interval_start < interval_end
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
   AND w.chain_end    > ai.interval_start
  GROUP BY ai.auid, ai.interval_start, ai.interval_end
),

window_segments AS (
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
    w.first_attributed_signout_at,
    w.max_cap_at,
    CASE
      WHEN (
        c.active_iuid_count > 1
        OR (
          w.preexisting_unattributable_activity_10m = TRUE
          AND w.auid_distinct_iuid_count > 1
        )
      ) THEN 'overlapping'
      WHEN LEAST(w.chain_end, c.interval_end) <= w.clean_end THEN 'clean'
      ELSE 'post_identity_conflict'
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
   AND w.chain_end    > c.interval_start
  WHERE GREATEST(w.window_start, c.interval_start) < LEAST(w.chain_end, c.interval_end)
)

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
  first_attributed_signout_at,
  max_cap_at
FROM window_segments`
  );

publish(
  pass3Name,
  stagingConfig("Pass 3: master conservative identity propagation table.", {
    dependencies: [...(params.dependencies || []), pass1Name, pass2Name]
  })
).query(
  ctx => `
WITH RECURSIVE

events_supp AS (
  SELECT
    e.*,
    p.is_anchor,
    p.inferred_user_id,
    p.auid_group_confident,
    p.auid_distinct_iuid_count,
    p.auid_has_anchor,

    p.chain_id_pass1,
    p.propagated_user_id_pass1,
    p.parent_match_confidence_pass1,
    p.parent_match_source_pass1,

    CASE
      WHEN COALESCE(p.auid_distinct_iuid_count, 0) = 1 THEN 'PRIMARY_SINGLE_USER'
      WHEN COALESCE(p.auid_distinct_iuid_count, 0) > 1 THEN 'SECONDARY_SHARED_AUID'
      WHEN COALESCE(p.auid_distinct_iuid_count, 0) = 0 THEN 'NO_SIGNIN_AUID'
      ELSE 'UNCLASSIFIED'
    END AS ruleset_type,

    (
      e.request_method = 'GET'
      AND COALESCE(SAFE_CAST(e.response_status AS STRING), 'X')
          NOT IN ('301', '302', '303', '307', '308')
    ) AS is_navigable_parent

  FROM ${ctx.ref(eventsTableName)} e
  JOIN ${ctx.ref(pass1Name)} p
    ON e.request_uuid = p.request_uuid
  WHERE e.event_type = 'web_request'
),

admin_group_map AS (
  SELECT
    auid_group_confident,
    TRUE AS has_admin_activity,
    MIN(occurred_at) AS first_admin_at
  FROM events_supp
  WHERE auid_group_confident IS NOT NULL
    AND request_method = 'GET'
    AND CAST(response_status AS STRING) NOT IN ('301','302','303','307','308')
    AND request_path LIKE '%/admin%'
  GROUP BY auid_group_confident
),

event_window_matches AS (
  SELECT
    e.request_uuid,
    e.anonymised_user_agent_and_ip AS auid,
    e.occurred_at,

    COUNT(w.iuid) AS matched_windows_total,
    COUNT(DISTINCT w.iuid) AS matched_distinct_iuid,
    COUNTIF(w.window_quality = 'overlapping') AS matched_overlapping_quality_count,
    MAX(w.iuid) AS matched_iuid_single

  FROM events_supp e
  LEFT JOIN ${ctx.ref(pass2Name)} w
    ON w.auid = e.anonymised_user_agent_and_ip
   AND e.occurred_at >= w.window_start
   AND e.occurred_at <  w.window_end

  GROUP BY e.request_uuid, auid, e.occurred_at
),

event_window_gate AS (
  SELECT
    request_uuid,
    auid,
    occurred_at,

    CASE
      WHEN matched_windows_total = 0 THEN NULL
      WHEN matched_overlapping_quality_count > 0 THEN NULL
      WHEN matched_distinct_iuid = 1 THEN matched_iuid_single
      ELSE NULL
    END AS eligible_window_iuid,

    CASE
      WHEN matched_windows_total = 0 THEN 'no_window_match'
      WHEN matched_overlapping_quality_count > 0 THEN 'matched_overlapping_window'
      WHEN matched_distinct_iuid > 1 THEN 'multiple_iuid_windows_match'
      ELSE 'eligible_single_iuid'
    END AS window_gate_reason

  FROM event_window_matches
),

events_wg AS (
  SELECT
    e.*,
    g.eligible_window_iuid,
    g.window_gate_reason,
    MIN(w.window_start) AS eligible_window_start,
    MAX(w.window_end)   AS eligible_window_end,
    COUNT(w.iuid)       AS matched_clean_windows

  FROM events_supp e
  LEFT JOIN event_window_gate g
    ON e.request_uuid = g.request_uuid
  LEFT JOIN ${ctx.ref(pass2Name)} w
    ON w.auid = e.anonymised_user_agent_and_ip
   AND w.iuid = g.eligible_window_iuid
   AND w.window_quality = 'clean'
   AND e.occurred_at >= w.window_start
   AND e.occurred_at <  w.window_end

  GROUP BY ALL
),

clean_window_children AS (
  SELECT
    e.request_uuid,
    e.eligible_window_iuid,
    e.eligible_window_start,
    e.eligible_window_end
  FROM events_wg e
  WHERE e.eligible_window_iuid IS NOT NULL
    AND e.eligible_window_start IS NOT NULL
    AND e.window_gate_reason = 'eligible_single_iuid'
),

base_seq AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY e.anonymised_user_agent_and_ip
      ORDER BY e.occurred_at, e.request_uuid
    ) AS seq
  FROM events_wg e
  WHERE e.anonymised_user_agent_and_ip IS NOT NULL
),

base_seq_with_prev AS (
  SELECT
    b.*,
    MAX(
      IF((b.is_navigable_parent OR b.is_anchor), b.seq, NULL)
    ) OVER (
      PARTITION BY b.anonymised_user_agent_and_ip
      ORDER BY b.occurred_at, b.request_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_parent_seq_auid
  FROM base_seq b
),

prev_by_auid AS (
  SELECT
    c.*,
    p.request_uuid AS prev_parent_request_uuid_auid,
    p.occurred_at  AS prev_parent_occurred_at_auid,
    p.request_path AS prev_parent_request_path_auid
  FROM base_seq_with_prev c
  LEFT JOIN base_seq p
    ON p.anonymised_user_agent_and_ip = c.anonymised_user_agent_and_ip
   AND p.seq = c.prev_parent_seq_auid
),

bootstrap_anchor_next_callback AS (
  SELECT
    a.request_uuid AS anchor_request_uuid,
    a.anonymised_user_agent_and_ip AS auid,
    a.occurred_at AS anchor_at,
    (SELECT MIN(a2.occurred_at)
     FROM events_wg a2
     WHERE a2.request_path = '/auth/dfe/callback'
       AND a2.is_anchor = TRUE
       AND a2.request_method = 'GET'
       AND a2.anonymised_user_agent_and_ip = a.anonymised_user_agent_and_ip
       AND a2.occurred_at > a.occurred_at
    ) AS next_callback_at
  FROM events_wg a
  WHERE a.request_path = '/auth/dfe/callback'
    AND a.is_anchor = TRUE
    AND a.request_method = 'GET'
    AND a.inferred_user_id IS NOT NULL
    AND a.anonymised_user_agent_and_ip IS NOT NULL
),

bootstrap_first_child_after_callback AS (
  SELECT
    anc.anchor_request_uuid,
    anc.auid,
    anc.anchor_at,
    anc.next_callback_at,
    child.request_uuid AS child_request_uuid,
    child.occurred_at  AS child_at
  FROM bootstrap_anchor_next_callback anc
  JOIN events_wg child
    ON child.anonymised_user_agent_and_ip = anc.auid
   AND child.occurred_at > anc.anchor_at
   AND child.occurred_at <= TIMESTAMP_ADD(anc.anchor_at, INTERVAL 30 MINUTE)
   AND (anc.next_callback_at IS NULL OR child.occurred_at < anc.next_callback_at)
   AND child.is_anchor = FALSE
   AND (child.request_referer_path_and_query IS NULL OR child.request_referer_path_and_query IN ('/', '/?'))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY anc.anchor_request_uuid
    ORDER BY child.occurred_at ASC, child.request_uuid ASC
  ) = 1
),

bootstrap_first_event_after_callback_30m AS (
  SELECT
    child_request_uuid AS child_request_uuid,
    anchor_request_uuid AS parent_request_uuid,
    child_at AS child_at,
    anchor_at AS parent_at,
    'medium' AS match_confidence,
    'bootstrap_first_event_after_callback_same_auid_30m' AS match_source
  FROM bootstrap_first_child_after_callback
),

referrer_candidates_strong_primary AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,
    CASE
      WHEN child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip THEN 'high'
      WHEN child.auid_group_confident IS NOT NULL AND child.auid_group_confident = parent.auid_group_confident THEN 'medium'
    END AS match_confidence,
    CASE
      WHEN child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip THEN 'referrer+auid'
      WHEN child.auid_group_confident IS NOT NULL AND child.auid_group_confident = parent.auid_group_confident THEN 'referrer+auid_group_confident'
      ELSE 'referrer+weak'
    END AS match_source

  FROM events_wg child
  JOIN events_wg parent
    ON parent.request_path_and_query = child.request_referer_path_and_query
   AND (parent.is_navigable_parent OR parent.is_anchor)
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND (
     child.anonymised_user_agent_and_ip = parent.anonymised_user_agent_and_ip
     OR (child.auid_group_confident IS NOT NULL AND child.auid_group_confident = parent.auid_group_confident)
   )
  WHERE child.ruleset_type = 'PRIMARY_SINGLE_USER'
    AND parent.ruleset_type = 'PRIMARY_SINGLE_USER'
    AND child.request_referer_path_and_query IS NOT NULL
    AND child.request_referer_path_and_query NOT IN ('/', '/?')
),

referrer_slash_prev_parent_by_auid_primary AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'low' AS match_confidence,
    'referrer_slash+prev_ELIGIBLE_parent_by_auid_10m_single_user' AS match_source
  FROM prev_by_auid child
  WHERE child.ruleset_type = 'PRIMARY_SINGLE_USER'
    AND child.request_referer_path_and_query IN ('/', '/?')
    -- AND child.request_path != '/sign-in'
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND child.prev_parent_request_path_auid NOT IN ('/', '/?')
),

broken_referrer_children_primary AS (
  SELECT e.*
  FROM events_wg e
  WHERE e.ruleset_type = 'PRIMARY_SINGLE_USER'
    AND (
      e.request_referer_path_and_query IS NULL
      OR (e.request_referer_path_and_query IN ('/', '/?') 
      -- AND e.request_path != '/sign-in'
      )
    )
),

broken_referrer_candidates_primary AS (
  SELECT
    c.request_uuid AS child_request_uuid,
    p.prev_parent_request_uuid_auid AS parent_request_uuid,
    c.occurred_at AS child_at,
    p.prev_parent_occurred_at_auid AS parent_at,
    'medium' AS match_confidence,
    'broken_referrer+prev_ELIGIBLE_parent_by_auid_in_clean_window_120m' AS match_source
  FROM broken_referrer_children_primary c
  JOIN prev_by_auid p
    ON p.request_uuid = c.request_uuid
  WHERE c.eligible_window_iuid IS NOT NULL
    AND c.eligible_window_start IS NOT NULL
    AND p.prev_parent_request_uuid_auid IS NOT NULL
    AND p.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(c.occurred_at, INTERVAL 120 MINUTE)
    AND p.prev_parent_occurred_at_auid >= c.eligible_window_start
    AND p.prev_parent_occurred_at_auid <  c.eligible_window_end
),

referrer_candidates_clean_secondary AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    parent.request_uuid AS parent_request_uuid,
    child.occurred_at AS child_at,
    parent.occurred_at AS parent_at,
    'high' AS match_confidence,
    'referrer_clean_window_same_iuid' AS match_source
  FROM events_wg child
  JOIN clean_window_children cw
    ON cw.request_uuid = child.request_uuid
  JOIN events_wg parent
    ON parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.is_navigable_parent = TRUE
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
  WHERE child.ruleset_type = 'SECONDARY_SHARED_AUID'
    AND parent.ruleset_type = 'SECONDARY_SHARED_AUID'
    AND child.request_referer_path_and_query IS NOT NULL
    AND child.request_referer_path_and_query NOT IN ('/', '/?')
    AND parent.anonymised_user_agent_and_ip = child.anonymised_user_agent_and_ip
    AND parent.occurred_at >= cw.eligible_window_start
    AND parent.occurred_at <  cw.eligible_window_end
    AND parent.eligible_window_iuid = cw.eligible_window_iuid
),

referrer_candidates_nonclean_dominant_same_auid_secondary AS (
  SELECT
    child_request_uuid,
    parent_request_uuid,
    child_at,
    parent_at,
    'low' AS match_confidence,
    'referrer_nonclean_dominant_same_auid' AS match_source
  FROM (
    SELECT
      child.request_uuid AS child_request_uuid,
      parent.request_uuid AS parent_request_uuid,
      child.occurred_at AS child_at,
      parent.occurred_at AS parent_at,
      ROW_NUMBER() OVER (PARTITION BY child.request_uuid ORDER BY parent.occurred_at DESC) AS rn,
      LEAD(parent.occurred_at) OVER (PARTITION BY child.request_uuid ORDER BY parent.occurred_at DESC) AS parent_at_2
    FROM events_wg child
    LEFT JOIN clean_window_children cw
      ON cw.request_uuid = child.request_uuid
    JOIN events_wg parent
      ON parent.request_path_and_query = child.request_referer_path_and_query
     AND parent.is_navigable_parent = TRUE
     AND parent.anonymised_user_agent_and_ip = child.anonymised_user_agent_and_ip
     AND parent.occurred_at < child.occurred_at
     AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
    WHERE child.ruleset_type = 'SECONDARY_SHARED_AUID'
      AND parent.ruleset_type = 'SECONDARY_SHARED_AUID'
      AND cw.request_uuid IS NULL
      AND child.request_referer_path_and_query IS NOT NULL
      AND child.request_referer_path_and_query NOT IN ('/', '/?')
  )
  WHERE rn = 1
    AND TIMESTAMP_DIFF(child_at, parent_at, SECOND) <= 120
    AND (parent_at_2 IS NULL OR TIMESTAMP_DIFF(parent_at, parent_at_2, SECOND) >= 300)
),

referrer_candidates_strong_secondary AS (
  SELECT * FROM referrer_candidates_clean_secondary
  UNION ALL
  SELECT * FROM referrer_candidates_nonclean_dominant_same_auid_secondary
),

referrer_slash_prev_parent_by_auid_secondary AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'low' AS match_confidence,
    'referrer_slash+prev_ELIGIBLE_parent_by_auid_10m_clean_window_same_iuid' AS match_source
  FROM prev_by_auid child
  JOIN clean_window_children cw
    ON cw.request_uuid = child.request_uuid
  JOIN events_wg parent
    ON parent.request_uuid = child.prev_parent_request_uuid_auid
   AND (parent.is_navigable_parent OR parent.is_anchor) = TRUE
  WHERE child.ruleset_type = 'SECONDARY_SHARED_AUID'
    AND parent.ruleset_type = 'SECONDARY_SHARED_AUID'
    AND child.request_referer_path_and_query IN ('/', '/?')
    AND child.request_path != '/sign-in'
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND child.prev_parent_request_path_auid NOT IN ('/', '/?')
    AND parent.eligible_window_iuid = cw.eligible_window_iuid
),

broken_referrer_children_secondary AS (
  SELECT e.*
  FROM events_wg e
  WHERE e.ruleset_type = 'SECONDARY_SHARED_AUID'
    AND (
      e.request_referer_path_and_query IS NULL
      OR (e.request_referer_path_and_query IN ('/', '/?') AND e.request_path != '/sign-in')
    )
),

broken_referrer_candidates_secondary AS (
  SELECT
    c.request_uuid AS child_request_uuid,
    p.prev_parent_request_uuid_auid AS parent_request_uuid,
    c.occurred_at AS child_at,
    p.prev_parent_occurred_at_auid AS parent_at,
    'medium' AS match_confidence,
    'broken_referrer+prev_ELIGIBLE_parent_by_auid_in_clean_window_120m' AS match_source
  FROM broken_referrer_children_secondary c
  JOIN prev_by_auid p
    ON p.request_uuid = c.request_uuid
  JOIN clean_window_children cw
    ON cw.request_uuid = c.request_uuid
  WHERE p.prev_parent_request_uuid_auid IS NOT NULL
    AND p.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(c.occurred_at, INTERVAL 120 MINUTE)
    AND p.prev_parent_occurred_at_auid >= cw.eligible_window_start
    AND p.prev_parent_occurred_at_auid <  cw.eligible_window_end
),

parent_candidates AS (
  SELECT * FROM referrer_candidates_strong_primary
  UNION ALL SELECT * FROM referrer_slash_prev_parent_by_auid_primary
  UNION ALL SELECT * FROM broken_referrer_candidates_primary

  UNION ALL

  SELECT * FROM referrer_candidates_strong_secondary
  UNION ALL SELECT * FROM referrer_slash_prev_parent_by_auid_secondary
  UNION ALL SELECT * FROM broken_referrer_candidates_secondary

  UNION ALL 
  SELECT * FROM bootstrap_first_event_after_callback_30m
),

best_parent AS (
  SELECT
    child_request_uuid,
    parent_request_uuid,
    match_confidence,
    match_source
  FROM parent_candidates
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child_request_uuid
    ORDER BY
      CASE match_confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC,
      parent_at DESC
  ) = 1
),

events_with_parent AS (
  SELECT
    e.*,
    CASE WHEN e.is_anchor THEN NULL ELSE bp.parent_request_uuid END AS parent_request_uuid_guarded,
    bp.match_confidence AS parent_match_confidence_guarded,
    bp.match_source     AS parent_match_source_guarded
  FROM events_wg e
  LEFT JOIN best_parent bp
    ON e.request_uuid = bp.child_request_uuid
),

walk AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.request_uuid AS current_request_uuid,
    e.parent_request_uuid_guarded AS parent_request_uuid,
    e.is_anchor,
    e.inferred_user_id,
    0 AS depth,
    IF(e.is_anchor, e.inferred_user_id, NULL) AS nearest_anchor_user_id
  FROM events_with_parent e

  UNION ALL

  SELECT
    w.start_request_uuid,
    p.request_uuid AS current_request_uuid,
    p.parent_request_uuid_guarded AS parent_request_uuid,
    p.is_anchor,
    p.inferred_user_id,
    w.depth + 1 AS depth,
    COALESCE(w.nearest_anchor_user_id, IF(p.is_anchor, p.inferred_user_id, NULL)) AS nearest_anchor_user_id
  FROM walk w
  JOIN events_with_parent p
    ON w.parent_request_uuid = p.request_uuid
  WHERE w.parent_request_uuid IS NOT NULL
    AND w.depth < 400
),

collapsed AS (
  SELECT
    start_request_uuid AS request_uuid,
    ARRAY_AGG(
      IF(parent_request_uuid IS NULL, current_request_uuid, NULL)
      IGNORE NULLS
      ORDER BY depth DESC
      LIMIT 1
    )[OFFSET(0)] AS chain_id_guarded,
    ARRAY_AGG(
      nearest_anchor_user_id
      IGNORE NULLS
      ORDER BY depth ASC
      LIMIT 1
    )[OFFSET(0)] AS propagated_user_id_guarded
  FROM walk
  GROUP BY start_request_uuid
),

final_assignment_base AS (
  SELECT
    e.*,
    c.chain_id_guarded,
    c.propagated_user_id_guarded AS inferred_user_id_propagated_guarded,

    IF(ag.has_admin_activity, TRUE, FALSE) AS is_admin_actor,

    CASE
      WHEN ag.has_admin_activity
       AND e.auid_group_confident IS NOT NULL
      THEN CONCAT('admin:', CAST(e.auid_group_confident AS STRING))
      ELSE NULL
    END AS admin_master_id,

    CASE
      WHEN e.is_anchor THEN e.inferred_user_id
      WHEN c.propagated_user_id_guarded IS NOT NULL THEN c.propagated_user_id_guarded
      WHEN e.eligible_window_iuid IS NOT NULL
        AND e.window_gate_reason = 'eligible_single_iuid'
        AND EXISTS (
          SELECT 1
          FROM ${ctx.ref(pass2Name)} w
          WHERE w.auid = e.anonymised_user_agent_and_ip
            AND w.iuid = e.eligible_window_iuid
            AND w.window_quality = 'clean'
            AND e.occurred_at >= w.window_start
            AND e.occurred_at <  w.window_end
        )
      THEN e.eligible_window_iuid
      ELSE NULL
    END AS inferred_user_id_final_raw

  FROM events_with_parent e
  LEFT JOIN collapsed c
    ON e.request_uuid = c.request_uuid
  LEFT JOIN admin_group_map ag
    ON ag.auid_group_confident = e.auid_group_confident
)

SELECT
  b.*,

  CASE
    WHEN b.is_admin_actor
     AND b.admin_master_id IS NOT NULL
     AND b.inferred_user_id_final_raw IS NOT NULL
    THEN b.admin_master_id
    ELSE b.inferred_user_id_final_raw
  END AS inferred_user_id_final,

  CASE
    WHEN b.is_anchor AND b.is_admin_actor THEN 'Is anchor event (admin normalised)'
    WHEN b.is_anchor THEN 'Is anchor event'

    WHEN b.inferred_user_id_propagated_guarded IS NOT NULL AND b.is_admin_actor
      THEN 'Propagated through chain from anchor event (admin normalised)'
    WHEN b.inferred_user_id_propagated_guarded IS NOT NULL
      THEN 'Propagated through chain from anchor event'

    WHEN b.eligible_window_iuid IS NOT NULL
      AND b.window_gate_reason = 'eligible_single_iuid'
      AND b.inferred_user_id_final_raw = b.eligible_window_iuid
      AND b.is_admin_actor
    THEN 'Inferred from sign-in window (broken chain) (admin normalised)'

    WHEN b.eligible_window_iuid IS NOT NULL
      AND b.window_gate_reason = 'eligible_single_iuid'
      AND b.inferred_user_id_final_raw = b.eligible_window_iuid
    THEN 'Inferred from sign-in window (broken chain)'

    WHEN b.ruleset_type = 'NO_SIGNIN_AUID'
    THEN 'Unassigned - no observed sign-in on this AUID'

    ELSE 'Unassigned'
  END AS iuid_assignment_source,

  CASE
    WHEN NOT b.is_anchor
     AND b.inferred_user_id_propagated_guarded IS NULL
     AND b.eligible_window_iuid IS NOT NULL
     AND b.window_gate_reason = 'eligible_single_iuid'
     AND b.request_path_and_query NOT LIKE '%/admin%'
     AND b.inferred_user_id_final_raw = b.eligible_window_iuid
    THEN TRUE
    ELSE FALSE
  END AS follows_missing_block

FROM final_assignment_base b`
);

  return publish(finalName, {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    bigquery: {
      partitionBy: "DATE(session_start_timestamp)",
      updatePartitionFilter:
        "final_session_page_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    assertions: {
      uniqueKey: [["session_id"]]
    },
    tags: [params.eventSourceName.toLowerCase()],
    description:
      "This table contains data on sessions and accompanying metrics. Each row is a single session.",
    dependencies: [...(params.dependencies || []), pass3Name],
    columns: {
      session_id: "The unique ID of the session",
      user_id: "UUID of the user. This is only available for users who have signed into the service during their session.",
      session_namespace: "The namespace of the instance of dfe-analytics that streamed the first web visit event in this session.",
      start_page: "The page URL of the first page visited in the session",
      utm_source: "UTM source.",
      utm_medium: "UTM medium.",
      utm_campaign: "UTM campaign.",
      medium: "Categorised traffic medium.",
      exit_page: "The page URL of the last page visited in the session",
      session_start_timestamp: "Timestamp of the first page visit in the session",
      final_session_page_timestamp: "Timestamp of the last page visit in the session",
      session_time_in_seconds: "The duration of the session in seconds.",
      count_pages_visited: "The number of pages visited during the session."
    }
  }).query(
    ctx => `
WITH
  events AS (
    SELECT
      anonymised_user_agent_and_ip,
      request_user_id AS request_user_id,
      occurred_at,
      namespace,
      request_path,
      request_query,
      request_path_and_query AS page_path_and_query,
      request_referer_domain,
      IF(
        SUBSTR(request_referer_path_and_query, -1) = '?',
        SPLIT(request_referer_path_and_query, "?")[0],
        request_referer_path_and_query
      ) AS referer_path_and_query
    FROM ${ctx.ref(pass3Name)}
    WHERE event_type = "web_request"
      AND device_category != "bot"
      AND CONTAINS_SUBSTR(response_content_type, "text/html")
      AND response_status NOT LIKE "3__"
      AND response_status NOT LIKE "4__"
      AND occurred_at > event_timestamp_checkpoint
  ),

  events_with_next_visit_to_url AS (
    SELECT
      *,
      COALESCE(
        LEAD(occurred_at) OVER (
          PARTITION BY request_user_id, page_path_and_query, DATE(occurred_at)
          ORDER BY occurred_at
        ),
        LEAD(occurred_at) OVER (
          PARTITION BY anonymised_user_agent_and_ip, page_path_and_query, DATE(occurred_at)
          ORDER BY occurred_at
        )
      ) AS next_same_page_path_and_query_occurred_at
    FROM events
  ),

  events_with_next_page_details AS (
    SELECT
      e1.*,
      e2.next_page_path_and_query,
      e2.next_page_user_id,
      e2.next_page_timestamp
    FROM events_with_next_visit_to_url e1
    LEFT JOIN (
      SELECT
        anonymised_user_agent_and_ip,
        page_path_and_query,
        referer_path_and_query,
        page_path_and_query AS next_page_path_and_query,
        request_user_id AS next_page_user_id,
        occurred_at AS next_page_timestamp
      FROM events
    ) e2
      ON (
        (e1.anonymised_user_agent_and_ip = e2.anonymised_user_agent_and_ip)
        OR (e1.request_user_id = e2.next_page_user_id)
      )
     AND e1.page_path_and_query = e2.referer_path_and_query
     AND DATE(e1.occurred_at) = DATE(e2.next_page_timestamp)
     AND e1.occurred_at < e2.next_page_timestamp
     AND (
       e1.next_same_page_path_and_query_occurred_at IS NULL
       OR e2.next_page_timestamp < e1.next_same_page_path_and_query_occurred_at
     )
  ),

  events_with_following_pages AS (
    SELECT
      anonymised_user_agent_and_ip,
      request_user_id,
      namespace,
      request_path,
      request_query,
      page_path_and_query,
      request_referer_domain,
      referer_path_and_query,
      occurred_at,
      ARRAY_AGG(
        STRUCT(
          next_page_path_and_query,
          next_page_user_id,
          next_page_timestamp
        )
        ORDER BY next_page_timestamp
      ) AS following_pages
    FROM events_with_next_page_details
    GROUP BY
      request_user_id,
      anonymised_user_agent_and_ip,
      namespace,
      request_path,
      request_query,
      page_path_and_query,
      request_referer_domain,
      referer_path_and_query,
      occurred_at
  ),

  events_with_next_and_prev_user_id AS (
    SELECT
      *,
      CASE
        WHEN TIMESTAMP_DIFF(
          LEAD(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at),
          occurred_at,
          MINUTE
        ) <= 30
        THEN FIRST_VALUE(request_user_id IGNORE NULLS)
          OVER (
            PARTITION BY anonymised_user_agent_and_ip
            ORDER BY UNIX_SECONDS(occurred_at)
            RANGE BETWEEN CURRENT ROW AND 1800 FOLLOWING
          )
        ELSE (
          SELECT MIN_BY(fp.next_page_user_id, fp.next_page_timestamp)
          FROM UNNEST(following_pages) AS fp
          WHERE fp.next_page_user_id IS NOT NULL
        )
      END AS next_user_id,
      CASE
        WHEN TIMESTAMP_DIFF(
          occurred_at,
          LAG(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at),
          MINUTE
        ) <= 30
        THEN LAST_VALUE(request_user_id IGNORE NULLS)
          OVER (
            PARTITION BY anonymised_user_agent_and_ip
            ORDER BY UNIX_SECONDS(occurred_at)
            RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW
          )
      END AS prev_user_id
    FROM events_with_following_pages
  ),

  events_with_users_estimated AS (
    SELECT
      anonymised_user_agent_and_ip,
      occurred_at,
      request_user_id,
      COALESCE(request_user_id, next_user_id, prev_user_id) AS estimated_user_id,
      namespace,
      request_path,
      request_query,
      page_path_and_query,
      request_referer_domain,
      referer_path_and_query,
      IF(
        (
          SELECT MIN_BY(fp.next_page_path_and_query, fp.next_page_timestamp)
          FROM UNNEST(following_pages) AS fp
        ) IS NOT NULL,
        TRUE,
        FALSE
      ) AS visited_future_page
    FROM events_with_next_and_prev_user_id
  ),

  user_page_visits AS (
    SELECT
      *,
      CASE
        WHEN LEAD(estimated_user_id) OVER page_visits_for_this_estimated_user IS NOT NULL
         AND estimated_user_id = LEAD(estimated_user_id) OVER page_visits_for_this_estimated_user
         AND (
           TIMESTAMP_DIFF(
             LEAD(occurred_at) OVER page_visits_for_this_estimated_user,
             occurred_at,
             MINUTE
           ) <= 30
           OR visited_future_page
         )
        THEN "Visited subsequent pages"
        ELSE "Left site immediately after this"
      END AS next_step
    FROM events_with_users_estimated
    WHERE estimated_user_id IS NOT NULL
    WINDOW page_visits_for_this_estimated_user AS (
      PARTITION BY estimated_user_id
      ORDER BY occurred_at
    )
  ),

  user_page_visits_with_session_boundaries AS (
    SELECT
      anonymised_user_agent_and_ip,
      estimated_user_id AS user_id,
      occurred_at AS page_visit_at,
      namespace,
      request_path AS page_path,
      ${parameter_functions.attributionParamFields(params)}
      request_referer_domain,
      REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
      next_step,
      CASE
        WHEN LAG(next_step) OVER page_visits_for_this_estimated_user IS NULL THEN TRUE
        WHEN LAG(next_step) OVER page_visits_for_this_estimated_user = "Left site immediately after this" THEN TRUE
        ELSE FALSE
      END AS new_session
    FROM user_page_visits
    WINDOW page_visits_for_this_estimated_user AS (
      PARTITION BY estimated_user_id
      ORDER BY occurred_at
    )
  ),

  user_page_visits_with_session_number AS (
    SELECT
      *,
      COUNT(CASE WHEN new_session THEN 1 END) OVER (PARTITION BY user_id ORDER BY page_visit_at) AS session_number,
      ${requestToMedium(ctx)} AS medium
    FROM user_page_visits_with_session_boundaries
  ),

  user_page_visits_with_session_id AS (
    SELECT
      *,
      CONCAT(
        user_id,
        "-",
        CAST(FIRST_VALUE(page_visit_at) OVER (PARTITION BY user_id, session_number ORDER BY page_visit_at) AS STRING)
      ) AS session_id
    FROM user_page_visits_with_session_number
  ),

  non_user_page_visits AS (
    SELECT
      *,
      CASE
        WHEN LEAD(estimated_user_id) OVER page_visits_for_this_anonymised_user_agent_and_ip IS NULL
         AND anonymised_user_agent_and_ip = LEAD(anonymised_user_agent_and_ip) OVER page_visits_for_this_anonymised_user_agent_and_ip
         AND (
           TIMESTAMP_DIFF(
             LEAD(occurred_at) OVER page_visits_for_this_anonymised_user_agent_and_ip,
             occurred_at,
             MINUTE
           ) <= 30
           OR visited_future_page
         )
        THEN "Visited Subsequent Pages"
        ELSE "Left site immediately after this"
      END AS next_step
    FROM events_with_users_estimated
    WHERE estimated_user_id IS NULL
    WINDOW page_visits_for_this_anonymised_user_agent_and_ip AS (
      PARTITION BY anonymised_user_agent_and_ip
      ORDER BY occurred_at
    )
  ),

  non_user_page_visits_with_session_boundaries AS (
    SELECT
      anonymised_user_agent_and_ip,
      estimated_user_id AS user_id,
      occurred_at AS page_visit_at,
      namespace,
      request_path AS page_path,
      ${parameter_functions.attributionParamFields(params)}
      request_referer_domain,
      REGEXP_EXTRACT(referer_path_and_query, r'^([^?]+)') AS previous_page_path,
      next_step,
      CASE
        WHEN LAG(next_step) OVER page_visits_for_this_anonymised_user_agent_and_ip IS NULL THEN TRUE
        WHEN LAG(next_step) OVER page_visits_for_this_anonymised_user_agent_and_ip = "Left site immediately after this" THEN TRUE
        ELSE FALSE
      END AS new_session
    FROM non_user_page_visits
    WINDOW page_visits_for_this_anonymised_user_agent_and_ip AS (
      PARTITION BY anonymised_user_agent_and_ip
      ORDER BY occurred_at
    )
  ),

  non_user_page_visits_with_session_number AS (
    SELECT
      *,
      COUNT(CASE WHEN new_session THEN 1 END) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY page_visit_at) AS session_number,
      ${requestToMedium(ctx)} AS medium
    FROM non_user_page_visits_with_session_boundaries
  ),

  nonuser_page_visits_with_session_id AS (
    SELECT
      *,
      CONCAT(
        anonymised_user_agent_and_ip,
        "-",
        CAST(FIRST_VALUE(page_visit_at) OVER (PARTITION BY anonymised_user_agent_and_ip, session_number ORDER BY page_visit_at) AS STRING)
      ) AS session_id
    FROM non_user_page_visits_with_session_number
  ),

  sessions_grouped AS (
    SELECT
      anonymised_user_agent_and_ip,
      user_id,
      page_visit_at,
      namespace,
      page_path,
      utm_source,
      utm_medium,
      utm_campaign,
      medium,
      request_referer_domain,
      previous_page_path,
      next_step,
      session_id
    FROM user_page_visits_with_session_id
    UNION ALL
    SELECT
      anonymised_user_agent_and_ip,
      user_id,
      page_visit_at,
      namespace,
      page_path,
      utm_source,
      utm_medium,
      utm_campaign,
      medium,
      request_referer_domain,
      previous_page_path,
      next_step,
      session_id
    FROM nonuser_page_visits_with_session_id
  ),

  page_times AS (
    SELECT
      session_id,
      anonymised_user_agent_and_ip,
      user_id,
      namespace,
      page_path,
      utm_source,
      utm_medium,
      utm_campaign,
      medium,
      request_referer_domain,
      previous_page_path,
      page_visit_at AS page_entry_time,
      LEAD(page_visit_at) OVER session_window AS page_exit_time,
      TIMESTAMP_DIFF(LEAD(page_visit_at) OVER session_window, page_visit_at, SECOND) AS page_duration_seconds,
      next_step
    FROM sessions_grouped
    WINDOW session_window AS (PARTITION BY session_id ORDER BY page_visit_at)
  ),

  session_metrics AS (
    SELECT
      session_id,
      user_id,
      ARRAY_AGG(
        STRUCT(
          anonymised_user_agent_and_ip,
          page_path AS page,
          request_referer_domain AS previous_page_domain,
          previous_page_path AS previous_page,
          page_entry_time,
          page_exit_time,
          page_duration_seconds AS duration,
          next_step
        )
        ORDER BY page_entry_time
      ) AS pages_visited_details,
      ARRAY_AGG(
        STRUCT(
          namespace,
          utm_source,
          utm_medium,
          utm_campaign,
          medium
        )
        ORDER BY page_entry_time
      ) AS session_level_metrics,
      MIN(page_entry_time) AS session_start_timestamp,
      MAX(page_entry_time) AS final_session_page_timestamp,
      COUNT(*) AS count_pages_visited
    FROM page_times
    GROUP BY session_id, user_id
  )

SELECT
  session_id,
  user_id,
  session_level_metrics[0].namespace AS session_namespace,
  pages_visited_details[0].previous_page_domain AS session_referer_domain,
  pages_visited_details[0].page AS start_page,
  session_level_metrics[0].utm_source AS utm_source,
  session_level_metrics[0].utm_medium AS utm_medium,
  session_level_metrics[0].utm_campaign AS utm_campaign,
  session_level_metrics[0].medium AS medium,
  ARRAY_REVERSE(pages_visited_details)[0].page AS exit_page,
  session_start_timestamp,
  final_session_page_timestamp,
  CASE
    WHEN final_session_page_timestamp = session_start_timestamp THEN NULL
    ELSE TIMESTAMP_DIFF(final_session_page_timestamp, session_start_timestamp, SECOND)
  END AS session_time_in_seconds,
  count_pages_visited,
  pages_visited_details
FROM session_metrics`
  ).preOps(ctx => `
    DECLARE event_timestamp_checkpoint TIMESTAMP DEFAULT (
      ${ctx.incremental() ? `SELECT MAX(session_start_timestamp) FROM ${ctx.self()}` : `SELECT TIMESTAMP("2000-01-01")`}
    );
  `);
};