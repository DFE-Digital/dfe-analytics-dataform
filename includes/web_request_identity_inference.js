/* 
IdentityConfig: 

Load the service-specific identity-inference configuration. This configuration file allows the same pipeline generator to be reused for multiple web analytics event sources.
   
Each service can define its own: 
    - identity-bearing anchor requests: Any authenticated database update can be a valid anchor if the update event is linked to the triggering web request by request_uuid and the extracted user ID is the authenticated actor.
    - sign-in and sign-out paths
    - authenticated area paths 
    - admin page patterns
    - earliest valid source data date (typically this will be chosen based on the amount of data that can be processed. Testing suggest 10 million web request events is the maximum that can be processed in a single run.)
*/
  const identityConfig = require("../definitions/web_analytics_identity_inference_config");

/* Load dfe-analytics shared parameter helper functions. */
  const parameter_functions = require("./parameter_functions");

/* Export a function that receives the pipeline parameters and dynamically defines the required Dataform models. The SQL and table names generated later in this file 
   depend on these params. */
  module.exports = params => {
    if (!params.enableWebRequestIdentityInference) {
      return true;
    }

/* --------------------------------------------------------------------------
   1a. Load and validate service-specific configuration

   Select the configuration object for the current event source.

   Example:
     params.eventSourceName = "itt_mentors"

   The matching configuration must exist at:
     identityConfig["itt_mentors"]

   This service-level configuration contains the identity rules and path
   definitions used to generate the SQL throughout the pipeline.

   Each configured service must include:
     - identity: rules for extracting trusted identity anchors (Defined as: Any 
     authenticated database update can be a valid anchor if the update event is 
     linked to the triggering web request by request_uuid and the extracted user ID 
     is the authenticated actor.)
     - paths: service-specific public, authentication, sign-out, and admin paths.

   The pipeline fails immediately if:
     - the current event source has no matching configuration entry;
     - the matching configuration is missing the required `identity` object;
     - the matching configuration is missing the required `paths` object.

   Failing early prevents the pipeline from generating incomplete SQL or
   silently applying identity resolution logic with missing service specific
   rules.
------------------------------------------------------------------------------ */

  const webAnalytics = identityConfig[params.eventSourceName];

  if (!webAnalytics) {
    throw new Error(
      `web request identity resolution is enabled for event source ` +
      `'${params.eventSourceName}', but no matching service configuration was found. ` +
      `Add an entry at identityConfig['${params.eventSourceName}'].`
    );
  }

  const identity = webAnalytics.identity;

  if (!identity) {
    throw new Error(
      `web request identity resolution configuration for event source ` +
      `'${params.eventSourceName}' is missing the required 'identity' object. ` +
      `Add identity rules at identityConfig['${params.eventSourceName}'].identity.`
    );
  }

  const paths = webAnalytics.paths;

  if (!paths) {
    throw new Error(
      `web request identity resolution configuration for event source ` +
      `'${params.eventSourceName}' is missing the required 'paths' object. ` +
      `Add path rules at identityConfig['${params.eventSourceName}'].paths.`
    );
  }

  /* Using ?? false means services must opt in explicitly. Missing features 
  or a missing flag will safely disable normalisation without breaking compilation. */
  const enableAdminNormalisation =
  webAnalytics.features?.enableAdminNormalisation ?? false;

/*
  The start date will be set to either the startDate defined in the service specific
  config file or '2025-01-01'. This date is chosen because prior to this date, and
  a less stable device identifier was collected and therefore cannot be reliable used.
*/

  const startDate =
    webAnalytics.startDate ||
    "2025-01-01";

/* --------------------------------------------------------------------------
   1b. Configure and validate trusted identity anchor sources

   Define the request patterns that can provide trusted links between:
     - an anonymous user identifier (AUID); and
     - a known inferred user identifier (IUID).

   Each element of `identity.anchorSources` represents one type of
   identity bearing request. The SQL generator uses these definitions to
   extract known IUIDs from qualifying events and create the anchor table that
   supports all downstream identity inference.

   Each anchor-source definition must include:
     - entityTableName:
         The source entity associated with the qualifying request.

     - requestPaths:
         One or more request paths that identify the relevant endpoint.
         Paths may include a trailing wildcard where prefix matching is needed.

     - requestMethods:
         One or more qualifying HTTP methods.
         If omitted, the SQL-generation helper defaults this to ["GET"].

     - dataField:
         The nested event-data field containing the identity-bearing value.

     - userIdKey:
         The key used to extract the IUID from the nested data field.

   The pipeline fails immediately if `identity.anchorSources` is missing, is
   not an array, or contains no entries. Identity propagation cannot operate
   without at least one configured source of trusted identity anchors.
------------------------------------------------------------------------------ */

  const anchorSources = identity.anchorSources;

  if (!Array.isArray(anchorSources) || anchorSources.length === 0) {
    throw new Error(
      `web request identity resolution configuration for event source ` +
      `'${params.eventSourceName}' must include at least one trusted identity anchor source. ` +
      `Add one or more entries to ` +
      `identityConfig['${params.eventSourceName}'].identity.anchorSources.`
    );
  }

  /*
    Validate each trusted anchor source definition before generating SQL.
  */

  anchorSources.forEach((source, index) => {
    const sourceLocation =
      `identityConfig['${params.eventSourceName}'].identity.anchorSources[${index}]`;

    if (!source || typeof source !== "object") {
      throw new Error(
        `${sourceLocation} must be an object containing a trusted anchor-source definition.`
      );
    }

    if (!source.entityTableName) {
      throw new Error(
        `${sourceLocation} is missing the required 'entityTableName' value.`
      );
    }

    if (!Array.isArray(source.requestPaths) || source.requestPaths.length === 0) {
      throw new Error(
        `${sourceLocation} must include at least one value in 'requestPaths'.`
      );
    }

    if (
      source.requestMethods !== undefined &&
      (
        !Array.isArray(source.requestMethods) ||
        source.requestMethods.length === 0
      )
    ) {
      throw new Error(
        `${sourceLocation}.requestMethods must be a non-empty array when provided.`
      );
    }

    if (!source.dataField) {
      throw new Error(
        `${sourceLocation} is missing the required 'dataField' value.`
      );
    }

    if (!source.userIdKey) {
      throw new Error(
        `${sourceLocation} is missing the required 'userIdKey' value.`
      );
    }
  });


/* --------------------------------------------------------------------------
    1c. Incremental Run config

    Number of additional hours to rebuild before the calculated incremental 
    checkpoint. 

    The pipeline deliberately reprocesses an overlap period on each incremental
    run. This allows identity chains, activity windows, and downstream sessions
    that cross the rebuild boundary to be reconstructed consistently.
------------------------------------------------------------------------------ */

  const identityIncrementalRebuildHours =
    params.identityIncrementalRebuildHours || 6;

/* --------------------------------------------------------------------------
  1d. Path config

   Define the page paths used by the identity-resolution rules.

   These path groups influence how the pipeline interprets navigation events,
   authentication transitions, activity window evidence, and admin activity.

   Some path groups may legitimately be empty:
     - authPrefixes:
         A service may not have a clearly defined authenticated area prefix.

     - adminPatterns:
         A service may not expose admin pages or may not require admin
         identity normalisation.

   An empty array should be used where a category is intentionally not
   applicable to the service.
------------------------------------------------------------------------------ */

  const preAuthPagePaths = paths.preAuth || ["/", "/?"];
  const signInPagePaths = paths.signIn || ["/sign-in"];
  const signOutPaths = paths.signOut || ["/sign-out"];

  const authPrefixes = paths.authPrefixes || [];
  const adminPagePatterns = paths.adminPatterns || [];

/* This is a small helper function that removes duplicates from an array */
  const unique = values => [...new Set(values)];

  const preAuthAndSignInPagePaths = unique([
    ...preAuthPagePaths,
    ...signInPagePaths
  ]);

  const publicAndAuthPagePaths = unique([
    ...preAuthPagePaths,
    ...signInPagePaths,
    ...signOutPaths
  ]);

  /*
    Validate optional path groups after applying defaults.

    Each path group is used as an array when generating SQL conditions.
    Empty arrays are valid for optional categories such as authenticated-area
    prefixes and admin-page patterns.
  */
  [
    ["preAuth", preAuthPagePaths],
    ["signIn", signInPagePaths],
    ["signOut", signOutPaths],
    ["authPrefixes", authPrefixes],
    ["adminPatterns", adminPagePatterns]
  ].forEach(([pathGroupName, configuredPaths]) => {
    if (!Array.isArray(configuredPaths)) {
      throw new Error(
        `web request identity resolution configuration for event source ` +
        `'${params.eventSourceName}' must define paths.${pathGroupName} as an array.`
      );
    }
  });

/* --------------------------------------------------------------------------
    1e. Define service specific table names

    Define the BigQuery dataset and model names used throughout the identity-
    resolution pipeline.

    The pipeline is generated dynamically for each event source. Appending
    `params.eventSourceName` to each model name allows the same shared pipeline
    logic to create separate tables for different services.

    Intermediate identity resolution models are published to the shared
    `web_analytics_staging_tables` dataset. Intermediate models are necessary to
    ensure that the query is not to complex for BigQuery to run.

    The generated model names broadly correspond to the pipeline stages:
      - source web events
      - trusted identity anchors
      - AUID risk classification
      - initial direct identity assignment
      - conservative recursive propagation
      - activity-window fallback
      - targeted post-window repair
      - solved event output after admin identity normalisation

    Some recursive stages use separate input, run, and persisted-state tables.
    This is necessary because the durable staging tables are incremental, but
    recursive SQL must run in a standard table rather than inside an incremental
    MERGE operation.

    The input table detects whether the model is running incrementally and
    creates a filtered rebuild scope. The standard run table performs the
    recursive walk on that scope. The persisted-state table then replaces the
    overlapping rebuild period with the newly calculated results while retaining
    the historical state outside that period.
------------------------------------------------------------------------------ */

  const stagingSchema = "web_analytics_staging_tables";

  const eventsTableName = `events_${params.eventSourceName}`;

  const anchorsAllName =
    `identity_anchors_all_${params.eventSourceName}`;

  const auidStateName =
    `identity_auid_state_${params.eventSourceName}`;

  const classifyEventsName =
  `identity_classify_events_${params.eventSourceName}`;

  const conservativeWalkInputName =
    `identity_conservative_recursive_walk_input_${params.eventSourceName}`;

  const conservativeWalkRunName =
    `identity_conservative_recursive_walk_run_${params.eventSourceName}`;

  const conservativeWalkName =
    `identity_conservative_recursive_walk_${params.eventSourceName}`;

  const activityWindowsName =
    `identity_activity_windows_${params.eventSourceName}`;

  const secondWalkInputName =
    `identity_second_recursive_walk_input_${params.eventSourceName}`;

  const secondWalkRunName =
    `identity_second_recursive_walk_run_${params.eventSourceName}`;

  const secondWalkName =
    `identity_second_recursive_walk_${params.eventSourceName}`;

  const resolvedAdminName =
    `identity_solved_events_${params.eventSourceName}`;

/* --------------------------------------------------------------------------
     1f. SQL helper functions

     These JavaScript helpers generate reusable SQL fragments. They allow the 
     same pipeline to adapt to service specific arrays of paths, prefixes, 
     anchor sources, and incremental run settings without duplicating SQL 
     for each service.
-------------------------------------------------------------------------- */

/* Convert a JavaScript value into an escaped SQL string literal. */
  function sqlString(value) {
    return `'${String(value).replace(/'/g, "\\'")}'`;
  }

/* Convert a JavaScript array into a BigQuery SQL array of escaped strings.*/
  function sqlStringArray(values = []) {
    return `[${values.map(sqlString).join(", ")}]`;
  }

/* Build a qualified SQL field reference such as `e.request_path`. */
  function sqlFieldRef(alias, fieldName) {
    return `${alias}.${fieldName}`;
  }

/* Generate a BigQuery `IN UNNEST([...])` condition. Return FALSE 
for an empty list so the generated SQL remains valid and correctly 
matches no rows. */
  function sqlInList(field, values = []) {
    if (!values.length) {
      return "FALSE";
    }
    return `${field} IN UNNEST(${sqlStringArray(values)})`;
  }

/* Generate a BigQuery `NOT IN UNNEST([...])` condition. Return FALSE for 
an empty list so the generated SQL remains valid and correctly matches 
no rows. */
  function sqlNotInList(field, values = []) {
    if (!values.length) {
      return "TRUE";
    }
    return `${field} NOT IN UNNEST(${sqlStringArray(values)})`;
  }

/* Generate an AND condition that checks whether a field starts with none of 
the configured prefixes. Return TRUE for an empty list so no paths are excluded 
when a service has no authenticated-area prefixes. This is a JS function to allow the length
of the input list to be dynamic. */
  function sqlNotStartsWithAny(field, prefixes = []) {
    if (!prefixes.length) {
      return "TRUE";
    }
    return prefixes
      .map(prefix => `NOT STARTS_WITH(${field}, ${sqlString(prefix)})`)
      .join(" AND ");
  }

/* Generate an OR condition that checks whether a field matches any configured
   regular expression pattern. Return FALSE for an empty list so that no rows
   are treated as admin page events when a service has no admin patterns. This 
   is a JS function to allow the length of the input list to be dynamic.  */
  function sqlRegexpContainsAny(field, patterns = []) {
    if (!patterns.length) {
      return "FALSE";
    }
    return patterns
      .map(pattern => `REGEXP_CONTAINS(${field}, ${sqlString(pattern)})`)
      .join(" OR ");
  }


/* Escape characters that have a special meaning inside a regular expression.
   This allows configured paths to be treated literally except for `*`, which
   is deliberately supported as a wildcard. */
function escapeRegex(value) {
  return value.replace(/[.+?^${}()|[\]\\]/g, "\\$&");
}

/* Generate a SQL condition that matches any configured request path.

   Exact paths use equality. Paths containing `*` are converted into anchored
   regular expressions, where each `*` matches zero or more characters.
   All other regular-expression characters are escaped and treated literally.

   Return FALSE for an empty list so that no paths are matched. */

function sqlPathMatchesAny(field, paths = []) {
  if (!paths.length) {
    return "FALSE";
  }

  return paths
    .map(path => {
      if (!path.includes("*")) {
        return `${field} = ${sqlString(path)}`;
      }

      const regexPattern = path
        .split("*")
        .map(escapeRegex)
        .join(".*");

      return `REGEXP_CONTAINS(${field}, ${sqlString(`^${regexPattern}$`)})`;
    })
    .join(" OR ");
}

/* Generate one trusted-anchor extraction query for each configured anchor
   source and combine the results with UNION ALL.

   JavaScript is required because services may define different numbers of
   anchor sources, and each source may use different request paths, methods,
   nested data fields, and user-ID keys. */
function sqlAnchorSourceQueries(sources) {
  return sources
    .map(source => `
      SELECT DISTINCT
        e.request_uuid,
        e.occurred_at,
        DATE(e.occurred_at) AS event_date,
        e.anonymised_user_agent_and_ip AS auid,
        d.value[SAFE_OFFSET(0)] AS iuid
      FROM events_base e
      JOIN UNNEST(e.${source.dataField}) AS d
      WHERE e.entity_table_name = ${sqlString(source.entityTableName)}
        AND (${sqlPathMatchesAny("e.request_path", source.requestPaths)})
        AND ${sqlInList(
          "e.request_method",
          source.requestMethods || ["GET"]
        )}
        AND d.key = ${sqlString(source.userIdKey)}
        AND d.value[SAFE_OFFSET(0)] IS NOT NULL
    `)
    .join("\nUNION ALL\n");
}

  /* Generate pre-operations for durable incremental event-state tables. On a full refresh, rebuild from the configured start date. 
  
  On an incremental run: 
  1. calculate an overlapping rebuild checkpoint from the latest stored date 
  2. move the checkpoint back by one day and the configured additional hours 
  3. delete the affected rows before rebuilding that period. 
  
  Dataform's `ctx.incremental()` value is evaluated when the model is compiled and determines which SQL should be generated. */

  function identityEventCheckpointPreOps(ctx) {
    return `
      DECLARE identity_rebuild_checkpoint TIMESTAMP DEFAULT (
        ${
          ctx.incremental()
            ? `SELECT TIMESTAMP_SUB(
                  TIMESTAMP(
                    DATE_SUB(
                      COALESCE(MAX(event_date), DATE(${sqlString(startDate)})),
                      INTERVAL 1 DAY
                    )
                  ),
                  INTERVAL ${identityIncrementalRebuildHours} HOUR
                )
                FROM ${ctx.self()}`
            : `SELECT TIMESTAMP(${sqlString(startDate)})`
        }
      );

      ${
        ctx.incremental()
          ? `DELETE FROM ${ctx.self()}
            WHERE event_date >= DATE(identity_rebuild_checkpoint)
              AND occurred_at >= identity_rebuild_checkpoint;`
          : ``
      }
      `;
        }

/* Generate pre-operations for the temporary input tables used before recursive
   identity walks.

   Recursive SQL is executed in a standard table rather than directly inside an
   incremental MERGE model. To keep each run efficient, the recursive walk
   should process only the current incremental rebuild scope rather than the
   full event history.

   Dataform can determine whether a run is incremental only within a model
   configured as incremental. This temporary input model therefore:
     1. calculates the rebuild checkpoint using the same overlap logic as the
        durable incremental state tables;
     2. clears its existing contents on each incremental run; and
     3. repopulates itself only with the events that fall within the current
        rebuild scope.

   The resulting scoped input table is then read by the standard recursive-run
   table. */
   
    function recursiveWalkInputScopePreOps(ctx) {
      return `
        DECLARE identity_rebuild_checkpoint TIMESTAMP DEFAULT (
          ${
            ctx.incremental()
              ? `SELECT TIMESTAMP_SUB(
                    TIMESTAMP(
                      DATE_SUB(
                        COALESCE(MAX(event_date), DATE(${sqlString(startDate)})),
                        INTERVAL 1 DAY
                      )
                    ),
                    INTERVAL ${identityIncrementalRebuildHours} HOUR
                  )
                  FROM ${ctx.self()}`
              : `SELECT TIMESTAMP(${sqlString(startDate)})`
          }
        );

    ${
      ctx.incremental()
        ? `DELETE FROM ${ctx.self()} WHERE TRUE;`
        : ``
    }
  `;
}
 
/* --------------------------------------------------------------------------

  2A. Prepare identity evidence: build all-time trusted anchors 
 
  Create the table of trusted links between: 
  - an identity-bearing request
  - the anonymous user identifier observed on that request (AUID)
  - the known authenticated user identifier extracted from the configured 
    source data (IUID). 
  
  This model is rebuilt across the full valid source data period rather than 
  incrementally because historical anchors affect the all time risk classification 
  of each AUID in the next stage. 
  
  Grain: One row per identity bearing request UUID. 
  
  Output: identity_anchors_all_[event source] 

------------------------------------------------------------------------------ */

  publish(
    anchorsAllName,
    {
      ...params.defaultConfig,
      schema: "web_analytics_staging_tables",
      type: "table",
      tags: [
        params.eventSourceName.toLowerCase(),
        "identity-staging"
      ],
      description: `All-time trusted identity anchors used by the web request identity-resolution pipeline.

        Each row represents an identity-bearing request that provides a trusted link between
        an anonymous user identifier (AUID) and an authenticated inferred user identifier (IUID).

        Grain: one row per identity-bearing request UUID.`,
      dependencies: params.dependencies,
      columns: {
        request_uuid: `Unique identifier for the identity bearing request. This is the grain of the table.`,
        auid: `Anonymous user identifier observed on the identity-bearing request. Derived from anonymised_user_agent_and_ip.`,
        event_date: `Calendar date on which the identity bearing request occurred. Derived from occurred_at and used to partition the table.`,
        occurred_at: `Timestamp at which the identity bearing request occurred.`,
        iuid: `Known inferred user identifier extracted from the configured trusted identity anchor source. Used as trusted identity evidence in downstream propagation.`
      },
      bigquery: {
        partitionBy: "event_date",
        clusterBy: ["auid", "iuid"]
      }
    }
  ).query(ctx => `

  WITH

  events_base AS (
    SELECT *
    FROM ${ctx.ref(eventsTableName)}
    WHERE occurred_at >= TIMESTAMP(${sqlString(startDate)})
      AND anonymised_user_agent_and_ip IS NOT NULL
  ),

  raw_anchors AS (
    ${sqlAnchorSourceQueries(anchorSources)}
  ),

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
  )

  SELECT *
  FROM anchors

  `);

  /* --------------------------------------------------------------------------
    2B. Prepare identity evidence: classify all-time AUID identity state 
    
    Create an all-time identity-risk profile for each anonymous user identifier 
    (AUID) observed in valid web request activity. This model determines whether 
    activity associated with an AUID can: 
      - be assigned directly to a known inferred user identifier (IUID)
      - remain anonymous because no trusted identity evidence exists
      - enter the conservative recursive identity-propagation stage. 
      
    This model is rebuilt across the full valid source data period rather than 
    incrementally because later activity can change the risk classification of 
    an AUID. For example, an AUID previously linked to one IUID may later be 
    observed with a second IUID and must then be treated as high risk. 
    
    Grain: One row per AUID observed in web request activity. 
    
    Output: identity_auid_state_[event source]
   -------------------------------------------------------------------------- */

    publish(
      auidStateName,
      {
        ...params.defaultConfig,
        schema: "web_analytics_staging_tables",
        type: "table",
        tags: [
          params.eventSourceName.toLowerCase(),
          "identity-staging"
        ],
        description: `
          All time AUID identity risk classification used by the web request identity resolution pipeline.

          Each row summarises the trusted identity history associated with one anonymous user identifier
          (AUID) and determines whether its events can be assigned directly, left anonymous, or passed
          into the recursive identity-propagation stages.

          Grain: one row per AUID observed with web request activity.
        `,
        dependencies: params.dependencies,
        columns: {
          auid: `Anonymous user identifier observed in web request activity. Derived from anonymised_user_agent_and_ip and used as the grain of the table.`,
          distinct_iuid_count_ever: `Number of distinct trusted inferred user identifiers historically linked to the AUID.`,
          single_known_iuid: `Trusted inferred user identifier linked to the AUID where the AUID has exactly one known identity. Used for direct assignment of low risk exclusive AUIDs.`,
          auid_risk_classification: `All time identity risk classification for the AUID. Determines whether its events can be assigned directly, left anonymous, or passed into recursive identity propagation.`,
          requires_walk: `Boolean indicating whether events associated with the AUID require deeper recursive identity inference.`
        },
        bigquery: {
          clusterBy: ["auid", "auid_risk_classification"]
        }
      }
    ).query(ctx => `

    WITH

    /* Restrict the source data to the valid processing period and remove events without
    an AUID, as these cannot be assigned an inferred identity. */

    events_base AS (
      SELECT *
      FROM ${ctx.ref(eventsTableName)}
      WHERE occurred_at >= TIMESTAMP(${sqlString(startDate)})
        AND anonymised_user_agent_and_ip IS NOT NULL
    ),

    /* Create a table of web requests only, as the identity inference code is only intended to
    infer and propagate an identity for these events. Other event types may provide trusted identity 
    anchors. */

    web_events_base AS (
      SELECT *
      FROM events_base
      WHERE event_type = 'web_request'
    ),

    /* Retain every AUID observed in web request activity, including public-only
      visitors, so every request can receive an AUID risk classification.

      Grain:
        One row per AUID observed in web request activity. */
    all_web_request_auids AS (
      SELECT DISTINCT
        anonymised_user_agent_and_ip AS auid
      FROM web_events_base
    ),

    /* Load the trusted all-time AUID-to-IUID anchor evidence created in the
      preceding model. */
    anchors AS (
      SELECT *
      FROM ${ctx.ref(anchorsAllName)}
    ),

    /* Identify dates containing meaningful service activity.

      Public landing pages and authentication-transition pages are excluded when
      testing for unanchored activity. Visiting these pages without generating a
      trusted anchor is not strong evidence that an anchored AUID was used by a
      different person.

      Grain:
        One row per AUID and meaningful active date. */
    auid_meaningful_active_dates AS (
      SELECT DISTINCT
        anonymised_user_agent_and_ip AS auid,
        DATE(occurred_at) AS event_date
      FROM web_events_base
      WHERE ${sqlNotInList("request_path", publicAndAuthPagePaths)}
    ),

    /* Identify each calendar date on which an AUID has trusted anchor evidence.

      Grain:
        One row per anchored AUID and date. */
    auid_anchor_dates AS (
      SELECT DISTINCT
        auid,
        event_date
      FROM anchors
    ),

    /* Summarise the complete trusted anchor history for each AUID.

      single_known_iuid is used downstream only where the AUID has exactly one
      distinct known IUID. The risk classification below enforces that condition.

      Grain:
        One row per anchored AUID. */
    auid_anchor_summary AS (
      SELECT
        auid,
        COUNT(DISTINCT iuid) AS distinct_iuid_count_ever,
        MIN(iuid) AS single_known_iuid
      FROM anchors
      GROUP BY auid
    ),

    /* Combine all observed AUIDs with meaningful activity dates and trusted anchor
      evidence.

      Public-only AUIDs remain in the output, but public and authentication-page
      visits do not count as evidence of unanchored service activity.

      meaningful_active_dates_without_trusted_anchor is used only to calculate the
      risk classification and is not persisted.

      Grain:
        One row per AUID observed in web request activity. */
    auid_classification_inputs AS (
      SELECT
        base.auid,

        IFNULL(MAX(s.distinct_iuid_count_ever), 0)
          AS distinct_iuid_count_ever,

        ANY_VALUE(s.single_known_iuid)
          AS single_known_iuid,

        COUNTIF(
          meaningful.event_date IS NOT NULL
          AND anchor_date.auid IS NULL
        ) AS meaningful_active_dates_without_trusted_anchor

      FROM all_web_request_auids base

      LEFT JOIN auid_meaningful_active_dates meaningful
        ON base.auid = meaningful.auid

      LEFT JOIN auid_anchor_dates anchor_date
        ON meaningful.auid = anchor_date.auid
      AND meaningful.event_date = anchor_date.event_date

      LEFT JOIN auid_anchor_summary s
        ON base.auid = s.auid

      GROUP BY base.auid
    ),

    /* Assign each AUID to a risk class and determine whether its events require
      deeper recursive identity inference.

      LOW_RISK_EXCLUSIVE_ANCHORED_AUID means:
        - the AUID has only ever been linked to one IUID; and
        - every meaningful service-activity date contains trusted anchor evidence.

      Public landing-page and authentication-transition activity does not count as
      evidence of unanchored service use.

      Anonymous-only AUIDs remain unresolved because no trusted identity evidence
      is available to propagate.

      Grain:
        One row per AUID observed in web request activity. */
    classified_auids AS (
      SELECT
        auid,
        distinct_iuid_count_ever,
        single_known_iuid,

        CASE
          WHEN distinct_iuid_count_ever = 0
            THEN 'NO_KNOWN_IUID_ANONYMOUS_ONLY'

          WHEN distinct_iuid_count_ever = 1
            AND meaningful_active_dates_without_trusted_anchor = 0
            THEN 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'

          WHEN distinct_iuid_count_ever = 1
            AND meaningful_active_dates_without_trusted_anchor > 0
            THEN 'UNCERTAIN_SINGLE_IUID_UNANCHORED_ACTIVITY'

          WHEN distinct_iuid_count_ever > 1
            THEN 'HIGH_RISK_MULTI_IUID_AUID'

          ELSE 'UNKNOWN'
        END AS auid_risk_classification,

        CASE
          WHEN distinct_iuid_count_ever = 0
            THEN FALSE

          WHEN distinct_iuid_count_ever = 1
            AND meaningful_active_dates_without_trusted_anchor = 0
            THEN FALSE

          ELSE TRUE
        END AS requires_walk

      FROM auid_classification_inputs
    )

    SELECT *
    FROM classified_auids

    `);

/* -------------------------------------------------------------------------- 
  3. Classify events and apply direct identity assignments 
  
  Create the initial event level identity-resolution state. For each valid web request event: 
  - attach the identity risk profile for its AUID 
  - identify whether the request is a trusted identity anchor
  - assign a known IUID to trusted anchor events 
  - directly assign activity on low-risk exclusive AUIDs 
  - leave anonymous or uncertain activity unresolved for later stages. 
  
  This is an incremental model. Each run rebuilds an overlapping recent period so that identity 
  chains, activity windows, and downstream sessions crossing the incremental boundary can be 
  reconstructed consistently. 
  
  Grain: One row per valid web request event. 
  
  Output: identity_classify_events_[event source] 
------------------------------------------------------------------------------ */

publish(
  classifyEventsName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "incremental",
    protected: true,
    uniqueKey: ["request_uuid", "occurred_at"],
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Initial event level identity resolution state for valid web request events.

      Each row combines the original request attributes with trusted anchor evidence and the
      AUID identity risk classification. Trusted anchor requests receive their known
      identity, while events on low risk exclusive AUIDs receive a direct inferred assignment.

      Grain: one row per valid web request event.`,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web request event. This is the grain of the table.`,
      occurred_at: `Timestamp at which the web request event occurred.`,
      event_date: `Calendar date on which the web request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web request events only.`,
      auid: `Anonymous user identifier observed on the web request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity anchor source where available.`,
      auid_risk_classification: `All time identity-risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      distinct_iuid_count_ever: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      current_iuid: `Best currently available inferred user identifier after applying trusted anchor and low risk direct assignment rules.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.preOps(ctx => identityEventCheckpointPreOps(ctx))
.query(ctx => `


WITH

/* Load events within the current incremental rebuild scope. Events without 
an AUID are excluded because they cannot be assigned or used to propagate an 
inferred identity. */

  events_base AS (
    SELECT *
    FROM ${ctx.ref(`events_${params.eventSourceName}`)}
    WHERE occurred_at >= identity_rebuild_checkpoint
      AND occurred_at >= TIMESTAMP(${sqlString(startDate)})
      AND anonymised_user_agent_and_ip IS NOT NULL
  ),

/* Restrict the output to web request activity. Other event types may provide 
trusted anchor evidence upstream, but the identity inference output is intended 
to resolve web request events only. */

  web_events_base AS (
    SELECT *
    FROM events_base
    WHERE event_type = 'web_request'
  ),

/* Load the all time trusted AUID to IUID anchor evidence created in stage 2A. */

  anchors AS (
    SELECT *
    FROM ${ctx.ref(anchorsAllName)}
  ),

/* Load the all time trusted AUID risk classifications created in stage 2B. */

  classified_auids AS (
    SELECT *
    FROM ${ctx.ref(auidStateName)}
  ),

/* Attach trusted anchor evidence and AUID risk classifications to each request, 
then create the initial identity resolution state. Trusted anchor events receive 
their known authenticated IUID. 

Non-anchor events on low risk exclusive AUIDs are assigned directly because: 
  - the AUID has only ever been linked to one known IUID
  - every active date for the AUID contains trusted anchor evidence

Other events remain unresolved so later stages can apply navigation
chain and activity window rules. 

Grain: One row per valid web request event. */

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

    /* Identify requests that contain trusted authenticated identity evidence. */
    IF(a.request_uuid IS NOT NULL, TRUE, FALSE) AS is_trusted_identity_anchor,

    /* Known authenticated identity extracted from the trusted anchor source. */
    a.iuid AS known_anchor_iuid,

    /* AUID risk classification and supporting audit metrics. */
    c.auid_risk_classification,
    c.requires_walk,
    c.distinct_iuid_count_ever,

    /* Assign identities inferred through the safe no walk rule. 
    
    Known anchor identities are excluded because they are observed 
    facts rather than inferred values. */

    /* Store the best currently available identity. Priority at this stage: 
    1. known trusted anchor IUID
    2. safe direct assignment for a low-risk exclusive AUID
    3. unresolved */

    CASE
      WHEN a.iuid IS NOT NULL
        THEN a.iuid

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN c.single_known_iuid

      ELSE NULL
    END AS current_iuid,


    /* Record the stage that assigned the current identity.

       Leave this NULL where no identity has yet been assigned. */
    CASE
      WHEN a.iuid IS NOT NULL
        THEN 'PART_1_KNOWN_TRUSTED_ANCHOR'

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 'PART_1_NO_WALK_ASSIGNMENT'

      ELSE NULL
    END AS current_resolution_stage,

    /* Record the rule that assigned the current identity.

       Leave this NULL where no identity has yet been assigned. */
    CASE
      WHEN a.iuid IS NOT NULL
        THEN 'KNOWN_TRUSTED_IDENTITY_ANCHOR_EVENT'

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 'NO_WALK_INFERRED_FROM_EXCLUSIVE_SINGLE_IUID_AUID'

      ELSE NULL
    END AS current_iuid_method,

    /* Express the relative strength of the current identity assignment. 
    Known anchor evidence receives the highest priority. Safe no walk assignments 
    remain strong but are scored lower than anchor requests identities. */

    CASE
      WHEN a.iuid IS NOT NULL
        THEN 100

      WHEN c.auid_risk_classification = 'LOW_RISK_EXCLUSIVE_ANCHORED_AUID'
        THEN 80

      ELSE 0
    END AS identity_resolution_priority,

  /* Create a field that lock trusted anchor identities so later inference stages 
  cannot overwrite them. */
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
   4A. Conservative recursive propagation: define recursive walk input scope

   Create the event subset that will be passed into the conservative recursive
   identity propagation model.

   This model exists as a separate incremental input table because the recursive
   walk itself must run in a standard Dataform table. A standard table cannot use
   Dataform's incremental run context to decide whether it should process the full 
   valid event history or only the overlapping rebuild period required for the 
   current incremental run.

   This incremental input model therefore acts as the boundary between the
  event state created in Stage 3 and the standard recursive run table
   created in Stage 4B.

   On a full refresh:
     1. `identity_rebuild_checkpoint` is set to the configured pipeline start date;
     2. all valid Stage 3 events from that date onwards are included.

   On an incremental run:
     1. recursiveWalkInputScopePreOps() calculates an overlapping rebuild
       checkpoint from the latest date already stored in this input table
     2. the checkpoint is moved backwards by one day and the configured additional
       rebuild hours
     3. the existing contents of this temporary input table are cleared
     4. only Stage 3 events occurring on or after the checkpoint are reloaded.

   The overlapping rebuild period is intentional. It ensures that identity
   chains crossing an incremental run boundary can be reconstructed consistently
   rather than being split between separate runs.

   The standard recursive run table in Stage 4B reads this scoped input table and
   performs the recursive walk only on the required event subset. The resulting
   identities are later persisted back into the durable incremental state table.

   Grain:
   One row per valid web request event within the current recursive walk scope.
   A web request event is uniquely identified by the combination of request_uuid
   and occurred_at.

   Output:
     identity_conservative_recursive_walk_input_[event source]
------------------------------------------------------------------------------ */

publish(
  conservativeWalkInputName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "incremental",
    protected: false,
    uniqueKey: ["request_uuid", "occurred_at"],
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Scoped input table for the conservative recursive identity walk.

      Each run reloads the overlapping rebuild period from the initial event-level
      identity resolution state. This allows the downstream recursive walk to run
      as a standard table while still limiting processing to the required rebuild scope.

      Grain: one row per valid web request event within the current recursive walk scope.
    `,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web request event. This is the grain of the table.`,
      occurred_at: `Timestamp at which the web request event occurred.`,
      event_date: `Calendar date on which the web request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web request events only.`,
      auid: `Anonymous user identifier observed on the web request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity anchor source where available.`,
      auid_risk_classification: `All-time identity risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      distinct_iuid_count_ever: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      current_iuid: `Best currently available inferred user identifier after applying trusted-anchor and low risk direct assignment rules.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.preOps(ctx => recursiveWalkInputScopePreOps(ctx))
.query(ctx => `

SELECT *
FROM ${ctx.ref(classifyEventsName)}
WHERE occurred_at >= identity_rebuild_checkpoint

`);

/* --------------------------------------------------------------------------
   4B. Conservative recursive identity propagation: run scoped recursive walk

   Attempt to assign identities to unresolved events by constructing
   conservative navigation chains and tracing those chains back to trusted or
   safely pre-assigned identity anchors.

   This model runs as a standard table rather than an incremental table because
   it uses WITH RECURSIVE. Stage 4A has already detected whether the pipeline is
   running incrementally and restricted the input to the required overlapping
   rebuild scope.

   The walk follows a conservative ruleset:
     - explicit referrer relationships are preferred
     - same-AUID relationships receive the highest confidence
     - selected cross-AUID relationships are permitted only where AUIDs share a
       lightweight shared context proxy key
     - selected short gap fallback rules are permitted only for single-IUID
       AUIDs
     - trusted anchors normally form chain roots
     - a POST trusted anchor may attach backwards to its exact same-AUID
       referrer parent where it represents an identity bearing form submission.

   The shared context proxy improves recall where organisational networks cause
   AUID changes during a journey. It does not directly assign identity.

   An unresolved event receives an IUID only where its complete chain contains
   exactly one distinct anchor IUID. Chains with no anchor or conflicting anchor
   IUIDs remain unresolved.

   Successful assignments that depend on the shared context proxy are recorded
   separately in current_iuid_method for QA.

   This model also identifies likely shunt arrivals for use when activity window
   boundaries are built in Stage 5. A shunt refers to an instance where a new users
   activity chain arrives on an AUID that is not directly connected to an identity
   anchor.

   Grain:
     One row per valid web request event within the current recursive walk scope.

   Input:
     identity_conservative_recursive_walk_input_[event source]

   Output:
     identity_conservative_recursive_walk_run_[event source]
------------------------------------------------------------------------------ */

publish(
  conservativeWalkRunName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "table",
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Scoped output from the conservative recursive identity walk.

      Each row retains the event-level identity state and records the result of tracing
      conservative navigation chains back to trusted anchors or safely pre-assigned identities.
      Previously unresolved events receive an inferred identity only where their complete chain
      resolves safely to exactly one anchor IUID.

      Grain: one row per valid web request event within the current recursive walk scope.`,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web-request event. Together with occurred_at, this forms the grain of the table.`,
      occurred_at: `Timestamp at which the web-request event occurred. Together with request_uuid, this forms the grain of the table.`,
      event_date: `Calendar date on which the web request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web request events only.`,
      auid: `Anonymous user identifier observed on the web request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity anchor source where available.`,
      auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      distinct_iuid_count_ever: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
      parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
      parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
      likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary downstream.`,
      current_iuid: `Best currently available inferred user identifier after applying trusted-anchor, low-risk direct-assignment, and conservative recursive-propagation rules.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier, including whether recursive propagation depended on a shared-context proxy.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.query(ctx => `

WITH RECURSIVE

/* -------------------------------------------------------------------------- 
4B.1. Load scoped events and define trusted identity anchors 

Load the Stage 3 event state for the current rebuild scope. 

Trusted anchors contain known authenticated IUIDs and seed the recursive 
propagation logic. 
------------------------------------------------------------------------------ */

events_with_part_1_identity AS (
  SELECT *
  FROM ${ctx.ref(conservativeWalkInputName)}
),

anchors_all AS (
  SELECT DISTINCT
    request_uuid,
    occurred_at,
    known_anchor_iuid AS inferred_user_id,
    auid,
    TRUE AS is_anchor
  FROM events_with_part_1_identity
  WHERE is_trusted_identity_anchor = TRUE
    AND known_anchor_iuid IS NOT NULL
    AND auid IS NOT NULL
),

/* -------------------------------------------------------------------------- 
4B.2. Create the shared-context proxy 

Assign each anchored AUID a lightweight proxy key using the alphabetically 
first trusted IUID observed on that AUID. 

This methodology is explained below.
------------------------------------------------------------------------------ */

/* 
   The following CTE (auid_shared_context_proxy_map) creates a lightweight 
   shared context proxy for each anchored AUID.

   Organisational networks can cause the AUID to change during an
   otherwise continuous journey. Different AUIDs may therefore represent the
   same user journey in the same organisational context while having been 
   observed in the data for overlapping but non identical sets of users.

   Constructing a comprehensive organisational device graph requires more
   complex and expensive many-to-many matching. This was attempted but greatly increased
   the processing time and cost, whilst providing only marginal improvements. Therefore,
   this stage uses a deliberately lightweight identifier: each AUID is assigned a stable
   proxy key using the alphabetically first trusted IUID observed on that AUID.

   Matching proxy keys do not prove that two AUIDs belong to the same device,
   network, or organisation. The proxy is known to be incomplete and may miss
   valid relationships where overlapping AUIDs reduce to different keys.

   The proxy is used only to improve recall when generating selected
   medium confidence navigation candidates. It does not directly assign an
   identity. Final identity propagation still requires the resulting chain to
   resolve safely to exactly one anchor IUID.

   The proxy match source is in the QA output so its contribution and
   accuracy can be evaluated separately. */

auid_shared_context_proxy_map AS (
  SELECT
    auid,
    MIN(inferred_user_id) AS auid_shared_context_proxy_key
  FROM anchors_all
  GROUP BY auid
),

/* -------------------------------------------------------------------------- 
4B.3. Restrict nearby supporting events using merged time-window islands 

Include every event requiring a walk. 

Load resolved GET and trusted-anchor events only where they occur within 
±120 minutes of relevant walk-target activity. Merge overlapping ranges into 
islands before matching supporting events to avoid a many to many timestamp 
join explosion.

Testing found that this optimisation allowed this code to run on ~5x 
the number of web_requests (ITT Mentors Service Data).
------------------------------------------------------------------------------ */

walk_target_proxy_times AS (
  SELECT DISTINCT
    c.auid_shared_context_proxy_key,
    e.occurred_at AS target_occurred_at
  FROM events_with_part_1_identity e
  LEFT JOIN auid_shared_context_proxy_map c
    ON e.auid = c.auid
  WHERE e.requires_walk = TRUE
    AND c.auid_shared_context_proxy_key IS NOT NULL
),

target_ranges AS (
  SELECT
    auid_shared_context_proxy_key,
    TIMESTAMP_SUB(target_occurred_at, INTERVAL 120 MINUTE) AS range_start,
    TIMESTAMP_ADD(target_occurred_at, INTERVAL 120 MINUTE) AS range_end
  FROM walk_target_proxy_times
),

ordered_ranges AS (
  SELECT
    *,
    MAX(range_end) OVER (
      PARTITION BY auid_shared_context_proxy_key
      ORDER BY range_start, range_end
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_max_range_end
  FROM target_ranges
),

range_breaks AS (
  SELECT
    *,
    CASE
      WHEN previous_max_range_end IS NULL THEN 1
      WHEN range_start > previous_max_range_end THEN 1
      ELSE 0
    END AS new_island_flag
  FROM ordered_ranges
),

range_islands AS (
  SELECT
    *,
    SUM(new_island_flag) OVER (
      PARTITION BY auid_shared_context_proxy_key
      ORDER BY range_start, range_end
      ROWS UNBOUNDED PRECEDING
    ) AS island_id
  FROM range_breaks
),

target_window_islands AS (
  SELECT
    auid_shared_context_proxy_key,
    island_id,
    MIN(range_start) AS window_start,
    MAX(range_end) AS window_end,
    COUNT(*) AS merged_target_count
  FROM range_islands
  GROUP BY
    auid_shared_context_proxy_key,
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
          OR e.is_trusted_identity_anchor = TRUE
        )
        AND EXISTS (
          SELECT 1
          FROM target_window_islands w
          WHERE w.auid_shared_context_proxy_key = c.auid_shared_context_proxy_key
            AND e.occurred_at BETWEEN w.window_start AND w.window_end
        )
        THEN 'SUPPORTING_RESOLVED_PARENT_SHARED_CONTEXT_PROXY_WITHIN_2H'

      ELSE 'EXCLUDED'
    END AS part_2_inclusion_reason

  FROM events_with_part_1_identity e
  LEFT JOIN auid_shared_context_proxy_map c
    ON e.auid = c.auid

  WHERE e.requires_walk = TRUE
     OR (
       e.current_iuid IS NOT NULL
       AND (
         e.request_method = 'GET'
         OR e.is_trusted_identity_anchor = TRUE
       )
       /* Bring in pre-assigned events that could be chained to */
       AND EXISTS (
         SELECT 1
         FROM target_window_islands w
         WHERE w.auid_shared_context_proxy_key = c.auid_shared_context_proxy_key
           AND e.occurred_at BETWEEN w.window_start AND w.window_end
       )
     )
),

/* -------------------------------------------------------------------------- 
4B.4. Prepare walk anchors and navigable parent events 

Mark trusted anchors and nearby directly assigned events as walk anchors. Treat 
non-redirect GET requests as navigable parents. These events may form links in 
the reconstructed navigation chain. Create hourly buckets to reduce the search 
space for later referrer joins. 
------------------------------------------------------------------------------ */

events_supp AS (
  SELECT
    e.*,

    CASE
      WHEN a.is_anchor = TRUE
        THEN TRUE

      WHEN e.part_2_inclusion_reason =
        'SUPPORTING_RESOLVED_PARENT_SHARED_CONTEXT_PROXY_WITHIN_2H'
        AND e.current_iuid IS NOT NULL
        THEN TRUE

      ELSE FALSE
    END AS walk_is_anchor,

    CASE
      WHEN a.is_anchor = TRUE
        THEN a.inferred_user_id

      WHEN e.part_2_inclusion_reason =
        'SUPPORTING_RESOLVED_PARENT_SHARED_CONTEXT_PROXY_WITHIN_2H'
        AND e.current_iuid IS NOT NULL
        THEN e.current_iuid

      ELSE NULL
    END AS walk_anchor_iuid,

    CASE
      WHEN a.is_anchor = TRUE
        THEN 'TRUSTED_IDENTITY_ANCHOR'

      WHEN e.part_2_inclusion_reason =
        'SUPPORTING_RESOLVED_PARENT_SHARED_CONTEXT_PROXY_WITHIN_2H'
        AND e.current_iuid IS NOT NULL
        THEN 'LOCAL_DIRECT_ASSIGNMENT_ANCHOR'

      ELSE NULL
    END AS walk_anchor_type,

    c.auid_shared_context_proxy_key,

    (
      e.request_method = 'GET'
      AND SAFE_CAST(e.response_status AS STRING)
        NOT IN ('301', '302', '303', '307', '308')
    ) AS is_navigable_parent,

    TIMESTAMP_TRUNC(e.occurred_at, HOUR) AS hour_bucket

  FROM events_base e
  LEFT JOIN anchors_all a
    ON e.request_uuid = a.request_uuid
    AND e.occurred_at = a.occurred_at 
  LEFT JOIN auid_shared_context_proxy_map c
    ON e.auid = c.auid
),

/* -------------------------------------------------------------------------- 
4B.5. Locate the nearest preceding same-AUID parent 

Sequence events chronologically within each AUID and identify the nearest 
earlier navigable parent or walk anchor. 

This supports tightly constrained fallback rules where an explicit referrer 
is missing or unhelpful. 
------------------------------------------------------------------------------ */

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

/* -------------------------------------------------------------------------- 
4B.6. Build explicit referrer parent candidates 

Match each child referrer path to an earlier parent path and query within a 
120 minute lookback period. 

Confidence: 
- HIGH: child and parent share the same AUID 
- MEDIUM: child and parent share a non null shared context proxy key. 

Hourly buckets narrow the initial join before exact timestamp rules are applied. 
------------------------------------------------------------------------------ */

parent_lookup AS (
  SELECT
    request_uuid,
    occurred_at,
    request_path,
    request_path_and_query,
    auid,
    auid_shared_context_proxy_key,
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
    child.auid_shared_context_proxy_key,
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
      WHEN child.auid = parent.auid
        THEN 'HIGH'

      ELSE 'MEDIUM'
    END AS match_confidence,

    CASE
      WHEN child.auid = parent.auid
        THEN 'STAGE_4_REFERRER_SAME_AUID'

      ELSE 'STAGE_4_REFERRER_SHARED_CONTEXT_PROXY'
    END AS match_source

  FROM referrer_child_buckets child
  JOIN parent_lookup parent
    ON child.request_referer_path_and_query = parent.request_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND NOT (
      parent.request_uuid = child.request_uuid
      AND parent.occurred_at = child.occurred_at
    )
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND (
     child.auid = parent.auid
     OR (
       child.auid_shared_context_proxy_key IS NOT NULL
       AND child.auid_shared_context_proxy_key = parent.auid_shared_context_proxy_key
     )
   )
),

/* -------------------------------------------------------------------------- 
4B.7. Add constrained fallback parent candidates 

Add limited fallback links for cases where explicit referrer matching is not 
sufficient: 

1. Anchor bootstrap: Link the first nearby navigable child after a trusted anchor. 
2. Public landing-page referrer: For single-IUID AUIDs only, link to the nearest recent same-AUID parent. 
3. Null referrer: For single-IUID AUIDs only, link to the nearest recent same-AUID parent. 

These rules are restricted by AUID, time, and identity history to avoid joining unrelated journeys. 
------------------------------------------------------------------------------ */

anchor_next_trusted_anchor AS (
  SELECT
    request_uuid AS anchor_request_uuid,
    auid,
    occurred_at AS anchor_at,

    LEAD(occurred_at) OVER (
      PARTITION BY auid
      ORDER BY occurred_at, request_uuid
    ) AS next_trusted_anchor_at

  FROM events_supp
  WHERE walk_is_anchor = TRUE
    AND walk_anchor_type = 'TRUSTED_IDENTITY_ANCHOR'
    AND walk_anchor_iuid IS NOT NULL
    AND auid IS NOT NULL
),

p1_bootstrap_first_child_after_callback AS (
  SELECT
    anc.anchor_request_uuid,
    anc.auid,
    anc.anchor_at,
    anc.next_trusted_anchor_at,

    child.request_uuid AS child_request_uuid,
    child.occurred_at AS child_at

  FROM anchor_next_trusted_anchor anc
  JOIN events_supp child
    ON child.auid = anc.auid
   AND child.occurred_at > anc.anchor_at
   AND child.occurred_at <= TIMESTAMP_ADD(anc.anchor_at, INTERVAL 30 MINUTE)
   AND (anc.next_trusted_anchor_at IS NULL OR child.occurred_at < anc.next_trusted_anchor_at)
   AND child.walk_is_anchor = FALSE
   AND child.is_navigable_parent = TRUE
  WHERE child.request_referer_path_and_query IS NULL
     OR ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY anc.anchor_request_uuid, anc.anchor_at
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
    'STAGE_4_BOOTSTRAP_FIRST_EVENT_AFTER_TRUSTED_ANCHOR_SAME_AUID_30M' AS match_source
  FROM p1_bootstrap_first_child_after_callback
),

p1_slash_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'LOW' AS match_confidence,
    'STAGE_4_HOME_REFERRER_PREVIOUS_PARENT_BY_AUID_10M_SINGLE_IUID_ONLY' AS match_source

  FROM prev_by_auid child
  WHERE ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
    AND child.prev_parent_request_uuid_auid != child.request_uuid
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND ${sqlNotInList("child.prev_parent_request_path_auid", preAuthPagePaths)}
    AND child.distinct_iuid_count_ever = 1
),

p1_null_prev_by_auid_single_user AS (
  SELECT
    child.request_uuid AS child_request_uuid,
    child.prev_parent_request_uuid_auid AS parent_request_uuid,
    child.occurred_at AS child_at,
    child.prev_parent_occurred_at_auid AS parent_at,
    'MEDIUM' AS match_confidence,
    'STAGE_4_NULL_REFERRER_PREVIOUS_PARENT_BY_AUID_10M_SINGLE_IUID_ONLY' AS match_source

  FROM prev_by_auid child
  WHERE child.request_referer_path_and_query IS NULL
    AND child.distinct_iuid_count_ever = 1
    AND child.prev_parent_request_uuid_auid IS NOT NULL
    AND child.prev_parent_request_uuid_auid != child.request_uuid
    AND child.prev_parent_occurred_at_auid >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 10 MINUTE)
    AND ${sqlNotInList("child.prev_parent_request_path_auid", preAuthAndSignInPagePaths)}
),

/* -------------------------------------------------------------------------- 

4B.8. Select one parent per child 

Combine all candidate parent rules and retain the strongest available link. 
Prefer: 
  1. higher confidence 
  2. the most recent parent 
  3. request UUID as a deterministic tiebreaker. 
------------------------------------------------------------------------------ */

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
    child_at,
    best.parent_request_uuid,
    best.parent_at,
    best.match_confidence,
    best.match_source
  FROM (
    SELECT
      child_request_uuid,
      child_at,

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
    GROUP BY child_request_uuid, child_at
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
        AND bp.match_source = 'STAGE_4_REFERRER_SAME_AUID'
        THEN bp.parent_request_uuid

      WHEN e.walk_is_anchor
        THEN NULL

      ELSE bp.parent_request_uuid
    END AS parent_request_uuid_pass1,
    bp.parent_at AS parent_occurred_at_pass1,

    bp.match_confidence AS parent_match_confidence_pass1,
    bp.match_source AS parent_match_source_pass1,
    (bp.match_source = 'STAGE_4_REFERRER_SHARED_CONTEXT_PROXY') AS parent_match_used_shared_context_proxy_pass1

  FROM events_supp e
  LEFT JOIN p1_best_parent bp
    ON e.request_uuid = bp.child_request_uuid
    AND e.occurred_at = bp.child_at
),

/* -------------------------------------------------------------------------- 
4B.9. Recursively traverse navigation chains 

Walk each selected parent relationship backwards until any of the following conditions is met: 
- the chain reaches a root 
- no further parent exists
- the defensive maximum depth of 100 links is reached. This is a fallback to minimise the risk
of self referring chains creating an infinite loop. There are mitigations throughout the code
to minimise the risk of this, so this is only the final fallback.

Carry forward whether any edge in the full chain used the shared context proxy so 
proxy assisted assignments can be identified in current_iuid_method. 
------------------------------------------------------------------------------ */

walk_pass1 AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.occurred_at AS start_occurred_at,
    e.request_uuid AS current_request_uuid,
    e.occurred_at AS current_occurred_at,
    e.parent_request_uuid_pass1 AS parent_request_uuid,
    e.parent_occurred_at_pass1 AS parent_occurred_at,
    0 AS depth,

    e.parent_match_used_shared_context_proxy_pass1
      AS chain_used_shared_context_proxy

  FROM events_with_parent_pass1 e

  UNION ALL

  SELECT
    w.start_request_uuid,
    w.start_occurred_at,
    p.request_uuid AS current_request_uuid,
    p.occurred_at AS current_occurred_at,
    p.parent_request_uuid_pass1 AS parent_request_uuid,
    p.parent_occurred_at_pass1 AS parent_occurred_at,
    w.depth + 1 AS depth,

    (
      w.chain_used_shared_context_proxy
      OR p.parent_match_used_shared_context_proxy_pass1
    ) AS chain_used_shared_context_proxy

  FROM walk_pass1 w
  JOIN events_with_parent_pass1 p
    ON w.parent_request_uuid = p.request_uuid
    AND w.parent_occurred_at = p.occurred_at
  WHERE w.parent_request_uuid IS NOT NULL
    AND NOT (
      w.parent_request_uuid = w.current_request_uuid
      AND w.parent_occurred_at = w.current_occurred_at)
    AND w.depth < 100
),

/* -------------------------------------------------------------------------- 
4B.10. Collapse each chain and identify safe assignments 

Reduce the recursive output to one chain identifier per event. Count the distinct 
anchor IUIDs observed in each chain. A chain is eligible for identity propagation 
only where it contains exactly one anchor IUID. 
------------------------------------------------------------------------------ */

collapsed_pass1 AS (
  SELECT
    start_request_uuid AS request_uuid,
    start_occurred_at AS occurred_at,

    MAX_BY(
      STRUCT(current_request_uuid, current_occurred_at),
      IF(parent_request_uuid IS NULL, 1, 0)
    ) AS chain_id_pass1,

    LOGICAL_OR(chain_used_shared_context_proxy)
      AS chain_used_shared_context_proxy

  FROM walk_pass1
  GROUP BY start_request_uuid, start_occurred_at
),

chain_anchor_summary_pass1 AS (
  SELECT
    c.chain_id_pass1,

    COUNT(DISTINCT e.walk_anchor_iuid) AS chain_distinct_anchor_iuid_count,
    MIN(e.walk_anchor_iuid) AS chain_single_anchor_iuid

  FROM collapsed_pass1 c
  JOIN events_with_parent_pass1 e
    ON c.chain_id_pass1.current_request_uuid = e.request_uuid
   AND c.chain_id_pass1.current_occurred_at = e.occurred_at
  WHERE e.walk_is_anchor = TRUE
    AND e.walk_anchor_iuid IS NOT NULL
  GROUP BY c.chain_id_pass1
),

/* -------------------------------------------------------------------------- 

4B.11. Detect likely shunt arrivals for Stage 5 

Identify short cross-AUID transitions where a child referrer matches exactly 
one recent parent path. These events may indicate the start of a new device level 
journey. Persist the resulting boolean for use as a conservative activity-window boundary.
------------------------------------------------------------------------------ */

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
      PARTITION BY child.request_uuid, child.occurred_at
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
    child_at,
    TRUE AS likely_shunt_arrival
  FROM likely_shunt_arrival_candidates
  WHERE candidate_count_last_3h = 1
),

walked_events AS (
  SELECT
    e.request_uuid,
    e.occurred_at,

    e.parent_request_uuid_pass1,
    e.parent_match_confidence_pass1,

    cas.chain_distinct_anchor_iuid_count,
    cas.chain_single_anchor_iuid,

    COALESCE(c.chain_used_shared_context_proxy, FALSE)
      AS chain_used_shared_context_proxy,

    IFNULL(s.likely_shunt_arrival, FALSE)
      AS likely_shunt_arrival

  FROM events_with_parent_pass1 e
  LEFT JOIN collapsed_pass1 c
    ON e.request_uuid = c.request_uuid
  AND e.occurred_at = c.occurred_at
  LEFT JOIN chain_anchor_summary_pass1 cas
    ON c.chain_id_pass1 = cas.chain_id_pass1
  LEFT JOIN likely_shunt_arrivals s
    ON e.request_uuid = s.child_request_uuid
    AND e.occurred_at = s.child_at
)

/* -------------------------------------------------------------------------- 
4B.12. Update the event-level identity state 

Preserve identities assigned in Stage 3. Assign a previously unresolved event 
only where its recursive chain contains exactly one anchor IUID. Record whether 
the successful assignment depended on a shared context proxy link. Persist only 
fields required by Stage 5. Discard temporary graph diagnostics. 
------------------------------------------------------------------------------ */

  SELECT
    direct.* EXCEPT (
      current_iuid,
      current_resolution_stage,
      current_iuid_method,
      identity_resolution_priority
    ),

    /* Required by Stage 5 when constructing activity-window boundaries. */
    w.parent_request_uuid_pass1,
    w.parent_match_confidence_pass1,
    w.likely_shunt_arrival,

    CASE
      WHEN direct.current_iuid IS NOT NULL
        THEN direct.current_iuid

      WHEN direct.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN w.chain_single_anchor_iuid

      ELSE NULL
    END AS current_iuid,

    CASE
      WHEN direct.current_iuid IS NOT NULL
        THEN direct.current_resolution_stage

      WHEN direct.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 'STAGE_4_CONSERVATIVE_RECURSIVE_PROPAGATION'

      ELSE NULL
    END AS current_resolution_stage,

    CASE
      WHEN direct.current_iuid IS NOT NULL
        THEN direct.current_iuid_method

      WHEN direct.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        AND w.chain_used_shared_context_proxy = TRUE
        THEN 'STRICT_CHAIN_SINGLE_ANCHOR_IUID_WITH_SHARED_CONTEXT_PROXY'

      WHEN direct.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 'STRICT_CHAIN_SINGLE_ANCHOR_IUID_WITHOUT_SHARED_CONTEXT_PROXY'

      ELSE NULL
    END AS current_iuid_method,

    CASE
      WHEN direct.identity_resolution_locked = TRUE
        THEN 100

      WHEN direct.current_iuid IS NOT NULL
        THEN direct.identity_resolution_priority

      WHEN direct.requires_walk = TRUE
        AND w.chain_distinct_anchor_iuid_count = 1
        THEN 70

      ELSE 0
    END AS identity_resolution_priority

  FROM events_with_part_1_identity direct
  LEFT JOIN walked_events w
    ON direct.request_uuid = w.request_uuid
    AND direct.occurred_at = w.occurred_at

`);

/* --------------------------------------------------------------------------
   4C. Conservative recursive identity propagation: persist durable state

   Persist the scoped Stage 4B recursive walk output into the durable
   incremental event state table read by downstream models.

   Stage 4B runs as a standard table because it uses WITH RECURSIVE. That table
   contains only the current rebuild scope produced by Stage 4A. This Stage 4C
   model converts that scoped run output back into durable incremental state.

   On a full refresh:
     - rebuild the complete valid event history.

   On an incremental run:
     - calculate the overlapping rebuild checkpoint
     - delete the affected period from the existing durable state table
     - replace that period with the newly calculated Stage 4B results
     - retain historical rows outside the rebuild scope unchanged.

   This separation allows recursive SQL to be used without sacrificing the
   incremental behaviour required for production runs.

   Grain:
     One row per valid web request event.

   Input:
     identity_conservative_recursive_walk_run_[event source]

   Output:
     identity_conservative_recursive_walk_[event source]
------------------------------------------------------------------------------ */

publish(
  conservativeWalkName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "incremental",
    protected: true,
    uniqueKey: ["request_uuid", "occurred_at"],
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Durable event-level identity state after conservative recursive propagation.

      Each incremental run replaces the overlapping rebuild period with the latest scoped
      conservative-walk output while retaining historical rows outside that period.
      Previously unresolved events receive an inferred identity only where their complete
      navigation chain resolves safely to exactly one anchor IUID.

      Grain: one row per valid web-request event.`,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web-request event. This is the grain of the table.`,
      occurred_at: `Timestamp at which the web-request event occurred.`,
      event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web-request events only.`,
      auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity-anchor source where available.`,
      auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      distinct_iuid_count_ever: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
      parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
      parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
      likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary downstream.`,
      current_iuid: `Best currently available inferred user identifier after applying trusted-anchor, low-risk direct-assignment, and conservative recursive-propagation rules.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier, including whether recursive propagation depended on a shared-context proxy.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.preOps(ctx => identityEventCheckpointPreOps(ctx))
.query(ctx => `

SELECT *
FROM ${ctx.ref(conservativeWalkRunName)}

`);

/* --------------------------------------------------------------------------
   5. Build activity windows and apply identity fallback

   Attempt to assign identities to unresolved events using time bounded periods
   of high confidence activity.

   Stage 4 resolves events through conservative navigation chain propagation.
   Some events remain unresolved because they cannot be linked safely through a
   strict chain. Stage 5 applies a separate temporal fallback.

   The model:
     - builds activity windows from events that already have a trusted or
       high confidence IUID
     - applies shorter safety thresholds where an AUID has historically been
       associated with multiple IUIDs
     - detects overlapping or ambiguous periods;
     - assigns an IUID only where an unresolved event matches one clean,
       non overlapping window
     - retains a narrow set of window context fields required by the later
       repair walk.

   The model is incremental. An overlapping period is rebuilt on each run so
   windows crossing the incremental boundary can be reconstructed consistently.

   Grain:
     One row per valid web request event.

   Input:
     identity_conservative_recursive_walk_[event source]

   Output:
     identity_activity_windows_[event source]
------------------------------------------------------------------------------ */

  publish(
    activityWindowsName,
    {
      ...params.defaultConfig,
      schema: "web_analytics_staging_tables",
      type: "incremental",
      protected: true,
      uniqueKey: ["request_uuid", "occurred_at"],
      tags: [
        params.eventSourceName.toLowerCase(),
        "identity-staging"
      ],
      description: `Durable event-level identity state after applying activity-window fallback.

        Each incremental run rebuilds an overlapping recent period, constructs time-bounded
        identity windows from high-confidence activity, and assigns an identity to previously
        unresolved events only where the matching window evidence is clean and unambiguous.

        The model also retains a narrow set of activity-window context fields required by the
        targeted post-window repair walk.

        Grain: one row per valid web-request event.`,
      dependencies: params.dependencies,
      columns: {
        request_uuid: `Unique identifier for the web-request event. This is the grain of the table.`,
        occurred_at: `Timestamp at which the web-request event occurred.`,
        event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
        event_type: `Type of source event. This model retains web-request events only.`,
        auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
        request_user_id: `User identifier recorded directly on the incoming web request where available.`,
        request_path: `Path component of the requested URL.`,
        request_query: `Query-string component of the requested URL.`,
        request_path_and_query: `Combined request path and query string.`,
        request_referer_path_and_query: `Path and query string of the referring request where available.`,
        request_referer_domain: `Domain of the referring request where available.`,
        request_method: `HTTP method used for the web request.`,
        response_status: `HTTP response status cast to a string for consistent downstream handling.`,
        response_content_type: `Content type returned by the web request.`,
        entity_table_name: `Source entity table associated with the event where available.`,
        namespace: `Namespace associated with the source event where available.`,
        device_category: `Category of device associated with the web request.`,
        is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
        known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity-anchor source where available.`,
        auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
        requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
        identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
        parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
        parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
        likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary.`,
        auid_distinct_iuid_count: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID. Renamed at this stage to make the AUID-level meaning explicit.`,
        current_iuid: `Best currently available inferred user identifier after applying trusted-anchor, low-risk direct-assignment, conservative recursive-propagation, and activity-window fallback rules.`,
        current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
        current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
        identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
        matched_overlapping_windows: `Number of overlapping activity-window segments matched by the event. Used downstream to prevent unsafe repair assignments.`,
        matched_post_conflict_windows: `Number of post-identity-conflict activity-window segments matched by the event. Used downstream to prevent unsafe repair assignments.`,
        matched_unknown_preexisting_activity_windows: `Number of matched activity-window segments with unattributable pre-existing activity. Used downstream to prevent unsafe repair assignments.`,
        eligible_window_iuid: `Inferred user identifier associated with the event's eligible clean activity window where one is available.`,
        eligible_window_start: `Start timestamp of the event's eligible clean activity window where one is available.`,
        eligible_window_has_unknown_preexisting_activity: `Boolean indicating whether the eligible activity window contains unattributable pre-existing activity.`
      },
      bigquery: {
        partitionBy: "event_date",
        clusterBy: ["auid", "current_iuid"]
      }
    }
  )
  .preOps(ctx => identityEventCheckpointPreOps(ctx))
  .query(ctx => `


WITH

/* --------------------------------------------------------------------------
   5.1. Load current identity state and identify fallback candidates

   Load the Stage 4 identity state for the current overlapping rebuild
   scope.

   Identify:
     - navigable GET activity events, excluding redirects;
     - unresolved events eligible for activity window fallback.

   Only uncertain single IUID and high risk multi IUID AUIDs enter this fallback.
   Anonymous only AUIDs remain unresolved because no trusted identity evidence
   exists to propagate.
------------------------------------------------------------------------------ */

current_identity_resolution_state AS (
  SELECT *
  FROM ${ctx.ref(conservativeWalkName)}
  WHERE occurred_at >= identity_rebuild_checkpoint
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

/* --------------------------------------------------------------------------
   5.2. Compress resolved activity to minute-level points

  Resolved activity is grouped to minute level before constructing activity
  islands and identity windows.

  Grouping at minute level reduces the amount of data that needs processing
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

    MAX(distinct_iuid_count_ever) AS distinct_iuid_count_ever

  FROM events_base
  WHERE auid IS NOT NULL
    AND current_iuid IS NOT NULL
    AND (
      is_trusted_identity_anchor = TRUE
      OR is_navigable_activity_event = TRUE
      OR identity_resolution_priority >= 70
    )
  GROUP BY
    auid,
    current_iuid,
    activity_minute
),

/* --------------------------------------------------------------------------
   5.3. Group resolved activity into continuous identity islands

   Order resolved activity chronologically within each AUID.

   Start a new island where:
     - there is no previous resolved activity
     - the resolved IUID changes
     - the inactivity gap exceeds the permitted threshold.

   Thresholds:
     - 180 minutes for single-IUID AUIDs;
     - 10 minutes for multi-IUID AUIDs.

   The shorter threshold for multi-IUID AUIDs is deliberately conservative becase
   shared identifiers create a greater risk of joining separate user journeys.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.4. Create one initial window seed per resolved activity island

   Collapse each island into a window seed containing:
     - AUID
     - IUID
     - first high-confidence activity timestamp
     - final high-confidence activity timestamp
     - request UUID of the earliest activity point
     - all time AUID identity-count context.

   These are used to infer identity using event timings, they are not trusted identity anchors.
------------------------------------------------------------------------------ */

activity_window_seeds AS (
  SELECT
    auid,
    current_iuid AS iuid,

    MIN(first_occurred_at) AS window_start,
    MAX(last_occurred_at) AS last_high_conf_activity_at,

    ARRAY_AGG(
      first_request_uuid
      ORDER BY first_occurred_at, first_request_uuid
      LIMIT 1
    )[OFFSET(0)] AS anchor_request_uuid,

    MAX(distinct_iuid_count_ever) AS auid_distinct_iuid_count

  FROM resolved_activity_events_with_island_id
  GROUP BY
    auid,
    current_iuid,
    activity_island_id
),

/* --------------------------------------------------------------------------
   5.5. Calculate conservative window boundaries

   Derive signals that may limit how far an activity window can extend:
     - attributed sign-out
     - inactivity after the last high-confidence activity
     - next window associated with a different IUID
     - likely shunt arrival
     - pre-existing unattributable activity
     - absolute 24 hour maximum duration (Safety fallback)

   These boundaries are intentionally conservative. They limit the period in
   which unresolved activity may inherit an IUID from nearby resolved activity.
------------------------------------------------------------------------------ */

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

/* Calculate the earliest relevant boundary signal for each window seed.

   preexisting_unattributable_activity_10m identifies multi IUID AUIDs where
   unresolved navigable activity already existed shortly before the seed.
   These windows require stricter treatment because it implies that there was
   activity on the AUID before the window began. This adds ambiguity to which user the activity
   belongs to. This does not include pre-auth or sign-in pages. 
   
   */

activity_window_seed_boundaries AS (
  SELECT
    a.*,

    (
      SELECT MIN(a2.window_start)
      FROM activity_window_seeds a2
      WHERE a2.auid = a.auid
        AND a2.window_start > a.window_start
        AND a2.iuid != a.iuid
    ) AS next_different_iuid_window_start_at,

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
        /*
          Ignore public and authentication-transition pages when testing for prior
          unattributable activity, as these are not treated as ordinary journey activity.
        */
        AND ${sqlNotInList("e.request_path", publicAndAuthPagePaths)}
    ) AS preexisting_unattributable_activity_10m,

    TIMESTAMP_ADD(a.window_start, INTERVAL 24 HOUR) AS max_cap_at

  FROM activity_window_seeds a
),

/* --------------------------------------------------------------------------
   5.6. Derive candidate window ends

   For each seed, calculate:

     inactivity_tail_end_at:
       Last high-confidence activity plus the permitted inactivity threshold.

     chain_end_base:
       Initial activity-chain end, limited by sign-out, inactivity, and the
       absolute 24-hour cap.

     hard_end:
       Maximum permissible end after applying sign-out and the 24 hour limit.

     clean_end:
       End of the period that can be treated as safely attributable before a
       conflicting identity, shunt arrival, sign-out, inactivity timeout, or
       maximum-duration cap is reached.

   clean_end may be earlier than the full chain end where a strong end signal occures before
   a timeout.
------------------------------------------------------------------------------ */

window_end_candidates AS (
  SELECT
    a.*,

    TIMESTAMP_ADD(
      a.last_high_conf_activity_at,
      INTERVAL IF(a.auid_distinct_iuid_count = 1, 180, 10) MINUTE
    ) AS inactivity_tail_end_at,

    IF(a.auid_distinct_iuid_count = 1, 180, 10) AS inactivity_tail_minutes

  FROM activity_window_seed_boundaries a
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
      IFNULL(next_different_iuid_window_start_at, max_cap_at),
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

/* --------------------------------------------------------------------------
   5.7. Detect overlapping identity windows

   Split each AUID timeline into atomic intervals using window start and end
   timestamps.

   Count the distinct active IUIDs within each interval. Where more than one IUID
   is active, combine adjacent overlapping intervals into overlap components.

   Overlap indicates that an unresolved event cannot safely inherit one identity
   without further evidence.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.8. Extend overlapping periods using observed device activity

   Where windows overlap, inspect navigable device activity within the overlap
   component.

   Extend the chain end using the final relevant device activity plus the
   applicable inactivity threshold, while respecting the hard end and 24 hour
   maximum cap.

   This preserves the full ambiguous activity period so it is not incorrectly
   reclassified as clean activity immediately after the initial overlap.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.9. Segment windows and classify attribution quality

   Split windows into atomic time segments and calculate the number of active
   IUIDs in each segment.

   Assign one quality label:

     CLEAN:
       One attributable IUID and no known ambiguity.

     OVERLAPPING:
       More than one active IUID, or unresolved pre existing activity makes a
       multi IUID AUID unsafe.

     POST_IDENTITY_CONFLICT:
       The segment falls after the clean attribution boundary (and of the clean end rules is applied)
       but remains inside the wider activity chain

   Only CLEAN segments may assign an IUID automatically.
------------------------------------------------------------------------------ */

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
    w.next_different_iuid_window_start_at,
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
    next_different_iuid_window_start_at,
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

/* --------------------------------------------------------------------------
   5.10. Match unresolved candidates to activity windows efficiently

   Assign candidate events and activity windows to 15-minute buckets before
   applying exact timestamp predicates.

   The bucket join reduces the number of candidate window comparisons for
   high activity AUIDs without changing the final matching semantics.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.11. Summarise candidate window matches and apply the safety gate

   For each unresolved candidate event, count the matched windows and classify
   the result.

   An event is eligible for fallback assignment only where:
     - exactly one distinct clean IUID matches
     - at least one clean window matches
     - no non-clean window matches
     - no overlapping window matches
     - no post-conflict window matches
     - no unknown pre-existing activity window matches

   This prevents assignment where the temporal evidence is ambiguous.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.12. Apply AUID-risk-specific fallback rules

   Assign an eligible clean-window IUID where: the AUID has one historical IUID 
   but contains unanchored activity OR the AUID has multiple historical IUIDs, but 
   the matched window contains only one active IUID and no unknown pre-existing activity.

   This ensures that multi IUID AUIDs face an additional safety gate. It is important that the valid
   pre-sign in paths are all added correctly here or it will block multi-user iuid assignment where no known risk exists.
------------------------------------------------------------------------------ */

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

/* --------------------------------------------------------------------------
   5.13. Retain narrow context for the Stage 6 repair walk

   Preserve only the window context fields required by the later repair stage.

   Some unresolved events must retain this context even where Stage 5 does not
   assign an IUID. Stage 6 uses it to prevent repair assignments that conflict
   with activity window evidence or occur inside ambiguous periods.
------------------------------------------------------------------------------ */

window_context_for_downstream AS (
  SELECT
    request_uuid,

    eligible_window_iuid,
    eligible_window_start,

    matched_overlapping_windows,
    matched_post_conflict_windows,
    matched_unknown_preexisting_activity_windows,

    eligible_window_has_unknown_preexisting_activity

  FROM window_assignment_decision
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

    'STAGE_5_ACTIVITY_WINDOW_FALLBACK' AS applied_resolution_stage,

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
    is_near_window_start_post_auth_activity,
    CURRENT_TIMESTAMP() AS window_assignment_applied_at

  FROM window_assignment_decision
  WHERE should_apply_window_iuid = TRUE
),

/* --------------------------------------------------------------------------
   5.14. Update event-level identity state

   Preserve identities assigned in earlier stages.

   For previously unresolved events, apply the eligible activity window IUID
   where the Stage 5 safety gate succeeds.

   Update the three field identity audit interface only for successful
   assignments. Retain the narrow Stage 6 repair support context and discard
   temporary window diagnostics.
------------------------------------------------------------------------------ */

current_state_after_window_assignments AS (
  SELECT
    e.* EXCEPT (
      distinct_iuid_count_ever,
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
      identity_resolution_priority
    ),

/* Rename the all-time AUID identity-count field at the Stage 5 boundary.
   Downstream models use this clearer AUID-level name when applying safety
   thresholds. */
e.distinct_iuid_count_ever AS auid_distinct_iuid_count,

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

    CASE
      WHEN e.identity_resolution_locked = TRUE
        THEN 100

      WHEN e.current_iuid IS NOT NULL
        THEN e.identity_resolution_priority

      WHEN a.applied_iuid IS NOT NULL
        THEN 60

      ELSE 0
    END AS identity_resolution_priority,

    COALESCE(wc.matched_overlapping_windows, 0) AS matched_overlapping_windows,
    COALESCE(wc.matched_post_conflict_windows, 0) AS matched_post_conflict_windows,
    COALESCE(wc.matched_unknown_preexisting_activity_windows, 0) AS matched_unknown_preexisting_activity_windows,

    wc.eligible_window_iuid,
    wc.eligible_window_start,
    wc.eligible_window_has_unknown_preexisting_activity,

    FROM current_identity_resolution_state e
    LEFT JOIN window_context_for_downstream wc
      ON e.request_uuid = wc.request_uuid
    LEFT JOIN window_assignments_to_apply a
      ON e.request_uuid = a.request_uuid
)

SELECT *
FROM current_state_after_window_assignments

`);

/* --------------------------------------------------------------------------
   6A. Targeted post-window repair propagation: define recursive walk input scope

   Create the event subset that will be passed into the targeted post-window
   repair model.

   This model exists as a separate incremental input table because the repair
   walk itself uses WITH RECURSIVE and must therefore run in a standard Dataform
   table rather than inside an incremental MERGE model.

   Stage 5 has already:
     - applied clean activity-window fallback assignments where safe;
     - retained narrow window-context fields for unresolved events;
     - preserved the event-level identity state required by later repair logic.

   This Stage 6A input model acts as the boundary between that durable Stage 5
   state and the standard recursive repair-run table created in Stage 6B.

   On a full refresh:
     - identity_rebuild_checkpoint is set to the configured pipeline start date;
     - all valid Stage 5 events from that date onwards are included.

   On an incremental run:
     - recursiveWalkInputScopePreOps() calculates an overlapping rebuild
       checkpoint from the latest date already stored in this input table;
     - the checkpoint is moved backwards by one day and the configured additional
       rebuild hours;
     - the existing contents of this temporary input table are cleared;
     - only Stage 5 events occurring on or after the checkpoint are reloaded.

   The overlapping rebuild period is intentional. It ensures that repair walks,
   activity-window context, and downstream sessions crossing an incremental-run
   boundary can be reconstructed consistently.

   The standard recursive repair-run table in Stage 6B reads this scoped input
   table and performs the targeted post-window walk only on the required event
   subset.

   Grain:
     One row per valid web request event within the current repair-walk scope.

   Input:
     identity_activity_windows_[event source]

   Output:
     identity_second_recursive_walk_input_[event source]
------------------------------------------------------------------------------ */

  publish(
    secondWalkInputName,
    {
      ...params.defaultConfig,
      schema: "web_analytics_staging_tables",
      type: "incremental",
      protected: false,
      uniqueKey: ["request_uuid", "occurred_at"],
      tags: [
        params.eventSourceName.toLowerCase(),
        "identity-staging"
      ],
      description: `
        Scoped input table for targeted post-window repair propagation.

        Each run reloads the overlapping rebuild period from the activity-window identity state.
        This allows the downstream recursive repair walk to run as a standard table while limiting
        processing to the required rebuild scope.

        Grain: one row per valid web-request event within the current repair-walk scope.
      `,
      dependencies: params.dependencies,
      columns: {
        request_uuid: `Unique identifier for the web-request event. This is the grain of the table.`,
        occurred_at: `Timestamp at which the web-request event occurred.`,
        event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
        event_type: `Type of source event. This model retains web-request events only.`,
        auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
        request_user_id: `User identifier recorded directly on the incoming web request where available.`,
        request_path: `Path component of the requested URL.`,
        request_query: `Query-string component of the requested URL.`,
        request_path_and_query: `Combined request path and query string.`,
        request_referer_path_and_query: `Path and query string of the referring request where available.`,
        request_referer_domain: `Domain of the referring request where available.`,
        request_method: `HTTP method used for the web request.`,
        response_status: `HTTP response status cast to a string for consistent downstream handling.`,
        response_content_type: `Content type returned by the web request.`,
        entity_table_name: `Source entity table associated with the event where available.`,
        namespace: `Namespace associated with the source event where available.`,
        device_category: `Category of device associated with the web request.`,
        is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
        known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity-anchor source where available.`,
        auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
        requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
        identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
        parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
        parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
        likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary.`,
        auid_distinct_iuid_count: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
        current_iuid: `Best currently available inferred user identifier after applying trusted-anchor, low-risk direct-assignment, conservative recursive-propagation, and activity-window fallback rules.`,
        current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
        current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
        identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
        matched_overlapping_windows: `Number of overlapping activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
        matched_post_conflict_windows: `Number of post-identity-conflict activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
        matched_unknown_preexisting_activity_windows: `Number of matched activity-window segments with unattributable pre-existing activity. Used to prevent unsafe repair assignments.`,
        eligible_window_iuid: `Inferred user identifier associated with the event's eligible clean activity window where one is available.`,
        eligible_window_start: `Start timestamp of the event's eligible clean activity window where one is available.`,
        eligible_window_has_unknown_preexisting_activity: `Boolean indicating whether the eligible activity window contains unattributable pre-existing activity.`
      },
      bigquery: {
        partitionBy: "event_date",
        clusterBy: ["auid", "current_iuid"]
      }
    }
  )
  .preOps(ctx => recursiveWalkInputScopePreOps(ctx))
  .query(ctx => `

SELECT *
FROM ${ctx.ref(activityWindowsName)}
WHERE occurred_at >= identity_rebuild_checkpoint

`);

/* --------------------------------------------------------------------------
   6B. Targeted post window repair propagation: run scoped recursive walk

   Attempt to assign identities to unresolved events that remain after the
   Stage 5 activity-window fallback.

   This is a deliberately local repair walk rather than a second full-history
   reconstruction. Stage 6A has already restricted the input to the current
   overlapping rebuild scope. This model further limits the event graph around
   AUIDs containing repair candidates.

   The repair walk uses three parent-link rule families:
     - exact referrer links within a 120 minute lookback;
     - null referrer or public landing page previous-parent fallbacks within a
       30 minute lookback;
     - near window start previous resolved parent links within a 30 minute
       lookback.

   Exact referrer links remain eligible during ambiguous activity window
   periods. Weaker temporal fallback links are created only where Stage 5 has
   not identified unsafe temporal context.

   Repair proposals are applied only where:
     - the proposed IUID does not conflict with an eligible Stage 5 window IUID
     - no attributed sign out exists between the candidate event and the repair
       source.

   Existing identity assignments are preserved. Only previously unresolved
   events may receive a Stage 6 identity.

   Grain:
   One row per valid web request event within the current recursive walk scope.
   A web request event is uniquely identified by the combination of request_uuid
   and occurred_at.

   Input:
     identity_second_recursive_walk_input_[event source]

   Output:
     identity_second_recursive_walk_run_[event source]
------------------------------------------------------------------------------ */

publish(
  secondWalkRunName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "table",
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Scoped output from the targeted post-window repair walk.

      Each row preserves the Stage 5 identity state and applies the strongest eligible local
      repair proposal to previously unresolved events. Existing identity assignments remain
      unchanged. Repair assignments are applied only where the proposed identity passes the
      configured safety gates.

      Grain: one row per valid web-request event within the current repair-walk scope.`,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web-request event. Together with occurred_at, this forms the grain of the table.`,
      occurred_at: `Timestamp at which the web-request event occurred. Together with request_uuid, this forms the grain of the table.`,
      event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web-request events only.`,
      auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity-anchor source where available.`,
      auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
      parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
      parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
      likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary.`,
      auid_distinct_iuid_count: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      current_iuid: `Best currently available inferred user identifier after applying all identity-resolution rules up to and including targeted post-window repair propagation.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier, including the local repair-walk rule where Stage 6 assigns the identity.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
      matched_overlapping_windows: `Number of overlapping activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
      matched_post_conflict_windows: `Number of post-identity-conflict activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
      matched_unknown_preexisting_activity_windows: `Number of matched activity-window segments with unattributable pre-existing activity. Used to prevent unsafe repair assignments.`,
      eligible_window_iuid: `Inferred user identifier associated with the event's eligible clean activity window where one is available.`,
      eligible_window_start: `Start timestamp of the event's eligible clean activity window where one is available.`,
      eligible_window_has_unknown_preexisting_activity: `Boolean indicating whether the eligible activity window contains unattributable pre-existing activity.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.query(ctx => `

WITH RECURSIVE

/* --------------------------------------------------------------------------
   6B.1. Load Stage 5 state and identify repair candidates

   Load the scoped Stage 5 event state.

   Mark:
     - navigable GET parents, excluding redirects
     - events that already have an identity
     - unresolved events eligible for repair
     - negative activity window context
     - unknown pre-existing activity risk

   Only unresolved, unlocked events on uncertain single IUID or high risk
   multi IUID AUIDs enter the repair walk.
------------------------------------------------------------------------------ */

current_state AS (
  SELECT *
  FROM ${ctx.ref(secondWalkInputName)}
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

/* --------------------------------------------------------------------------
   6B.2. Restrict the local AUID event graph

   For each AUID containing repair candidates, load only events betweenn two hours 
   before the earliest candidate and the latest candidate timestamp.

   The two hour lower bound covers the longest lookback used by any individual
   parent link rule.

   Stage 6 is intentionally local. Longer journeys should already have been
   resolved through Stage 4 recursive propagation or Stage 5 activity windows.
------------------------------------------------------------------------------ */

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

      Stage 6 is intentionally a local repair walk rather than a full-history
      reconstruction. It searches for reachable identity evidence within two hours
      before the earliest candidate event.

      This covers the maximum lookback required by any single repair rule:
        - explicit referrer: 120 minutes
        - previous parent: 30 minutes
        - near-window-start parent: 30 minutes

      A longer multi-edge chain may extend beyond this local scope and will not be
      repaired in Stage 6. Longer journeys should already have been resolved by the
      stricter Stage 4 propagation or Stage 5 activity windows.
    */

    TIMESTAMP_SUB(MIN(occurred_at), INTERVAL 2 HOUR) AS min_needed_at,
    MAX(occurred_at) AS max_needed_at

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
   6B.3. Prepare rule specific parent lookups

   Create narrow parent tables for the three repair rule families.

   Eligible parents are:
     - navigable GET events; or
     - events that already have an identity.

   Sign out requests are excluded so later activity cannot inherit an identity
   directly from a sign out event.

   Separate lookup tables reduce repeated joins against unnecessary rows without
   changing the rule logic.
------------------------------------------------------------------------------ */

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
  WHERE (
      is_navigable_parent = TRUE
      OR has_current_identity = TRUE
    )
    AND ${sqlNotInList("request_path", signOutPaths)}
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
   6B.4. Rule 1: exact-referrer parent links

   Match a candidate child to an earlier same-AUID parent where:
     - child referrer path-and-query equals parent path-and-query;
     - the parent occurred within the previous 120 minutes;
     - the parent is not the same request.

   Keep:
     - the closest valid exact referrer parent;
     - the closest resolved exact referrer parent.

   Resolved parents receive HIGH confidence. Unresolved parents receive MEDIUM
   confidence and may allow a chain to reach resolved evidence indirectly.

   Exact referrer links remain eligible even where Stage 5 identified overlap or
   post conflict activity-window context.

   Hour buckets reduce the initial join size. Exact timestamp predicates remain
   in place for correctness.
------------------------------------------------------------------------------ */

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
        THEN 'STAGE_6_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
      ELSE 'STAGE_6_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
    END AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM referrer_child_buckets child
  JOIN referrer_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND NOT (
      parent.request_uuid = child.request_uuid
      AND parent.occurred_at = child.occurred_at
    )

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid, child.occurred_at
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
    'STAGE_6_REFERRER_TO_RESOLVED_PARENT_SAME_AUID' AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM referrer_child_buckets child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.request_path_and_query = child.request_referer_path_and_query
   AND parent.hour_bucket = child.parent_hour_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 120 MINUTE)
   AND NOT (
  parent.request_uuid = child.request_uuid
  AND parent.occurred_at = child.occurred_at
)

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid, child.occurred_at
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
   6B.5. Rule 2: short-gap previous-parent fallbacks

   Where no useful explicit referrer is available, link selected events to the
   nearest recent same-AUID parent within 30 minutes.

   Apply this weak fallback only where:
     - the referrer is NULL or a configured public landing-page path
     - the child is not a sign-in page
     - Stage 5 did not identify overlap or post-conflict context
     - high-risk multi-IUID AUIDs do not have unknown pre-existing activity risk

   Unsafe weak edges are excluded before recursion begins. This prevents an
   ambiguous link from entering the repair graph while preserving stronger
   exact referrer routes.

   PERFORMANCE STRATEGY:
   Use generated minute buckets as equality join keys for the 30-minute
   previous-parent lookback.

   The exact timestamp filters remain for correctness. The generated minute
   buckets prevent BigQuery from joining every child to all same-AUID parent
   rows before applying the time range.
------------------------------------------------------------------------------ */

home_or_null_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE (
      child.request_referer_path_and_query IS NULL
      OR ${sqlInList("child.request_referer_path_and_query", preAuthPagePaths)}
    )
    AND ${sqlNotInList("child.request_path", signInPagePaths)}

    /*
      Weak previous-parent links are permitted only where the child event is
      not already inside an ambiguous temporal period identified by Stage 5.

      If this condition fails, no weak edge is created for the child. The
      recursive chain is therefore broken at that point, while stronger
      explicit-referrer links remain available.
    */
    AND child.has_negative_window_context = FALSE

    /*
      For high-risk multi-IUID AUIDs, also block weak links where Stage 5 found
      evidence of unknown activity before the candidate window.
    */
    AND (
      child.auid_risk_classification != 'HIGH_RISK_MULTI_IUID_AUID'
      OR child.has_unknown_preexisting_activity_risk = FALSE
    )
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
        THEN 'STAGE_6_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      WHEN child.request_referer_path_and_query IS NULL
        THEN 'STAGE_6_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'

      WHEN parent.current_iuid IS NOT NULL
        THEN 'STAGE_6_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'

      ELSE 'STAGE_6_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
    END AS match_source,

    TRUE AS is_weak_previous_parent_rule

  FROM home_or_null_child_parent_minutes child
  JOIN previous_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND NOT (
  parent.request_uuid = child.request_uuid
  AND parent.occurred_at = child.occurred_at
)

  WHERE ${sqlNotInList("parent.request_path", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid, child.occurred_at
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
        THEN 'STAGE_6_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
      ELSE 'STAGE_6_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
    END AS match_source,

    TRUE AS is_weak_previous_parent_rule

  FROM home_or_null_child_parent_minutes child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND NOT (
  parent.request_uuid = child.request_uuid
  AND parent.occurred_at = child.occurred_at
)

  WHERE ${sqlNotInList("parent.request_path", preAuthPagePaths)}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY child.request_uuid, child.occurred_at
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
   6B.6. Rule 3: near-window-start previous resolved parent

   For selected unresolved events occurring within 30 minutes of an eligible
   Stage 5 window start, search for the nearest earlier resolved parent on the
   same AUID.

   This temporal fallback is available only where:
     - the child is not a public landing page or sign-in page;
     - Stage 5 did not identify overlap or post-conflict context;
     - high-risk multi IUID AUIDs do not have unknown pre-existing activity risk.

   Only resolved parents are eligible for this rule.

   PERFORMANCE STRATEGY:
   Reuse minute bucket equality joins for this 30 minute resolved parent search.
-------------------------------------------------------------------------- */

near_window_start_children AS (
  SELECT child.*
  FROM children_to_repair child
  WHERE child.eligible_window_start IS NOT NULL
    AND child.occurred_at >= child.eligible_window_start
    AND child.occurred_at <= TIMESTAMP_ADD(child.eligible_window_start, INTERVAL 30 MINUTE)
    AND ${sqlNotInList("child.request_path", preAuthPagePaths)}
    AND ${sqlNotInList("child.request_path", signInPagePaths)}
    AND child.has_negative_window_context = FALSE
    AND (
      child.auid_risk_classification != 'HIGH_RISK_MULTI_IUID_AUID'
      OR child.has_unknown_preexisting_activity_risk = FALSE
    )
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
    'STAGE_6_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID' AS match_source,

    FALSE AS is_weak_previous_parent_rule

  FROM near_window_start_child_parent_minutes child
  JOIN resolved_parent_lookup parent
    ON parent.auid = child.auid
   AND parent.minute_bucket = child.parent_minute_bucket
   AND parent.occurred_at < child.occurred_at
   AND parent.occurred_at >= TIMESTAMP_SUB(child.occurred_at, INTERVAL 30 MINUTE)
   AND NOT (
  parent.request_uuid = child.request_uuid
  AND parent.occurred_at = child.occurred_at
)

    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY child.request_uuid, child.occurred_at
    ORDER BY
      parent.occurred_at DESC,
      parent.request_uuid
  ) = 1
),

/* --------------------------------------------------------------------------
   6B.7. Select the strongest parent link for each child

   Combine all rule-derived candidates and retain one parent per child.

   Apply the following precedence before recency:

     1. exact referrer to resolved parent
     2. exact referrer to unresolved parent
     3. near-window-start previous resolved parent
     4. null or public-landing-page fallback to resolved parent
     5. null or public-landing-page fallback to unresolved parent

   Where multiple candidates have the same rule strength, prefer:
     - the most recent parent
     - request UUID as a deterministic tiebreaker.
------------------------------------------------------------------------------ */

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
    child_at,
    best.parent_request_uuid,
    best.parent_at,
    best.match_confidence,
    best.match_source,
    best.is_weak_previous_parent_rule
  FROM (
    SELECT
      child_request_uuid,
      child_at,

      ARRAY_AGG(
        STRUCT(
          parent_request_uuid,
          parent_at,
          match_confidence,
          match_source,
          is_weak_previous_parent_rule
        )
        ORDER BY
          CASE
            WHEN match_source = 'STAGE_6_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
              THEN 5

            WHEN match_source = 'STAGE_6_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
              THEN 4

            WHEN match_source =
              'STAGE_6_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'
              THEN 3

            WHEN match_source IN (
              'STAGE_6_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M',
              'STAGE_6_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
            )
              THEN 2

            WHEN match_source IN (
              'STAGE_6_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M',
              'STAGE_6_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
            )
              THEN 1

            ELSE 0
          END DESC,
          parent_at DESC,
          parent_request_uuid
        LIMIT 1
      )[OFFSET(0)] AS best

    FROM parent_candidates
    GROUP BY child_request_uuid, child_at
  )
),

/* --------------------------------------------------------------------------
   6B.8. Build the local repair graph

   Attach the selected parent link to each event in scope.

   Events that already have an identity are treated as terminal repair sources
   and do not point backwards to another parent.
------------------------------------------------------------------------------ */

events_with_parent AS (
  SELECT
    e.*,

    CASE
      WHEN e.has_current_identity = TRUE THEN NULL
      ELSE bp.parent_request_uuid
    END AS parent_request_uuid_repair,

    CASE
      WHEN e.has_current_identity = TRUE THEN NULL
      ELSE bp.parent_at
    END AS parent_occurred_at_repair,

    bp.match_confidence AS parent_match_confidence_repair,
    bp.match_source AS parent_match_source_repair,
    bp.is_weak_previous_parent_rule AS parent_is_weak_previous_parent_rule_repair

  FROM events_scope e
  LEFT JOIN best_parent bp
    ON e.request_uuid = bp.child_request_uuid
   AND e.occurred_at = bp.child_at
),

/* --------------------------------------------------------------------------
   6B.9. Recursively walk to the nearest resolved identity

   Starting from each unresolved repair candidate, follow selected parent links
   backwards until one of the following conditions:
     - a currently resolved event is reached
     - no parent exists
     - a cycle is detected
     - the defensive maximum depth of 25 links is reached.

   Track visited request UUIDs to prevent recursive cycles.

   The first reachable current IUID becomes the proposed repair identity.
------------------------------------------------------------------------------ */

repair_walk AS (
  SELECT
    e.request_uuid AS start_request_uuid,
    e.occurred_at AS start_occurred_at,

    e.request_uuid AS current_request_uuid,
    e.occurred_at AS current_occurred_at,

    e.parent_request_uuid_repair AS parent_request_uuid,
    e.parent_occurred_at_repair AS parent_occurred_at,

    e.has_current_identity,
    e.current_iuid,
    0 AS depth,

    [
      STRUCT(
        e.request_uuid AS request_uuid,
        e.occurred_at AS occurred_at
      )
    ] AS visited_events,

    IF(e.has_current_identity, e.current_iuid, NULL)
      AS nearest_current_iuid

  FROM events_with_parent e
  WHERE e.is_repair_walk_candidate = TRUE

  UNION ALL

  SELECT
    w.start_request_uuid,
    w.start_occurred_at,

    p.request_uuid AS current_request_uuid,
    p.occurred_at AS current_occurred_at,

    p.parent_request_uuid_repair AS parent_request_uuid,
    p.parent_occurred_at_repair AS parent_occurred_at,

    p.has_current_identity,
    p.current_iuid,
    w.depth + 1 AS depth,

    ARRAY_CONCAT(
      w.visited_events,
      [
        STRUCT(
          p.request_uuid AS request_uuid,
          p.occurred_at AS occurred_at
        )
      ]
    ) AS visited_events,

    COALESCE(
      w.nearest_current_iuid,
      IF(p.has_current_identity, p.current_iuid, NULL)
    ) AS nearest_current_iuid

  FROM repair_walk w
  JOIN events_with_parent p
    ON w.parent_request_uuid = p.request_uuid
   AND w.parent_occurred_at = p.occurred_at

  WHERE w.parent_request_uuid IS NOT NULL

    AND NOT (
      w.parent_request_uuid = w.current_request_uuid
      AND w.parent_occurred_at = w.current_occurred_at
    )

    AND w.nearest_current_iuid IS NULL
    AND w.depth < 25

    AND STRUCT(
      p.request_uuid AS request_uuid,
      p.occurred_at AS occurred_at
    ) NOT IN UNNEST(w.visited_events)
),

/* --------------------------------------------------------------------------
   6B.10. Collapse each repair walk to one proposed source

   Reduce the recursive output to one result per starting request UUID.

   Prefer the nearest reachable event containing a current IUID. Record:
     - repair source request UUID
     - repair source timestamp
     - repair depth
     - proposed repaired IUID
------------------------------------------------------------------------------ */

collapsed_repair_walk AS (
  SELECT
    start_request_uuid AS request_uuid,
    start_occurred_at AS occurred_at,

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
  GROUP BY
    start_request_uuid,
    start_occurred_at
),

repair_assignments AS (
  SELECT
    c.request_uuid,
    c.occurred_at,

    c.best_repair.current_request_uuid
      AS repair_source_request_uuid,

    c.best_repair.current_occurred_at
      AS repair_source_occurred_at,

    c.best_repair.depth AS repair_depth,
    c.best_repair.nearest_current_iuid AS repaired_iuid

  FROM collapsed_repair_walk c
  WHERE c.best_repair.nearest_current_iuid IS NOT NULL
),

/* --------------------------------------------------------------------------
   6B.11. Apply proposal level safety gates

   Convert each reachable repair source into an identity proposal.

   Apply the proposal only where:
     - the repaired IUID does not conflict with an eligible Stage 5 window IUID
     - no attributed sign-out for that IUID occurs between the candidate event
       and the repair source.

   These checks apply to both strong and weak repair routes.

   Leave proposed identity audit fields NULL where no repaired IUID is found.
------------------------------------------------------------------------------ */

repair_walk_proposals AS (
  SELECT
    e.request_uuid,
    e.occurred_at,

    r.repaired_iuid AS proposed_iuid,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'STAGE_6_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'
        THEN 'STAGE_6_REFERRER_TO_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'STAGE_6_REFERRER_TO_UNRESOLVED_PARENT_SAME_AUID'
        THEN 'STAGE_6_REFERRER_CHAIN_TO_RESOLVED_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'STAGE_6_NULL_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M',
          'STAGE_6_HOME_REFERRER_PREVIOUS_RESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'STAGE_6_PREVIOUS_RESOLVED_PARENT_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair IN (
          'STAGE_6_NULL_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M',
          'STAGE_6_HOME_REFERRER_PREVIOUS_UNRESOLVED_PARENT_SAME_AUID_30M'
        )
        THEN 'STAGE_6_PREVIOUS_PARENT_CHAIN_TO_RESOLVED_SAME_AUID_SHORT_GAP'

      WHEN r.repaired_iuid IS NOT NULL
        AND p.parent_match_source_repair = 'STAGE_6_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'
        THEN 'STAGE_6_NEAR_WINDOW_START_PREVIOUS_RESOLVED_PARENT_SAME_AUID'

      WHEN r.repaired_iuid IS NOT NULL
        THEN 'STAGE_6_REPAIR_WALK_TO_NEAREST_CURRENT_IDENTITY'

      ELSE 'UNRESOLVED_AFTER_STAGE_6_REPAIR_WALK'
    END AS proposed_iuid_method,

    CASE
      WHEN r.repaired_iuid IS NOT NULL
        THEN 'STAGE_6_TARGETED_POST_WINDOW_REPAIR_PROPAGATION'
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
  AND e.occurred_at = p.occurred_at
  LEFT JOIN repair_assignments r
    ON e.request_uuid = r.request_uuid
  AND e.occurred_at = r.occurred_at
  WHERE e.is_repair_walk_candidate = TRUE
),

/* --------------------------------------------------------------------------
   6B.12. Retain the best eligible repair proposal

   Keep only proposals that pass the final safety gates.

   Where more than one eligible proposal exists defensively retain the strongest:
     - higher identity-resolution priority
     - shorter repair depth
     - earlier creation timestamp
     - IUID as a deterministic tiebreaker
------------------------------------------------------------------------------ */

deduplicated_repair_walk_proposals AS (
  SELECT
    request_uuid,
    occurred_at,
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
  GROUP BY request_uuid, occurred_at
)

/* --------------------------------------------------------------------------
   6B.13. Update the event-level identity state

   Preserve all existing identity assignments.

   For a previously unresolved event, apply the best eligible Stage 6 repair
   proposal and update
     - current_iuid
     - current_resolution_stage
     - current_iuid_method
     - identity_resolution_priority

   Do not persist temporary graph or proposal diagnostics.
------------------------------------------------------------------------------ */

  SELECT
    c.* EXCEPT (
      current_iuid,
      current_iuid_method,
      current_resolution_stage,
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

    CASE
      WHEN c.identity_resolution_locked = TRUE
        THEN 100

      WHEN c.current_iuid IS NOT NULL
        THEN c.identity_resolution_priority

      WHEN p.best_proposal.proposed_iuid IS NOT NULL
        THEN p.best_proposal.proposed_identity_resolution_priority

      ELSE c.identity_resolution_priority
    END AS identity_resolution_priority

  FROM current_state c
  LEFT JOIN deduplicated_repair_walk_proposals p
    ON c.request_uuid = p.request_uuid
  AND c.occurred_at = p.occurred_at

`);

/* --------------------------------------------------------------------------
6C. Targeted post-window repair propagation: persist durable state

Persist the scoped Stage 6B post-window repair-walk output into the durable
incremental event-state table read by downstream models.

Stage 6B runs as a standard table because it uses WITH RECURSIVE. That table
contains only the current rebuild scope produced by Stage 6A. This Stage 6C
model converts that scoped run output back into durable incremental state.

On a full refresh:
- rebuild the complete valid event history.

On an incremental run:
- calculate the overlapping rebuild checkpoint;
- delete the affected period from the existing durable state table;
- replace that period with the newly calculated Stage 6B results;
- retain historical rows outside the rebuild scope unchanged.

This separation allows recursive SQL to be used without sacrificing the
incremental behaviour required for production runs.

Grain:
One row per valid web request event.

Input:
identity_second_recursive_walk_run_[event source]

Output:
identity_second_recursive_walk_[event source]
------------------------------------------------------------------------------ */

publish(
  secondWalkName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "incremental",
    protected: true,
    uniqueKey: ["request_uuid", "occurred_at"],
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `
      Durable event-level identity state after targeted post-window repair propagation.

      Each incremental run replaces the overlapping rebuild period with the latest scoped
      repair-walk output while retaining historical rows outside that period. Existing
      identity assignments remain unchanged, and previously unresolved events receive an
      inferred identity only where the strongest eligible repair proposal passes the
      configured safety gates.

      Grain: one row per valid web-request event.
    `,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web-request event. This is the grain of the table.`,
      occurred_at: `Timestamp at which the web-request event occurred.`,
      event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web-request events only.`,
      auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      is_trusted_identity_anchor: `Boolean indicating whether the request contains trusted authenticated identity evidence.`,
      known_anchor_iuid: `Known inferred user identifier extracted from the trusted identity-anchor source where available.`,
      auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
      requires_walk: `Boolean indicating whether the event's AUID requires deeper recursive identity inference.`,
      identity_resolution_locked: `Boolean indicating whether the current identity is trusted anchor evidence and must not be overwritten downstream.`,
      parent_request_uuid_pass1: `Unique identifier of the selected parent request used by the conservative recursive walk where a valid parent relationship is available.`,
      parent_match_confidence_pass1: `Confidence level assigned to the selected conservative parent relationship.`,
      likely_shunt_arrival: `Boolean indicating whether the event is a likely cross-AUID shunt arrival. Used as a conservative activity-window boundary.`,
      auid_distinct_iuid_count: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      current_iuid: `Best currently available inferred user identifier after applying all identity-resolution rules up to and including targeted post-window repair propagation.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current identity resolution.`,
      matched_overlapping_windows: `Number of overlapping activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
      matched_post_conflict_windows: `Number of post-identity-conflict activity-window segments matched by the event. Used to prevent unsafe repair assignments.`,
      matched_unknown_preexisting_activity_windows: `Number of matched activity-window segments with unattributable pre-existing activity. Used to prevent unsafe repair assignments.`,
      eligible_window_iuid: `Inferred user identifier associated with the event's eligible clean activity window where one is available.`,
      eligible_window_start: `Start timestamp of the event's eligible clean activity window where one is available.`,
      eligible_window_has_unknown_preexisting_activity: `Boolean indicating whether the eligible activity window contains unattributable pre-existing activity.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.preOps(ctx => identityEventCheckpointPreOps(ctx))
.query(ctx => `

SELECT *
FROM ${ctx.ref(secondWalkRunName)}

`);

/* --------------------------------------------------------------------------
   7. Produce final solved event output with admin AUID normalisation

   Create the final identity resolution output consumed by downstream web
   analytics models.

   Stages 3 to 6 infer the best available IUID for each web request event.
   This final stage preserves that inferred identity and its audit fields, then
   applies a separate downstream safeguard for admin activity.

   Admin users may move between user contexts through privileged or partially
   observable paths. As a result, the propagated IUID attached to admin activity
   may not reliably identify the human administrator or the user context in
   which the request should be analysed.

   To prevent admin activity from contaminating ordinary user journeys:
     - identify each AUID that has ever produced qualifying admin-page activity;
     - flag every event associated with that historically admin-exposed AUID;
     - assign a stable synthetic analytics identity derived from the AUID.

   A separate synthetic identity is created for each admin exposed AUID rather
   than assigning one service-wide admin identity. This avoids stitching
   activity from unrelated admin devices into artificial long-running sessions.

   Preserve `current_iuid` unchanged. It remains the auditable identity inferred
   by the propagation pipeline. Downstream sessionisation and routine behavioural
   analytics should use `admin_normalised_iuid`.

   This model is rebuilt across the complete durable Stage 6C history rather
   than incrementally. A newly observed admin-page visit changes the appropriate
   downstream identity for all historical activity on the same AUID. Rebuilding
   the complete final table ensures that older rows are updated consistently.

   Grain:
     One row per valid web request event.

   Input:
     identity_second_recursive_walk_[event source]

   Output:
     identity_solved_events_[event source]
------------------------------------------------------------------------------ */

publish(
  resolvedAdminName,
  {
    ...params.defaultConfig,
    schema: "web_analytics_staging_tables",
    type: "table",
    tags: [
      params.eventSourceName.toLowerCase(),
      "identity-staging"
    ],
    description: `Final solved web-request events with an analytics-ready inferred identity.

      Each row retains the auditable identity inferred by the propagation pipeline, flags activity
      associated with historically admin-exposed AUIDs, and provides the identity recommended for
      downstream analytics.

      Where admin normalisation is enabled, analytics_iuid uses a stable synthetic identity for
      historically admin-exposed AUIDs. Otherwise, analytics_iuid equals current_iuid.

      Grain: one row per valid web-request event.`,
    dependencies: params.dependencies,
    columns: {
      request_uuid: `Unique identifier for the web-request event. This is the grain of the table.`,
      occurred_at: `Timestamp at which the web-request event occurred.`,
      event_date: `Calendar date on which the web-request event occurred. Derived from occurred_at and used to partition the table.`,
      event_type: `Type of source event. This model retains web-request events only.`,
      auid: `Anonymous user identifier observed on the web-request event. Derived from anonymised_user_agent_and_ip.`,
      request_user_id: `User identifier recorded directly on the incoming web request where available.`,
      request_path: `Path component of the requested URL.`,
      request_query: `Query-string component of the requested URL.`,
      request_path_and_query: `Combined request path and query string.`,
      request_referer_path_and_query: `Path and query string of the referring request where available.`,
      request_referer_domain: `Domain of the referring request where available.`,
      request_method: `HTTP method used for the web request.`,
      response_status: `HTTP response status cast to a string for consistent downstream handling.`,
      response_content_type: `Content type returned by the web request.`,
      entity_table_name: `Source entity table associated with the event where available.`,
      namespace: `Namespace associated with the source event where available.`,
      device_category: `Category of device associated with the web request.`,
      current_iuid: `Best inferred user identifier assigned by the identity-resolution pipeline. Preserved unchanged for audit and QA.`,
      auid_risk_classification: `All-time identity-risk classification assigned to the event's AUID.`,
      current_iuid_method: `Specific rule used to assign the current inferred user identifier.`,
      current_resolution_stage: `Pipeline stage that assigned the current inferred user identifier.`,
      identity_resolution_priority: `Numeric confidence priority assigned to the current inferred user identifier.`,
      auid_distinct_iuid_count: `Number of distinct trusted inferred user identifiers historically linked to the event's AUID.`,
      current_auid_is_admin_exposed: `Boolean indicating whether the event's AUID has ever produced qualifying admin-page activity.`,
      analytics_iuid: `Recommended inferred user identifier for downstream analytics. Where admin normalisation is enabled, historically admin exposed AUIDs receive a stable service specific synthetic identity. Otherwise, this equals current_iuid.`
    },
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["auid", "current_iuid"]
    }
  }
)
.query(ctx => `

WITH

/* --------------------------------------------------------------------------
   7.1. Load the complete durable identity resolution state

   Load all valid event level identity assignments produced by Stage 6C.

   The complete history is required because admin exposure is an all-time AUID
   classification. AUIDs observed on an admin page must be normalised consistently
   across both historical and recent activity.
------------------------------------------------------------------------------ */

identity_resolution_state AS (
  SELECT *
  FROM ${ctx.ref(secondWalkName)}
),

/* --------------------------------------------------------------------------
   7.2. Identify historically admin exposed AUIDs

   Treat an AUID as admin-exposed where it has ever produced a qualifying GET
   request to a configured admin page path.

   Exclude redirect responses because they do not represent meaningful page
   activity.

   Grain:
     One row per historically admin-exposed AUID.
------------------------------------------------------------------------------ */

admin_exposed_auids AS (
  SELECT DISTINCT
    auid
  FROM identity_resolution_state
  WHERE auid IS NOT NULL
    AND request_method = 'GET'
    AND COALESCE(SAFE_CAST(response_status AS STRING), 'X')
      NOT IN ('301', '302', '303', '307', '308')
    AND (${sqlRegexpContainsAny("request_path", adminPagePatterns)})
),

/* --------------------------------------------------------------------------
   7.3. Produce the explicit final output schema

   Retain:
     - the event fields required by downstream web analytics
     - the three-field identity audit interface
     - the two additional fields required by downstream processing
     - the admin-exposure flag
     - the analytics-safe normalised identity

   For ordinary activity:
     admin_normalised_iuid = current_iuid

   For activity on a historically admin exposed AUID:
     admin_normalised_iuid = stable synthetic identity derived from the AUID

   Include the event source name when hashing so the synthetic identity is
   explicitly namespaced to the current service.
------------------------------------------------------------------------------ */

final_solved_events AS (
  SELECT
    e.request_uuid,
    e.occurred_at,
    e.event_date,
    e.event_type,
    e.auid,
    e.request_user_id,

    e.request_path,
    e.request_query,
    e.request_path_and_query,
    e.request_referer_path_and_query,
    e.request_referer_domain,
    e.request_method,
    e.response_status,
    e.response_content_type,

    e.entity_table_name,
    e.namespace,
    e.device_category,

    /* Auditable identity resolution output. */
    e.current_iuid,
    e.auid_risk_classification,
    e.current_iuid_method,
    e.current_resolution_stage,

    /* Additional fields required by downstream processing. */
    e.identity_resolution_priority,
    e.auid_distinct_iuid_count,

    /* Flag all activity associated with a historically admin-exposed AUID. */
    (admin.auid IS NOT NULL) AS current_auid_is_admin_exposed,

    /* Provide the recommended identity for downstream analytics.

      Preserve current_iuid (above) unchanged for audit and QA.

      Where admin normalisation is enabled, replace the inferred identity for
      historically admin exposed AUIDs with a stable service specific synthetic
      identity derived from the AUID.

      Where admin normalisation is disabled, analytics_iuid remains equal to
      current_iuid even when the AUID is flagged as admin exposed. */

    CASE
      WHEN ${enableAdminNormalisation}
        AND admin.auid IS NOT NULL
        THEN CONCAT(
          'ADMIN_AUID:',
          TO_HEX(
            SHA256(
              CONCAT(
                ${sqlString(params.eventSourceName)},
                '|',
                e.auid
              )
            )
          )
        )

      ELSE e.current_iuid
    END AS analytics_iuid

  FROM identity_resolution_state e
  LEFT JOIN admin_exposed_auids admin
    ON e.auid = admin.auid
)

SELECT *
FROM final_solved_events

`);
};