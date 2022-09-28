module.exports = (params) => {
  const attributionParamFieldMetadata = (params) => {
    return params.map(param => ({
      [param]: `Value of the ${param} URL parameter included in the first pageview in this session.`
    })
    )
  };
  return publish("sessions_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "table",
    bigquery: {
      partitionBy: "DATE(session_started_at)",
      clusterBy: ["user_ids", "anonymised_user_agent_and_ip"],
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    description: "User sessions from " + params.eventSourceName + ", with attribution fields (e.g. medium, referer_domain) for each session. Includes the session_started_at and next_session_started_at timestamps to allow attribution modelling of a goal conversion that occurred between those timestamps.",
    columns: Object.assign({
      session_started_at: "The timestamp at which the first pageview in this session occurred.",
      next_session_started_at: "The timestamp at which the first pageview in the next session happened, for any user with the same user_id or anonymised_user_agent_and_ip linked to any of these user_ids.",
      anonymised_user_agent_and_ip: "One way hash of a combination of the user's IP address and user agent. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
      user_ids: "Comma-separated list of authenticated user IDs for this anonymised_user_agent_and_ip. Will be set even if this anonymised_user_agent_and_ip logged in in a different session to this one.",
      landing_request_uuid: "UUID of the web request which was the first pageview in this session. Can be joined to request_uuid in an events table e.g. when linking to goal conversions.",
      landing_page_path: "The path, starting with a / and excluding any query parameters, of the web request that was the first pageview in this session.",
      user_agent: "The user agent of the web requests in this session. Allows a user's browser and operating system to be identified.",
      device_category: "The category of device used for this session - desktop, mobile, bot or unknown.",
      browser_name: "The name of the browser used for this session.",
      browser_version: "The version of the browser used for this session.",
      operating_system_name: "The name of the operating system used for this session.",
      operating_system_vendor: "The vendor of the operating system used for this session.",
      operating_system_version: "The version of the operating system used for this session.",
      next_step: "String indicating whether, at the end of this funnel, the user 'Left site immediately after this' or 'Visited subsequent pages'",
      medium: "Categorises where the traffic came from outside the site. NULL for traffic that was not newly arrived traffic. Possible values are PPC, Social, Email, Referral, Organic, or 'Direct or unknown'.",
      referer_domain: "Domain of the site the traffic came from outside the site. NULL for traffic that was not newly arrived traffic. Note that channels other than Referral may still have a referer_domain - for example, the domain name of the search engine that PPC/Organic traffic came from, or the social media site that Social traffic came from."},
      ...attributionParamFieldMetadata(params.attributionParameters))
      
  }).query(ctx => `WITH
  user_link AS (
  SELECT
    request_user_id AS user_id,
    anonymised_user_agent_and_ip
  FROM
    ${ctx.ref("events_" + params.eventSourceName)}
  WHERE
    request_user_id IS NOT NULL
    AND anonymised_user_agent_and_ip IS NOT NULL),
  session_with_user_ids AS (
  SELECT
    occurred_at,
    session.anonymised_user_agent_and_ip,
    STRING_AGG(DISTINCT user_link.user_id, ", "
    ORDER BY
      user_id ASC) AS user_ids,
  FROM
    ${ctx.ref("pageview_with_funnels_" + params.eventSourceName)} AS session
  LEFT JOIN
    user_link
  USING
    (anonymised_user_agent_and_ip)
  WHERE
    newly_arrived = "Newly arrived traffic"
    AND session.anonymised_user_agent_and_ip IS NOT NULL
  GROUP BY
    session.occurred_at,
    session.anonymised_user_agent_and_ip)
SELECT
  occurred_at AS session_started_at,
IF
  (user_ids IS NULL, FIRST_VALUE(occurred_at) OVER (PARTITION BY anonymised_user_agent_and_ip ORDER BY occurred_at ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), FIRST_VALUE(occurred_at) OVER (PARTITION BY user_ids ORDER BY occurred_at ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) AS next_session_started_at,
  anonymised_user_agent_and_ip,
  user_ids,
  request_uuid AS landing_request_uuid,
  request_path AS landing_page_path,
  request_user_agent AS user_agent,
  device_category,
  browser_name,
  browser_version,
  operating_system_name,
  operating_system_vendor,
  operating_system_version,
  ${params.attributionParameters.toString()},
  medium,
  referer_domain,
  next_step
FROM
  ${ctx.ref("pageview_with_funnels_" + params.eventSourceName)} AS session
JOIN
  session_with_user_ids
USING
  (occurred_at,
    anonymised_user_agent_and_ip)
WHERE
  newly_arrived = "Newly arrived traffic"
  `)
}
