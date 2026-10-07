/* Generates audited_web_requests_{source}{suffix}: web request events that have a matching audit,
   joined on request_uuid. This is where identifying request/device context is retained (partitions
   expire per params.airbyteAudit.webRequestEventExpirationDays), kept separate from the version
   tables so identifying data has its own retention boundary. */

module.exports = (params) => {
  if (!params.enableAirbyteSource || !params.airbyteAudit || !params.airbyteAudit.enabled) return null;

  const suffix = params.airbyteConfig.tableSuffix || '_airbyte';
  const tableName = `audited_web_requests_${params.eventSourceName}${suffix}`;
  const eventsTableName = `events_${params.eventSourceName}`;

  return publish(tableName, {
    type: "table",
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase(),
        sourcetype: 'airbyte',
        entitytype: 'audited_web_request'
      }
    },
    tags: [params.eventSourceName.toLowerCase(), 'airbyte', 'audit'],
    description: `[AIRBYTE] Web request events that have a matching audit record, joined on request_uuid. Retained separately from version tables so identifying request context can be expired independently via webRequestEventExpirationDays.`,
    columns: {
      request_uuid: "request_uuid shared with the version table's linked audit.",
      occurred_at: "Timestamp the web request event occurred.",
      request_path: "Path of the web request.",
      request_user_id: "User id associated with the web request, if any.",
      request_method: "HTTP method of the web request.",
      request_user_agent: "User agent string of the web request.",
      request_referer: "Referer header of the web request.",
      response_status: "HTTP response status of the request.",
      anonymised_user_agent_and_ip: "Anonymised hash of user agent and IP for the request."
    }
  }).query(ctx => `
SELECT
  event.request_uuid,
  event.occurred_at,
  event.request_path,
  event.request_user_id,
  event.request_method,
  event.request_user_agent,
  event.request_referer,
  event.response_status,
  event.anonymised_user_agent_and_ip
FROM
  ${ctx.ref(eventsTableName)} AS event
WHERE
  event.event_type = 'web_request'
`)
    .preOps(ctx => `
      ${params.airbyteAudit.webRequestEventExpirationDays
        ? `DELETE FROM ${ctx.self()} WHERE DATE(occurred_at) < CURRENT_DATE - ${params.airbyteAudit.webRequestEventExpirationDays};`
        : ``}
    `);
};