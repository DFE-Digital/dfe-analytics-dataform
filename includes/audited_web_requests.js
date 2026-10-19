/* Generates audited_web_requests_{source}{suffix}: web request events that have a matching audit,
   joined on request_uuid. This is where identifying request/device context is retained (partitions
   expire per params.airbyteAudit.webRequestEventExpirationDays), kept separate from the version
   tables so identifying data has its own retention boundary. */

const {auditNames, isAuditLinkEnabled} = require("./airbyte_audits");

module.exports = (params) => {
  if (!params.enableAirbyteSource || !params.airbyteAudit || !params.airbyteAudit.enabled) return null;

  const suffix = params.airbyteConfig.tableSuffix || '_airbyte';
  const tableName = `audited_web_requests_${params.eventSourceName}${suffix}`;
  const eventsTableName = `events_${params.eventSourceName}`;

  return publish(tableName, {
    type: "incremental",
    uniqueKey: ["request_uuid"],
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      ...(params.airbyteAudit.webRequestEventExpirationDays
      ? { partitionExpirationDays: params.airbyteAudit.webRequestEventExpirationDays }
      : {}),
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
  request_uuid,
  occurred_at,
  request_path,
  request_user_id,
  request_method,
  request_user_agent,
  request_referer,
  request_query,
  response_content_type,
  response_status,
  anonymised_user_agent_and_ip,
  device_category,
  browser_name,
  browser_version,
  operating_system_name,
  operating_system_vendor,
  operating_system_version
FROM
  ${ctx.ref(eventsTableName)}
WHERE
  event_type = 'web_request'
  AND request_uuid IN (
      ${params.dataSchema.filter(e => isAuditLinkEnabled(params, e)).map(e =>
        `SELECT request_uuid FROM ${ctx.ref(auditNames(params, e).auditsTableName)}`
      ).join("\nUNION DISTINCT\n")}
    )
    ${params.airbyteAudit.webRequestEventExpirationDays
      ? `AND DATE(occurred_at) >= CURRENT_DATE - ${params.airbyteAudit.webRequestEventExpirationDays}`
      : ``}
    ${ctx.when(ctx.incremental(),
      `AND DATE(occurred_at) > (SELECT TIMESTAMP_SUB(MAX(occurred_at), INTERVAL 3 DAY) FROM ${ctx.self()})`)}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY request_uuid ORDER BY occurred_at) = 1    
`)
};