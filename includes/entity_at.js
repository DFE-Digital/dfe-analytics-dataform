module.exports = (params) => {
  return params.dataSchema.forEach(tableSchema => operate(tableSchema.entityTableName + '_at_' + params.eventSourceName).queries(ctx => `
  CREATE OR REPLACE TABLE FUNCTION ${ctx.resolve(tableSchema.entityTableName + '_version_' + params.eventSourceName).replace('_version_','_at_')}(timestamp_at TIMESTAMP)
  AS
  SELECT
    * EXCEPT (valid_from,
      valid_to,
      event_type,
      request_uuid,
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
      operating_system_version)
  FROM
    ${ctx.ref(tableSchema.entityTableName + "_version_" + params.eventSourceName)}
  WHERE
    (valid_to IS NULL
      OR valid_to > timestamp_at)
    AND valid_from <= timestamp_at
  `))
}
