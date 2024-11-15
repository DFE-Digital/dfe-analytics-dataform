const getKeys = (keys) => {
    return keys.map(key => ({
        [key.alias || key.keyName]: {
          description: key.description,
          bigqueryPolicyTags: key.hidden && key.hiddenPolicyTagLocation ? [key.hiddenPolicyTagLocation] : []
        }
    }))
};
module.exports = (params) => {
    return params.customEventSchema.forEach(customEvent => {
        publish(customEvent.eventType + "_" + params.eventSourceName, {
            ...params.defaultConfig,
            type: "incremental",
            dependencies: params.customEventSchema.some(customEvent => customEvent.keys.length > 0) ? [
                params.eventSourceName + "_" + "hidden_pii_configuration_does_not_match_custom_events_streamed_yesterday",
                params.eventSourceName + "_" + "hidden_pii_configuration_does_not_match_sample_of_historic_custom_events_streamed"
            ]: [],
            assertions: {
                nonNull: ["occurred_at"]
            },
            bigquery: {
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase(),
                    entitytabletype: "custom_event"
                },
                partitionBy: "DATE(occurred_at)",
                partitionExpirationDays: customEvent.expirationDays || params.expirationDays,
            },
            tags: [params.eventSourceName.toLowerCase()],
            description: "Custom events with type " + customEvent.eventType + " streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project. Description of these custom events is: " + customEvent.description,
            columns: Object.assign({
                occurred_at: "Timestamp when this event occurred.",
                request_user_id: "If a user was logged in when they sent a web request event that caused this event to happen, then this is the UID of this user.",
                request_uuid: "UUID of the web request that caused this event to happen.",
                request_method: "Whether the web request that caused this event to happen was a GET or a POST request.",
                request_path: "The path, starting with a / and excluding any query parameters, of the web request that caused this event to happen.",
                request_user_agent: "The user agent of the web request that caused this event to happen. Allows a user's browser and operating system to be identified.",
                request_referer: "The URL of any page the user was viewing when they initiated the web request that caused this event to happen. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
                request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that caused this event to happen.",
                response_content_type: "Content type of any data that was returned to the browser following the web request that caused this event to happen. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
                response_status: "HTTP response code returned by the application in response to the web request that caused this event to happen. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
                anonymised_user_agent_and_ip: "One way hash of a combination of the IP address and user agent of the user who made the web request that caused this event to happen. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
                device_category: "The category of device that caused the web request that caused this event to happen - desktop, mobile, bot or unknown.",
                browser_name: "The name of the browser that caused the web request that caused this event to happen.",
                browser_version: "The version of the browser that caused the web request that caused this event to happen.",
                operating_system_name: "The name of the operating system that caused the web request that caused this event to happen.",
                operating_system_vendor: "The vendor of the operating system that caused the web request that caused this event to happen.",
                operating_system_version: "The version of the operating system that caused the web request that caused this event to happen."
            }, ...getKeys(customEvent.keys))
        }).query(ctx => `SELECT
  occurred_at,
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
  operating_system_version,
  ${customEvent.keys.map(key => {
      let coalesceSql;
      if (!key.pastKeyNames) {
        coalesceSql = `data_struct.\`${key.keyName}\``;
      }
      else {
        coalesceSql = `COALESCE(data_struct.\`${key.keyName}\`, data_struct.\`${key.pastKeyNames.join('\`, data_struct.\`')}\`)`;
      }
      let fieldSql;
      if (key.dataType == 'boolean') {
        fieldSql = `SAFE_CAST(${coalesceSql} AS BOOL)`;
      } else if (key.dataType == 'timestamp') {
        fieldSql = `${data_functions.stringToTimestamp(coalesceSql)}`;
      } else if (key.dataType == 'date') {
        fieldSql = `${data_functions.stringToDate(coalesceSql)}`;
      } else if (key.dataType == 'integer') {
        fieldSql = `SAFE_CAST(${coalesceSql} AS INT64)`;
      } else if (key.dataType == 'integer_array') {
        fieldSql = `${data_functions.stringToIntegerArray(coalesceSql)}`;
      } else if (key.dataType == 'float') {
        fieldSql = `SAFE_CAST(${coalesceSql} AS FLOAT64)`;
      } else if (key.dataType == 'json') {
        fieldSql = `SAFE.PARSE_JSON(${coalesceSql})`;
      } else if (key.dataType == 'string' || key.dataType == undefined) {
        fieldSql = `${coalesceSql}`;
      }
      // Else error which is handled in index.js
      return `${fieldSql} AS \`${key.alias || key.keyName}\``;
    }
    ).join(',\n')
      }
FROM (
  SELECT
    * EXCEPT(data, hidden_data),
    ${customEvent.keys.length > 0 ?
      `(
      SELECT
        AS STRUCT
        ${customEvent.keys.map(key => {
          let valueField = key.coalesceWithLegacyPII
                              ? (
                              key.isArray
                                ? 'CASE WHEN hidden THEN (SELECT TO_HEX(SHA256(value)) FROM UNNEST(value_array) AS value) ELSE value_array END'
                                : 'CASE WHEN hidden THEN TO_HEX(SHA256(value)) ELSE value END'
                              )
                              : (
                              key.isArray
                                ? 'value_array'
                                : 'value'
                              );
          let pastKeyNamesSql = key.pastKeyNames ? key.pastKeyNames.map(pastKeyName => {
            return `ANY_VALUE(IF(key = "${pastKeyName}", ${valueField}, NULL)) AS \`${pastKeyName}\`, \n`;
          }).join('') : '';
          return `ANY_VALUE(IF(key = "${key.keyName}", ${valueField}, NULL)) AS \`${key.keyName}\`, \n` + pastKeyNamesSql;
        }
        ).join('')
        }
      FROM (
        SELECT
          AS STRUCT data_combined.key,
          NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT_AGG(data_combined.value), ","),
            "") AS value,
          ARRAY_CONCAT_AGG(data_combined.value) AS value_array,
          LOGICAL_OR(hidden_data.value IS NOT NULL) AS hidden
        FROM
          UNNEST(
            ARRAY_CONCAT(data, hidden_data)
            ) AS data_combined
        LEFT JOIN
          UNNEST(hidden_data) AS hidden_data USING(key)
        GROUP BY
          key )
      ) AS data_struct`
    : ``}
FROM
  ${ctx.ref("events_" + params.eventSourceName)}
WHERE
  event_type = "${customEvent.eventType}"
  AND DATE(occurred_at) >= DATE(event_timestamp_checkpoint))`)
.preOps(ctx => `
    DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(), `SELECT MAX(occurred_at) FROM ${ctx.self()}`, `SELECT TIMESTAMP("2000-01-01")`)})`)
})
}
    