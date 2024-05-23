const getKeys = (keys) => {
    return keys.map(key => ({
        [key.alias || key.keyName]: {
          description: key.description,
          bigqueryPolicyTags: key.hidden && key.hiddenPolicyTagLocation ? [key.hiddenPolicyTagLocation] : []
        }
    }))
};
module.exports = (params) => {
    return params.dataSchema.forEach(tableSchema => {
        publish(tableSchema.entityTableName + "_version_" + params.eventSourceName, {
            ...params.defaultConfig,
            type: tableSchema.materialisation,
            ...(tableSchema.materialisation == "table" ? {
                assertions: {
                    uniqueKey: ["valid_from", "id"],
                    nonNull: ["id"],
                    rowConditions: [
                        'valid_from < valid_to OR valid_to IS NULL'
                    ]
                },
            } : {}),
            bigquery: {
                labels: {
                    eventsource: params.eventSourceName.toLowerCase(),
                    sourcedataset: params.bqDatasetName.toLowerCase(),
                    entitytabletype: "version"
                },
                ...(tableSchema.materialisation == "table" ? {
                    partitionBy: "DATE(valid_to)"
                } : {})
            },
            tags: [params.eventSourceName.toLowerCase()],
            description: "Versions of entities in the database valid between valid_from and valid_to. Taken from entity Create, Update and Delete events streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project. Description of these entities is: " + tableSchema.description,
            columns: Object.assign({
                valid_from: "Timestamp from which this version of this entity started to be valid.",
                valid_to: "Timestamp until which this version of this entity was valid.",
                type: "Event type of the event that provided us with this version of this entity. Either entity_created, entity_updated or entity_imported.",
                id: {
                    description: "ID of this entity from the database.",
                    bigqueryPolicyTags: params.hidePrimaryKey && params.hiddenPolicyTagLocation  ? [params.hiddenPolicyTagLocation] : []
                },
                created_at: {
                    description: "Timestamp this entity was first saved in the database, according to this version of the entity.",
                    bigqueryPolicyTags: params.hideCreatedAt && params.hiddenPolicyTagLocation  ? [params.hiddenPolicyTagLocation] : []
                },
                updated_at: {
                    description: "Timestamp this entity was last updated in the database, according to this version of the entity.",
                    bigqueryPolicyTags: params.hideUpdatedAt && params.hiddenPolicyTagLocation  ? [params.hiddenPolicyTagLocation] : []
                },
                request_user_id: "If a user was logged in when they sent a web request event that caused this version to be created, then this is the UID of this user.",
                request_uuid: "UUID of the web request that caused this version to be created.",
                request_method: "Whether the web request that caused this version to be created was a GET or a POST request.",
                request_path: "The path, starting with a / and excluding any query parameters, of the web request that caused this version to be created.",
                request_user_agent: "The user agent of the web request that caused this version to be created. Allows a user's browser and operating system to be identified.",
                request_referer: "The URL of any page the user was viewing when they initiated the web request that caused this version to be created. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
                request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that caused this version to be created.",
                response_content_type: "Content type of any data that was returned to the browser following the web request that caused this version to be created. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
                response_status: "HTTP response code returned by the application in response to the web request that caused this version to be created. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
                anonymised_user_agent_and_ip: "One way hash of a combination of the IP address and user agent of the user who made the web request that caused this version to be created. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
                device_category: "The category of device that caused this version to be created - desktop, mobile, bot or unknown.",
                browser_name: "The name of the browser that caused this version to be created.",
                browser_version: "The version of the browser that caused this version to be created.",
                operating_system_name: "The name of the operating system that caused this version to be created.",
                operating_system_vendor: "The vendor of the operating system that caused this version to be created.",
                operating_system_version: "The version of the operating system that caused this version to be created."
            }, ...getKeys(tableSchema.keys))
        }).query(ctx => `SELECT
  valid_from,
  valid_to,
  event_type,
  entity_id AS id,
  created_at,
  updated_at,
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
  ${tableSchema.keys.map(key => {
      let coalesceSql;
      if (!key.pastKeyNames) {
        coalesceSql = `DATA_struct.\`${key.keyName}\``;
      }
      else {
        coalesceSql = `COALESCE(DATA_struct.\`${key.keyName}\`, DATA_struct.\`${key.pastKeyNames.join('\`, DATA_struct.\`')}\`)`;
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
    (
    SELECT
      AS STRUCT
      ${tableSchema.keys.map(key => {
        let valueField = 'value';
        if (key.isArray) {
          valueField = 'value_array';
        }
        let pastKeyNamesSql = '';
        if (key.pastKeyNames) {
          key.pastKeyNames.forEach(pastKeyName => {
            pastKeyNamesSql += `ANY_VALUE(IF(key = "${pastKeyName}", ${valueField}, NULL)) AS \`${pastKeyName}\`, \n`;
          });
        }
        return `ANY_VALUE(IF(key = "${key.keyName}", ${valueField}, NULL)) AS \`${key.keyName}\`, \n` + pastKeyNamesSql;
      }
      ).join('')
      }
    FROM (
      SELECT
        AS STRUCT key,
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ","),
          "") AS value,
        ARRAY_CONCAT_AGG(value) AS value_array
      FROM
        UNNEST(
          ARRAY_CONCAT(data, hidden_data)
          )
      GROUP BY
        key )) AS DATA_struct
FROM
  ${ctx.ref(params.eventSourceName + "_entity_version")}
WHERE
  entity_table_name = "${tableSchema.entityTableName}")`)
.postOps(ctx => tableSchema.materialisation == "table" ? data_functions.setKeyConstraints(ctx, dataform, {
    primaryKey: "id, valid_from"
    }) : ``)
})
}
    