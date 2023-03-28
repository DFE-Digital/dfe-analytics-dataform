const getKeys = (keys) => {
    return keys.map(key => ({
      [key.alias || key.keyName]: key.description
    })
  )
};
module.exports = (params) => {
  return params.dataSchema.forEach(tableSchema => publish(tableSchema.entityTableName + "_version_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "table",
    dependencies: [params.eventSourceName + "_entities_are_missing_expected_fields"],
    assertions: {
      uniqueKey: ["valid_from", "id"],
      nonNull: ["id"],
      rowConditions: [
        'valid_from < valid_to OR valid_to IS NULL'
      ]
    },
    bigquery: {
      partitionBy: "DATE(valid_to)",
      updatePartitionFilter: "valid_to IS NULL",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase(),
        entitytabletype: "version"
      }
    },
    description: "Versions of entities in the database valid between valid_from and valid_to. Taken from entity Create, Update and Delete events streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project. Description of these entities is: " + tableSchema.description,
    columns: Object.assign({
        valid_from: "Timestamp from which this version of this entity started to be valid.",
        valid_to: "Timestamp until which this version of this entity was valid.",
        type: "Event type of the event that provided us with this version of this entity. Either entity_created, entity_updated or entity_imported.",
        id: "Hashed (anonymised) version of the ID of this entity from the database.",
        created_at: "Timestamp this entity was first saved in the database, according to the latest version of the data received from the database.",
        updated_at: "Timestamp this entity was last updated in the database, according to the latest version of the data received from the database.",
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
      if(['id','created_at','updated_at'].includes(key.alias || key.keyName)) {
        throw new Error(`${key.keyName}' is included as a field in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform automatically, so would produce a table with more than one column with the same name. Remove this field from your dataSchema to prevent this error. Or if you're sure that you want to include the same field more than once, use an alias by setting 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema.`);
      } else if(['valid_from','valid_to','event_type','request_uuid','request_path','request_user_id','request_method','request_user_agent','request_referer','request_query','response_content_type','response_status','anonymised_user_agent_and_ip','device_category','browser_name','browser_version','operating_system_name','operating_system_vendor','operating_system_version'].includes(key.alias || key.keyName)) {
        throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
      }
      else {
        var coalesceSql;
        if (!key.pastKeyNames) {
          coalesceSql = `DATA_struct.${key.keyName}`;
        }
        else {
          coalesceSql = `COALESCE(DATA_struct.${key.keyName}, DATA_struct.${key.pastKeyNames.join(', DATA_struct.')})`;
        }
        var fieldSql;
        if(key.dataType == 'boolean') {
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
        } else {
          throw new Error(`Unrecognised dataType '${key.dataType}' for field '${key.keyName}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float, json or string or not set.`);
        }
        return `${fieldSql} AS ${key.alias || key.keyName}`;
        }
      }
    ).join(',\n')
  }
FROM (
  SELECT
    * EXCEPT(DATA),
    (
    SELECT
      AS STRUCT
      ${tableSchema.keys.map(key => {
          var pastKeyNamesSql = '';
          if (key.pastKeyNames) {
          key.pastKeyNames.forEach(pastKeyName => {
            pastKeyNamesSql += `  ANY_VALUE(IF(key="${pastKeyName}",value,NULL)) AS ${pastKeyName},\n`;
            });
          }
          return `ANY_VALUE(IF(key="${key.keyName}",value,NULL)) AS ${key.keyName},\n` + pastKeyNamesSql;
      }
    ).join('')
  }
    FROM (
      SELECT
        AS STRUCT key,
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ","),
          "") AS value
      FROM
        UNNEST(DATA)
      GROUP BY
        key )) AS DATA_struct
FROM
  ${ctx.ref(params.eventSourceName + "_entity_version")}
WHERE
  entity_table_name = "${tableSchema.entityTableName}")`)
  )
}
