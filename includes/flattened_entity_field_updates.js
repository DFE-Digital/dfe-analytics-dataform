const getNewColumnDescriptions = (keys) => {
  return keys.map(key => ({
    ["new_" + (key.alias || key.keyName)]: "Value immediately after this update of: " + key.description
  })
  )
};
const getPreviousColumnDescriptions = (keys) => {
  return keys.map(key => ({
    ["previous_" + (key.alias || key.keyName)]: "Value immediately before this update of: " + key.description
  })
  )
};
module.exports = (params) => {
  return params.dataSchema.forEach(tableSchema => publish(tableSchema.entityTableName + "_field_updates_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: tableSchema.materialisation,
    bigquery: {
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase(),
        entitytabletype: "field_updates"
      },
      ...(tableSchema.materialisation == "table" ? {partitionBy: "DATE(occurred_at)", clusterBy: ["key_updated"]} : {})
    },
    tags: [params.eventSourceName.toLowerCase()],
    description: "One row for each time a field was updated for each " + tableSchema.entityTableName + " that was/were streamed as events from the database, setting out the previous value of the field and the new value of the field. Entity deletions and updates to the updated_at field are not included, but NULL values are. Taken from entity Create, Update and Delete events streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project. Description of these entities is: " + tableSchema.description,
    columns: Object.assign({
      update_id: "UID for the collection of field updates that took place to this entity at this time. One-way hash of entity_id and occurred_at. Useful for COUNT DISTINCTs.",
      occurred_at: "Timestamp of the streamed entity update event that this field update was part of.",
      entity_id: "ID of this entity from the database, some IDs may have been removed or hashed (anonymised) if they contained personally identifiable information (PII).",
      created_at: "Timestamp this entity was first saved in the database, according to the streamed entity update event.",
      updated_at: "Timestamp this entity was last updated in the database, according to the streamed entity update event. Should be similar to occurred_at",
      key_updated: "The name of the field that was updated.",
      new_value: "The value of this field after it was updated.",
      previous_value: "The value of this field before it was updated.",
      previous_occurred_at: "Timestamp this entity was previously updated.",
      seconds_since_previous_update: "The number of seconds between occurred_at and previous_occurred_at.",
      seconds_since_created: "The number of seconds between occurred_at and created_at.",
      original_event_type: "Usually should be either create_entity or entity_imported, depending on whether the first entity data we have available is from when it was created, or whether we're relying on an import.",
      previous_event_type: "Usually should be either create_entity, update_entity or entity_imported, depending on whether the previous event was a creation, update or an import.",
      event_type: "Type of streamed event that contained this update to a field. Usually should be either create_entity, update_entity or entity_imported, depending on whether the event was a creation, update or an import.",
      change_from_original_value: "TRUE if this update to this field represents a change away from the original value that the entity was created with, if that original value was not null or empty.",
      request_user_id: "If a user was logged in when they sent a web request event that caused this update, then this is the UID of this user.",
      request_uuid: "UUID of the web request that caused this update.",
      request_method: "Whether the web request that caused this update was a GET or a POST request.",
      request_path: "The path, starting with a / and excluding any query parameters, of the web request that caused this update.",
      request_user_agent: "The user agent of the web request that caused this update. Allows a user's browser and operating system to be identified.",
      request_referer: "The URL of any page the user was viewing when they initiated the web request that caused this update. This is the full URL, including protocol (https://) and any query parameters, if the browser shared these with our application as part of the web request. It is very common for this referer to be truncated for referrals from external sites.",
      request_query: "ARRAY of STRUCTs, each with a key and a value. Contains any query parameters that were sent to the application as part of the web request that caused this update.",
      response_content_type: "Content type of any data that was returned to the browser following the web request that caused this update. For example, 'text/html; charset=utf-8'. Image views, for example, may have a non-text/html content type.",
      response_status: "HTTP response code returned by the application in response to the web request that caused this update. See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status.",
      anonymised_user_agent_and_ip: "One way hash of a combination of the IP address and user agent of the user who made the web request that caused this update. Can be used to identify the user anonymously, even when user_id is not set. Cannot be used to identify the user over a time period of longer than about a month, because of IP address changes and browser updates.",
      device_category: "The category of device used to cause this update - desktop, mobile, bot or unknown.",
      browser_name: "The name of the browser that caused this update.",
      browser_version: "The version of the browser that caused this update.",
      operating_system_name: "The name of the operating system that caused this update.",
      operating_system_vendor: "The vendor of the operating system that caused this update.",
      operating_system_version: "The version of the operating system that caused this update."
    }, ...getNewColumnDescriptions(tableSchema.keys), ...getPreviousColumnDescriptions(tableSchema.keys))
  }).query(ctx => `SELECT
  * EXCEPT(new_DATA_struct, previous_DATA_struct, entity_table_name, updated_at),
  ${tableSchema.keys.map(key => {
    var newFieldCoalesceSql;
    var previousFieldCoalesceSql;
    if (!key.pastKeyNamesToCoalesce) {
      newFieldCoalesceSql = `new_DATA_struct.${key.alias || key.keyName}`;
      previousFieldCoalesceSql = `previous_DATA_struct.${key.alias || key.keyName}`;
    }
    else {
      newFieldCoalesceSql = `COALESCE(new_DATA_struct.${key.alias || key.keyName}, new_DATA_struct.${key.pastKeyNamesToCoalesce.join(', new_DATA_struct.')})`;
      previousFieldCoalesceSql = `COALESCE(previous_DATA_struct.${key.alias || key.keyName}, previous_DATA_struct.${key.pastKeyNamesToCoalesce.join(', previous_DATA_struct.')})`;
    }
    var fieldSql;
    if (key.dataType == 'boolean') {
      fieldSql = `SAFE_CAST(${newFieldCoalesceSql} AS BOOL) AS new_${key.alias || key.keyName},\nSAFE_CAST(${previousFieldCoalesceSql} AS BOOL) AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'timestamp') {
      fieldSql = `${data_functions.stringToTimestamp(newFieldCoalesceSql)} AS new_${key.alias || key.keyName},\n${data_functions.stringToTimestamp(previousFieldCoalesceSql)} AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'date') {
      fieldSql = `${data_functions.stringToDate(newFieldCoalesceSql)} AS new_${key.alias || key.keyName},\n${data_functions.stringToDate(previousFieldCoalesceSql)} AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'integer') {
      fieldSql = `SAFE_CAST(${newFieldCoalesceSql} AS INT64) AS new_${key.alias || key.keyName},\nSAFE_CAST(${previousFieldCoalesceSql} AS INT64) AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'integer_array') {
      fieldSql = `${data_functions.stringToIntegerArray(newFieldCoalesceSql)} AS new_${key.alias || key.keyName},\n${data_functions.stringToIntegerArray(previousFieldCoalesceSql)} AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'float') {
      fieldSql = `SAFE_CAST(${newFieldCoalesceSql} AS FLOAT64) AS new_${key.alias || key.keyName},\nSAFE_CAST(${previousFieldCoalesceSql} AS FLOAT64) AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'json') {
      fieldSql = `SAFE.PARSE_JSON(${newFieldCoalesceSql}) AS new_${key.alias || key.keyName},\nSAFE.PARSE_JSON(${previousFieldCoalesceSql}) AS previous_${key.alias || key.keyName}`;
    } else if (key.dataType == 'string' || key.dataType == undefined) {
      fieldSql = `${newFieldCoalesceSql} AS new_${key.alias || key.keyName},\n${previousFieldCoalesceSql} AS previous_${key.alias || key.keyName}`;
    }
    // Else error which is handled in index.js
    return fieldSql;
  }
  ).join(',\n')
    }
FROM
  (
  SELECT
    * EXCEPT(new_DATA, previous_DATA, original_DATA),
    (
    SELECT
      AS STRUCT
      ${tableSchema.keys.map(key => {
      if (['id', 'created_at', 'updated_at'].includes(key.alias || key.keyName)) {
        throw new Error(`${key.keyName}' is included as a field in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform automatically, so would produce a table with more than one column with the same name. Remove this field from your dataSchema to prevent this error. Or if you're sure that you want to include the same field more than once, use an alias by setting 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema.`);
      }
      else if (['valid_from', 'valid_to', 'event_type', 'request_uuid', 'request_path', 'request_user_id', 'request_method', 'request_user_agent', 'request_referer', 'request_query', 'response_content_type', 'response_status', 'anonymised_user_agent_and_ip', 'device_category', 'browser_name', 'browser_version', 'operating_system_name', 'operating_system_vendor', 'operating_system_version'].includes(key.alias || key.keyName)) {
        throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
      }
      else {
        return `ANY_VALUE(IF(key="${key.keyName}",value,NULL)) AS ${key.alias || key.keyName},`;
      }
    }
    ).join('\n')
    }
    FROM (
      SELECT
        AS STRUCT key,
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ","),
          "") AS value
      FROM
        UNNEST(new_DATA)
      GROUP BY
        key )) AS new_DATA_struct,
    (
    SELECT
      AS STRUCT
      ${tableSchema.keys.map(key => {
      return `ANY_VALUE(IF(key="${key.keyName}",value,NULL)) AS ${key.alias || key.keyName},`;
    }
    ).join('\n')
    }
    FROM (
      SELECT
        AS STRUCT key,
        NULLIF(ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ","),
          "") AS value
      FROM
        UNNEST(previous_DATA)
      GROUP BY
        key )) AS previous_DATA_struct
FROM
  ${ctx.ref(params.eventSourceName + "_entity_field_updates")}
WHERE
  entity_table_name = "${tableSchema.entityTableName}")`)
  )
}
