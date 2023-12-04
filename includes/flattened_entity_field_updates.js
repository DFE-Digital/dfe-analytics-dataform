const getColumnDescriptions = (keys) => {
  return keys.map(key => ({
    [(key.alias || key.keyName)]: "Value immediately before this update of: " + key.description
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
      previous_event_type: "Usually should be either create_entity, update_entity or entity_imported, depending on whether the previous event was a creation, update or an import.",
      event_type: "Type of streamed event that contained this update to a field. Usually should be either create_entity, update_entity or entity_imported, depending on whether the event was a creation, update or an import.",
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
    }, ...getColumnDescriptions(tableSchema.keys))
  }).query(ctx => `
WITH field_update AS (
  /* Pre filter field updates before the LEFT JOIN further down, reducing bytes processed */
  SELECT
      * EXCEPT(new_DATA, previous_DATA, entity_table_name, updated_at)
  FROM
    ${ctx.ref(params.eventSourceName + "_entity_field_updates")}
  WHERE
    entity_table_name = "${tableSchema.entityTableName}")

SELECT
  field_update.*,
    ${tableSchema.keys.map(key => {
          return `version.${key.alias || key.keyName}`;
        }
      ).join(',\n')
    }
FROM
  field_update
LEFT JOIN
  ${ctx.ref(tableSchema.entityTableName + "_version_" + params.eventSourceName)} AS version
ON
  field_update.entity_id = version.id
  AND field_update.occurred_at >= version.valid_from
  AND (field_update.occurred_at < version.valid_to
    OR version.valid_to IS NULL)
  `)
  )
}
