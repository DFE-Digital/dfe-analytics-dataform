module.exports = (params) => {
  return publish(params.eventSourceName + "_entity_field_updates", {
    ...params.defaultConfig,
    type: "table",
    protected: false,
    assertions: {
      uniqueKey: ["entity_id", "occurred_at", "key_updated", "entity_table_name"],
      nonNull: ["entity_id", "occurred_at", "key_updated", "entity_table_name"]
    },
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      clusterBy: ["entity_table_name", "key_updated"],
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    },
    tags: [params.eventSourceName.toLowerCase()],
    description: "One row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to the updated_at field are not included, but NULL values are.",
    columns: {
      update_id: "UID for the collection of field updates that took place to this entity at this time. One-way hash of entity_id and occurred_at. Useful for COUNT DISTINCTs.",
      occurred_at: "Timestamp of the streamed entity update event that this field update was part of.",
      event_type: "Type of streamed event that contained this update to a field. Usually should be either create_entity, update_entity or entity_imported, depending on whether the event was a creation, update or an import.",
      entity_table_name: "Indicates which table this entity came from e.g. application_choices",
      entity_id: "ID of this entity from the database, some IDs may have been removed or hashed (anonymised) if they contained personally identifiable information (PII).",
      created_at: "Timestamp this entity was first saved in the database, according to the streamed entity update event.",
      updated_at: "Timestamp this entity was last updated in the database, according to the streamed entity update event. Should be similar to occurred_at",
      key_updated: "The name of the field that was updated.",
      new_value: "The value of this field after it was updated.",
      previous_value: "The value of this field before it was updated.",
      new_data: "Full DATA struct for this entity after it was updated.",
      previous_data: "Full DATA struct for this entity before it was updated.",
      previous_occurred_at: "Timestamp this entity was previously updated.",
      seconds_since_previous_update: "The number of seconds between occurred_at and previous_occurred_at.",
      seconds_since_created: "The number of seconds between occurred_at and created_at.",
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
    }
  }).query(ctx => `WITH instance_versions AS (
  SELECT
    event_type,
    occurred_at,
    entity_table_name,
    ${data_functions.eventDataExtract("data", "id")} AS entity_id,
    ${data_functions.eventDataExtract("data", "created_at", false, "timestamp")} AS created_at,
    ${data_functions.eventDataExtract("data", "updated_at", false, "timestamp")} AS updated_at,
    data,
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
    operating_system_version
  FROM
    ${ctx.ref("events_" + params.eventSourceName)}
  WHERE
    event_type IN (
      "update_entity",
      "create_entity",
      "import_entity"
    )
    AND entity_table_name IS NOT NULL
    AND ${data_functions.eventDataExtract("data", "id")} IS NOT NULL
),
instance_updates AS (
  SELECT
    *
  EXCEPT
    (data),
    instance_versions.data AS new_data,
    LAG(data) OVER versions_of_this_instance_over_time AS previous_data,
    LAG(occurred_at) OVER versions_of_this_instance_over_time AS previous_occurred_at,
    LAG(event_type) OVER versions_of_this_instance_over_time AS previous_event_type
  FROM
    instance_versions WINDOW versions_of_this_instance_over_time AS (
      PARTITION BY entity_table_name, entity_id
      ORDER BY
        occurred_at ASC)
)
SELECT
  instance_updates.*,
  FARM_FINGERPRINT(entity_id || entity_table_name || CAST(occurred_at AS STRING)) AS update_id,
  new_data.key AS key_updated,
  ARRAY_TO_STRING(new_data.value,",") AS new_value,
  ARRAY_TO_STRING(previous_data.value,",") AS previous_value,
  TIMESTAMP_DIFF(occurred_at, previous_occurred_at, SECOND) AS seconds_since_previous_update,
  TIMESTAMP_DIFF(occurred_at, created_at, SECOND) AS seconds_since_created
FROM
  instance_updates
  CROSS JOIN UNNEST(new_data) AS new_data
  CROSS JOIN UNNEST(previous_data) AS previous_data
WHERE
  instance_updates.event_type IN ("update_entity", "create_entity")
  AND new_data.key = previous_data.key
  AND ARRAY_TO_STRING(new_data.value,"","null") != ARRAY_TO_STRING(previous_data.value,"","null")
  AND new_data.key != "updated_at"`)
}
