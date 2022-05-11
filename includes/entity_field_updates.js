module.exports = (params) => {
  return publish(params.tableSuffix + "_entity_field_updates", {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    assertions: {
      uniqueKey: ["entity_id", "occurred_at", "key_updated", "entity_table_name"],
      nonNull: ["entity_id", "occurred_at", "key_updated", "entity_table_name"]
    },
    bigquery: {
      partitionBy: "DATE(occurred_at)",
      clusterBy: ["entity_table_name"]
    },
    description: "One row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to the updated_at field are not included, but NULL values are.",
    columns: {
      update_id: "UID for the collection of field updates that took place to this entity at this time. One-way hash of entity_id and occurred_at. Useful for COUNT DISTINCTs.",
      occurred_at: "Timestamp of the streamed entity update event that this field update was part of.",
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
      original_DATA: "Full DATA struct for the first version of this entity that we have available, either from when it was created or imported.",
      original_event_type: "Usually should be either create_entity or entity_imported, depending on whether the first entity data we have available is from when it was created, or whether we're relying on an import.",
      change_from_original_value: "TRUE if this update to this field represents a change away from the original value that the entity was created with, if that original value was not null or empty.",
      request_ab_tests: "STRUCT of A:B tests and variants that were active when this update was made.",
      original_request_ab_tests: "STRUCT of A:B tests and variants that were active when this entity was first created."
    }
  }).query(ctx => `WITH instance_versions AS (
  SELECT
    event_type,
    occurred_at,
    entity_table_name,
    ${data_functions.eventDataExtract("data", "id")} AS entity_id,
    ${data_functions.eventDataExtractTimestamp("data", "created_at")} AS created_at,
    ${data_functions.eventDataExtractTimestamp("data", "updated_at")} AS updated_at,
    data
  FROM
    ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)}
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
    (original_occurred_at),
   
  FROM
    (
      SELECT
        *
      EXCEPT
        (data),
        instance_versions.data AS new_data,
        NTH_VALUE(data, 2) OVER this_instance_over_time_descending AS previous_data,
        LAST_VALUE(data) OVER this_instance_over_time_descending AS original_data,
        NTH_VALUE(occurred_at, 2) OVER this_instance_over_time_descending AS previous_occurred_at,
        NTH_VALUE(event_type, 2) OVER this_instance_over_time_descending AS previous_event_type,
        LAST_VALUE(event_type) OVER this_instance_over_time_descending AS original_event_type,
        LAST_VALUE(occurred_at) OVER this_instance_over_time_descending AS original_occurred_at
      FROM
        instance_versions WINDOW this_instance_over_time_descending AS (
          PARTITION BY entity_table_name,entity_id
          ORDER BY
            occurred_at DESC ROWS BETWEEN CURRENT ROW
            AND UNBOUNDED FOLLOWING
        )
    )
  WHERE
    event_type IN ("update_entity", "create_entity")
    AND occurred_at > event_timestamp_checkpoint
)
SELECT
  instance_updates.*
EXCEPT(event_type),
  FARM_FINGERPRINT(entity_id || entity_table_name || CAST(occurred_at AS STRING)) AS update_id,
  new_data.key AS key_updated,
  new_data.value AS new_value,
  previous_data.value AS previous_value,
  TIMESTAMP_DIFF(occurred_at, previous_occurred_at, SECOND) AS seconds_since_previous_update,
  TIMESTAMP_DIFF(occurred_at, created_at, SECOND) AS seconds_since_created,
  /* Works out whether this update represented a change from the original value of this field. For this to be the case we check that (a) this is a change (we know this from the WHERE below) (b) the value we're changing from is the original value of the field and (c) the original value we have came from an entity creation event, and not an import event. */
  ${data_functions.eventDataExtract("instance_updates.original_data", "new_data.key")} = ARRAY_TO_STRING(previous_data.value,",")
  AND ARRAY_TO_STRING(previous_data.value,",") IS NOT NULL
  AND ARRAY_TO_STRING(previous_data.value,",") NOT IN (
    "",
    "[]",
    " "
  )
  AND original_event_type = "create_entity" AS change_from_original_value
FROM
  instance_updates
  CROSS JOIN UNNEST(new_data) AS new_data
  CROSS JOIN UNNEST(previous_data) AS previous_data
WHERE
  new_data.key = previous_data.key
  AND ARRAY_TO_STRING(new_data.value,"","null") != ARRAY_TO_STRING(previous_data.value,"","null")
  AND new_data.key != "updated_at"`).preOps(ctx => `DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(),`SELECT MAX(occurred_at) FROM ${ctx.self()}`,`SELECT TIMESTAMP("2018-01-01")`)}
      )`)
}
