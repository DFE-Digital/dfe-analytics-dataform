module.exports = (params) => {
  return publish(params.eventSourceName + "_entity_version", {
    ...params.defaultConfig,
    type: "incremental",
    protected: false,
    uniqueKey: ["entity_table_name", "entity_id", "valid_from"],
    assertions: {
      uniqueKey: ["entity_table_name", "entity_id", "valid_from"],
      nonNull: ["entity_table_name", "entity_id", "valid_from"],
      rowConditions: [
        'valid_from < valid_to OR valid_to IS NULL'
      ]
    },
    bigquery: {
      partitionBy: "DATE(valid_to)",
      clusterBy: ["entity_table_name"],
      updatePartitionFilter: "valid_to IS NULL",
      labels: {
        eventSourceName: params.eventSourceName
      }
    },
    description: "Each row represents a version of an entity in the " + params.eventSourceName + " database that was been streamed into the events table. Versions are valid from valid_from until just before valid_to. If valid_to is NULL then this version is the latest version of this entity. If valid_to is not NULL, but no later version exists, then this entity has been deleted.",
    columns: {
      valid_from: "Timestamp from which this version of this entity started to be valid.",
      valid_to: "Timestamp until which this version of this entity was valid.",
      event_type: "Event type of the event that provided us with this version of this entity. Either create_entity, update_entity or import_entity.",
      entity_table_name: "Indicates which table this entity version came from",
      entity_id: "Hashed (anonymised) version of the ID of this entity from the database.",
      created_at: "Timestamp this entity was first saved in the database, according to the latest version of the data received from the database.",
      updated_at: "Timestamp this entity was last updated in the database, according to the latest version of the data received from the database.",
      DATA: "ARRAY of STRUCTs containing all data stored against this entity as of the latest version we have. Some fields that are in the database may have been removed or hashed (anonymised) if they contained personally identifiable information (PII) or were not deemed to be useful for analytics. NULL if entity has been deleted from the database."
    }
  }).query(ctx => `WITH entity_events AS (
  /* all entity events that have been streamed since we last ran this query, UNION ALLed with the latest events only for events that this query already processed in the past - this avoids having to process any unnecessary entity events later in the query */
  SELECT
    *
  FROM
    (
      SELECT
        event_type,
        occurred_at,
        entity_table_name,
        ${data_functions.eventDataExtract("data", "id")} AS entity_id,
        ${data_functions.eventDataExtractTimestamp("data", "created_at")} AS created_at,
        ${data_functions.eventDataExtractTimestamp("data", "updated_at")} AS updated_at,
        DATA
        /*,
        request_ab_tests*/
      FROM
        ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)}
      WHERE
        occurred_at > event_timestamp_checkpoint
        AND event_type IN (
          "create_entity",
          "update_entity",
          "delete_entity",
          "import_entity"
        )
        AND entity_table_name IS NOT NULL
        AND ${data_functions.eventDataExtract("data", "id")} IS NOT NULL
    )
    ${
      ctx.when(ctx.incremental(),
        `UNION ALL (SELECT event_type, valid_from AS occurred_at, entity_table_name, entity_id, created_at, updated_at, DATA/*, request_ab_tests*/ FROM ${ctx.self()} WHERE valid_to IS NULL AND valid_from <= event_timestamp_checkpoint)`)
    }
)
SELECT
  *
FROM
  (
    /* for each event, work out the valid_from and valid_to timestamps for the version of the entity it represents by working out the next value of occurred_at for this instance of this entity */
    SELECT
      occurred_at AS valid_from,
      FIRST_VALUE(occurred_at) OVER future_events_for_this_entity AS valid_to,
      event_type,
      entity_table_name,
      entity_id,
      created_at,
      updated_at,
      DATA
      /*,
      request_ab_tests*/
    FROM
      entity_events WINDOW future_events_for_this_entity AS (
        PARTITION BY entity_table_name,
        entity_id
        ORDER BY
          occurred_at ASC ROWS BETWEEN 1 FOLLOWING
          AND UNBOUNDED FOLLOWING
      )
  )
WHERE
  event_type != "delete_entity"
  AND (
    /* If we run a backfill job for entity events then occurred_at is set to the created_at date of the entity, not the timestamp when the import event happened. However this creates two entity versions at the same time, one of which is only valid for zero seconds. This excludes these from this table, ensuring that the combination of valid_from, entity_id and entity_table_name provides a unique identifier for an entity version. */
    valid_from != valid_to
    OR valid_to IS NULL
  )`).preOps(ctx => `DECLARE event_timestamp_checkpoint DEFAULT (
        ${ctx.when(ctx.incremental(),`SELECT MAX(valid_to) FROM ${ctx.self()}`,`SELECT TIMESTAMP("2018-01-01")`)}
      )`)
}
