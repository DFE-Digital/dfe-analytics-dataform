const getKeys = (keys) => {
    return keys.map(key => ({
      [key.keyName]: key.description
    })
  )
};
module.exports = (params) => {
  return params.dataSchema.forEach(tableSchema => publish(tableSchema.entityTableName + "_latest_" + params.eventSourceName, {
    ...params.defaultConfig,
    type: "table",
    assertions: {
      uniqueKey: ["id"],
      nonNull: ["last_streamed_event_occurred_at", "last_streamed_event_type", "id", "created_at", "updated_at"]
    },
    bigquery: {
      partitionBy: "DATE(created_at)",
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase(),
        entitytabletype: "latest"
      }
    },
    description: "Latest version of " + tableSchema.entityTableName + ". Taken from entity Create, Update and Delete events streamed into the events table in the " + params.bqDatasetName + " dataset in the " + params.bqProjectName + " BigQuery project." + tableSchema.description,
    columns: Object.assign({
        last_streamed_event_occurred_at: "Timestamp of the event that we think provided us with the latest version of this entity.",
        last_streamed_event_type: "Event type of the event that we think provided us with the latest version of this entity. Either entity_created, entity_updated, entity_destroyed or entity_imported.",
        id: "UID",
        created_at: "Date this entity was created, according to the latest version of the data received from the database.",
        updated_at: "Date this entity was last updated something in the database, according to the latest version of the data received from the database.",
      }, ...getKeys(tableSchema.keys))
  }).query(ctx => `SELECT
  *
EXCEPT
  (valid_to, valid_from, event_type),
  valid_from AS last_streamed_event_occurred_at,
  event_type AS last_streamed_event_type
FROM
  ${ctx.ref(tableSchema.entityTableName + "_version_" + params.eventSourceName)}
WHERE
  valid_to IS NULL
`)
)
}
