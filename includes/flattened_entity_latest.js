const getKeys = (keys) => {
    return keys.map(key => ({
        [key.alias || key.keyName]: key.description
    }))
};
module.exports = (params) => {
    return params.dataSchema.forEach(tableSchema => publish(tableSchema.entityTableName + "_latest_" + params.eventSourceName, {
        ...params.defaultConfig,
        type: tableSchema.materialisation,
        ...(tableSchema.materialisation == "table" ? {
            assertions: {
                uniqueKey: ["id"],
                nonNull: ["last_streamed_event_occurred_at", "last_streamed_event_type", "id"]
            }
        } : {}),
        bigquery: {
            labels: {
                eventsource: params.eventSourceName.toLowerCase(),
                sourcedataset: params.bqDatasetName.toLowerCase(),
                entitytabletype: "latest"
            },
            ...(tableSchema.materialisation == "table" ? {
                partitionBy: "DATE(created_at)"
            } : {})
        },
        tags: [params.eventSourceName.toLowerCase()],
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
  (valid_to, valid_from, event_type,request_uuid,request_path,request_user_id,request_method,request_user_agent,request_referer,request_query,response_content_type,response_status,anonymised_user_agent_and_ip,device_category,browser_name,browser_version,operating_system_name,operating_system_vendor,operating_system_version),
  valid_from AS last_streamed_event_occurred_at,
  event_type AS last_streamed_event_type
FROM
  ${ctx.ref(tableSchema.entityTableName + "_version_" + params.eventSourceName)}
WHERE
  valid_to IS NULL
`).postOps(ctx => tableSchema.materialisation == "table" ? `
  IF TRUE THEN /* Workaround - without putting the ALTER TABLE statements in a conditional block, although the script would execute without error, BigQuery query compilation unhelpfully returns an error on the line that attempts to add a primary key if a primary key already exists, ignoring the fact that a previous step in the script removes the primary key. */
  ${data_functions.dropAllKeyConstraints(ctx, dataform)}
  ALTER TABLE ${ctx.self()} ADD PRIMARY KEY(id) NOT ENFORCED;
    ${tableSchema.keys
            // Only generate foreign key constraints for keys in the table that have the foreignKeyTable parameter set and when referential integrity checks are enabled at either key or eventDataSource level
            .filter(key => key.foreignKeyTable && (key.foreignKeyName == "id" || !key.foreignKeyName))
            .map(
              key => `ALTER TABLE ${ctx.self()} ADD CONSTRAINT ${key.alias || key.keyName}_relationship FOREIGN KEY(${key.alias || key.keyName}) REFERENCES ${ctx.ref(key.foreignKeyTable + "_latest_" + params.eventSourceName)}(id) NOT ENFORCED;`
              ).join('\n')}

  END IF;
    ` : ``))
}
