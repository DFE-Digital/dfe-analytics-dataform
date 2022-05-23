const getKeys = (keys) => {
    return keys.map(key => ({
      [key.keyName]: key.description
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
      nonNull: ["id", "created_at", "updated_at"],
      rowConditions: [
        'valid_from < valid_to OR valid_to IS NULL'
      ]
    },
    bigquery: {
      partitionBy: "DATE(valid_to)",
      updatePartitionFilter: "valid_to IS NULL"
    },
    description: "Versions of entities in the database valid between valid_from and valid_to. Description of these entities is: " + tableSchema.description,
    columns: Object.assign({
        valid_from: "Timestamp from which this version of this entity started to be valid.",
        valid_to: "Timestamp until which this version of this entity was valid.",
        type: "Event type of the event that provided us with this version of this entity. Either entity_created, entity_updated or entity_imported.",
        id: "Hashed (anonymised) version of the ID of this entity from the database.",
        created_at: "Timestamp this entity was first saved in the database, according to the latest version of the data received from the database.",
        updated_at: "Timestamp this entity was last updated in the database, according to the latest version of the data received from the database.",
      }, ...getKeys(tableSchema.keys))
  }).query(ctx => `SELECT
  valid_from,
  valid_to,
  event_type,
  entity_id AS id,
  ${data_functions.eventDataExtractTimestamp("DATA","created_at")} AS created_at,
  ${data_functions.eventDataExtractTimestamp("DATA","updated_at")} AS updated_at,
  ${tableSchema.keys.map(key => { 
        if(key.dataType == 'boolean') {
          return `SAFE_CAST(${data_functions.eventDataExtract("DATA",key.keyName)} AS BOOL) AS ${key.keyName},`;
        } else if (key.dataType == 'timestamp') {
          return `${data_functions.eventDataExtractTimestamp("DATA",key.keyName)} AS ${key.keyName},`;
        } else if (key.dataType == 'date') {
          return `${data_functions.eventDataExtractDate("DATA",key.keyName)} AS ${key.keyName},`;
        } else if (key.dataType == 'integer') {
          return `SAFE_CAST(${data_functions.eventDataExtract("DATA",key.keyName)} AS INT64) AS ${key.keyName},`;
        } else if (key.dataType == 'integer_array') {
          return `${data_functions.eventDataExtractIntegerArray("DATA",key.keyName)} AS ${key.keyName},`;
        } else if (key.dataType == 'float') {
          return `SAFE_CAST(${data_functions.eventDataExtract("DATA",key.keyName)} AS FLOAT64) AS ${key.keyName},`;
        } else if (key.dataType == 'string' || key.dataType == undefined) {
          return `${data_functions.eventDataExtract("DATA",key.keyName)} AS ${key.keyName},`;
        } else {
          throw new Error(`Unrecognised dataType '${key.dataType}' for field '${key.keyName}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float or string or not set.`);
        }
      }
    ).join('\n')
  }
FROM
  ${ctx.ref(params.eventSourceName + "_entity_version")}
WHERE
  entity_table_name = "${tableSchema.entityTableName}"`)
  )
}
