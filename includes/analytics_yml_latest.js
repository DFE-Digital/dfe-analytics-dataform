module.exports = (params) => {
  return publish(params.eventSourceName + "_analytics_yml_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "Structured version of the latest version of the schema which dfe-analytics-dataform has been configured to process entity CRUD events from. This is NOT necessarily the same as the latest version in analytics.yml in the production Github repository.",
    dependencies: params.dependencies,
    columns: {
      entity_name: "Name of the table we want entities to be streamed for.",
      description: "Description of this table to include in table metadata.",
      keys: "ARRAY of STRINGs listing the names of the fields we want streamed entity events to contain."
    },
    bigquery: {
      labels: {
        eventsource: params.eventSourceName.toLowerCase(),
        sourcedataset: params.bqDatasetName.toLowerCase()
      }
    }
  }).query(ctx => `SELECT * FROM UNNEST([\n
      ${params.dataSchema.map(tableSchema => {
        return `STRUCT("${tableSchema.entityTableName}" AS entity_name,\n"${tableSchema.description}" AS description,\n[${tableSchema.keys.filter(key => !key.historic).map(key => {
          return `"${key.keyName}"`;
          }
        ).join(',\n')}] AS keys)`;
      }
    ).join(',')}  
  ])`)
}
