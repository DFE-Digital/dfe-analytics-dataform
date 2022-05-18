module.exports = (params) => {
  return publish(params.eventSourceName + "_analytics_yml_latest", {
    ...params.defaultConfig,
    type: "table",
    description: "Structured version of the latest version of the schema which dfe-analytics-dataform has been configured to process entity CRUD events from. This is NOT necessarily the same as the latest version in analytics.yml in the production Github repository.",
    columns: {
      entity_name: "Name of the table we want entities to be streamed for.",
      description: "Description of this table to include in table metadata.",
      keys: "ARRAY of STRINGs listing the names of the fields we want streamed entity events to contain."
    }
  }).query(ctx => `SELECT * FROM UNNEST([
      ${params.dataSchema.map(tableSchema => {
        return `STRUCT("${tableSchema.entityTableName}" AS entity_name, "${tableSchema.description}" AS description, [${tableSchema.keys.map(key => {
          return `"${key.keyName}"`;
          }
        ).join(',')}] AS keys)`;
      }
    ).join(',')}  
  ])`)
}
