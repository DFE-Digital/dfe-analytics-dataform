module.exports = (params) => {
  
  return publish("test_table_counting_"+params.tableSuffix+"_events_today", {
    ...params.defaultConfig
  }).query(ctx => `
SELECT COUNT(*) AS count FROM ${ctx.ref(params.bqDatasetName,params.bqEventsTableName)} WHERE DATE(occurred_at) = CURRENT_DATE
`)
}
