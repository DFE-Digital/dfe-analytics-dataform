module.exports = (params) => {
  
  return publish("test_table_counting_"+params.tableSuffix+"_events_today2", {
    ...params.defaultConfig
  }).query(ctx => `
SELECT SUM(count) AS sum FROM ${ctx.ref("test_table_counting_"+params.tableSuffix+"_events_today")}
`)
}
