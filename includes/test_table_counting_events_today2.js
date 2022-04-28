module.exports = (params) => {
  
  return publish("test_table_counting_"+params.tableSuffix+"_events_today2", {
    ...params.defaultConfig
  }).query(ctx => `
SELECT SUM(*) AS sum FROM ${ctx.ref("test_table_counting_"+params.tableSuffix+"_events_today")}
`)
}
