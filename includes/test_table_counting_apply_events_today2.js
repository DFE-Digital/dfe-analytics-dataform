module.exports = (params) => {
  
  return publish("test_table_counting_apply_events_today2_"+params.tableSuffix, {
    ...params.defaultConfig
  }).query(ctx => `
SELECT SUM(*) AS sum FROM ${ctx.ref("test_table_counting_apply_events_today_"+params.tableSuffix)}
`)
}
