module.exports = (params) => {
  
  return publish("test_table_counting_apply_events_today_"+params.tableSuffix, {
    ...params.defaultConfig
  }).query(ctx => `
SELECT COUNT(*) AS count FROM ${ctx.ref("apply_events_production","events")} WHERE DATE(occurred_at) = CURRENT_DATE
`)
}
