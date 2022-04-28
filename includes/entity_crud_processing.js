module.exports = (params) => {
  return declare({
    type: "declaration",
    database: params.bqProjectName,
    schema: params.bqDatasetName,
    name: params.bqTableName
  })
}
