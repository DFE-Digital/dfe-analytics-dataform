module.exports = (params) => {
  
  return declare({
    schema: params.bqDatasetName,
    name: params.bqTableName
  })
}
