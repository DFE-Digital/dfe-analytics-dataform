module.exports = (params) => {
  return declare({
    type: "declaration",
    schema: params.bqDatasetName,
    name: params.bqTableName
  });
}
