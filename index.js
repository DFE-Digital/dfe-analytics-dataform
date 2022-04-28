// TODO: import files from includes
// const file_one = require("./includes/file_one");
// const file_two = require("./includes/file_two");

const dataFunctions = require("./includes/data_functions");
const testTable1 = require ("./includes/test_table_counting_apply_events_today");
const testTable2 = require ("./includes/test_table_counting_apply_events_today2");

module.exports = (params) => {
    
 params = {
    tableSuffix: null, // suffix to append to table names to distinguish them if this package is run more than once
    bqProjectName: null, // name of the BigQuery project that dfe-analytics streams event data into
    bqDatasetName: null, // name of the BigQuery dataset that dfe-analytics streams event data into
    bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
    ...params
  };

  const {
    defaultConfig,
    tableSuffix,
    bqProjectName,
    bqDatasetName,
    bqEventsTableName
  } = params;
  
  // Declare the source table
  const eventsRaw = declare({
    ...defaultConfig,
    database: bqProjectName,
    schema: bqDatasetName,
    name: bqEventsTableName
  });

  // Publish and return datasets.

  return {
      eventsRaw,
      testTable1: testTable1(params),
      testTable2: testTable2(params),
      dataFunctions
  }
}
