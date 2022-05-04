// TODO: import files from includes
// const file_one = require("./includes/file_one");
// const file_two = require("./includes/file_two");

const dataFunctions = require("./includes/data_functions");
const testTable1 = require("./includes/test_table_counting_events_today");
const testTable2 = require("./includes/test_table_counting_events_today2");
const entityVersion = require("./includes/entity_version");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const analyticsYmlLatest = require("./includes/analytics_yml_latest");

module.exports = (params) => {

  params = {
    tableSuffix: null, // suffix to append to table names to distinguish them if this package is run more than once
    bqProjectName: null, // name of the BigQuery project that dfe-analytics streams event data into
    bqDatasetName: null, // name of the BigQuery dataset that dfe-analytics streams event data into
    bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
    transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables,
    dataSchema: [{
      entityTableName: "application_experiences",
      keys: [{
        keyName: "application_form_id",
        dataType: "string"
      }, {
        keyName: "commitment",
        dataType: "string"
      }, {
        keyName: "currently_working",
        dataType: "boolean"
      }, {
        keyName: "details",
        dataType: "string"
      }, {
        keyName: "end_date",
        dataType: "date_as_timestamp"
      }, {
        keyName: "end_date_unknown",
        dataType: "boolean"
      }, {
        keyName: "organisation",
        dataType: "string"
      }, {
        keyName: "relevant_skills",
        dataType: "boolean"
      }, {
        keyName: "role",
        dataType: "string"
      }, {
        keyName: "start_date",
        dataType: "date_as_timestamp"
      }, {
        keyName: "start_date_unknown",
        dataType: "boolean"
      }, {
        keyName: "type",
        dataType: "string"
      }, {
        keyName: "working_pattern",
        dataType: "string"
      }, {
        keyName: "working_with_children",
        dataType: "boolean"
      }]
    }],
    analyticsYmlFileLatest: null,
    ...params
  };

  const {
    defaultConfig,
    tableSuffix,
    bqProjectName,
    bqDatasetName,
    bqEventsTableName,
    transformEntityEvents,
    dataSchema,
    analyticsYmlFileLatest
  } = params;

  // Declare the source table
  const eventsRaw = declare({
    ...defaultConfig,
    database: bqProjectName,
    schema: bqDatasetName,
    name: bqEventsTableName
  });

  // Publish and return datasets.

  if (params.transformEntityEvents) {
    return {
      eventsRaw,
      testTable1: testTable1(params),
      testTable2: testTable2(params),
      entityVersion: entityVersion(params),
      /*flattenedEntityVersion: flattenedEntityVersion(params),
      params.dataSchema.forEach(tableSchema => {flattenedEntityVersion(tableSchema, ...params)}), */
      dataFunctions,
      analyticsYmlLatest: analyticsYmlLatest(params)
    }
  } else {
    return {
      eventsRaw,
      testTable1: testTable1(params),
      testTable2: testTable2(params)
    }
  }
}
