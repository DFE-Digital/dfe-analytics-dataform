// TODO: import files from includes
// const file_one = require("./includes/file_one");
// const file_two = require("./includes/file_two");

const dataFunctions = require("./includes/data_functions");
const entityVersion = require("./includes/entity_version");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const analyticsYmlLatest = require("./includes/analytics_yml_latest");
const entitiesAreMissingExpectedFields = require("./includes/entities_are_missing_expected_fields");

module.exports = (params) => {

  params = {
    tableSuffix: null, // suffix to append to table names to distinguish them if this package is run more than once
    bqProjectName: null, // name of the BigQuery project that dfe-analytics streams event data into
    bqDatasetName: null, // name of the BigQuery dataset that dfe-analytics streams event data into
    bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
    transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables,
    dataSchema: [],
    ...params
  };

  const {
    defaultConfig,
    tableSuffix,
    bqProjectName,
    bqDatasetName,
    bqEventsTableName,
    transformEntityEvents,
    dataSchema
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
      entityVersion: entityVersion(params),
      flattenedEntityVersion: flattenedEntityVersion(params),
      flattenedEntityLatest: flattenedEntityLatest(params),
      dataFunctions,
      analyticsYmlLatest: analyticsYmlLatest(params),
      entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params)
    }
  } else {
    return {
      eventsRaw
    }
  }
}
