const dataFunctions = require("./includes/data_functions");
const events = require("./includes/events");
const entityVersion = require("./includes/entity_version");
const entityFieldUpdates = require("./includes/entity_field_updates");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const analyticsYmlLatest = require("./includes/analytics_yml_latest");
const entitiesAreMissingExpectedFields = require("./includes/entities_are_missing_expected_fields");
const unhandledFieldOrEntityIsBeingStreamed = require("./includes/unhandled_field_or_entity_is_being_streamed");
const dataSchemaJSONLatest = require("./includes/data_schema_json_latest");

module.exports = (params) => {

  params = {
    eventSourceName: null, // suffix to append to table names to distinguish them if this package is run more than once
    bqProjectName: null, // name of the BigQuery project that dfe-analytics streams event data into
    bqDatasetName: null, // name of the BigQuery dataset that dfe-analytics streams event data into
    bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
    transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables,
    dataSchema: [],
    ...params
  };

  const {
    defaultConfig,
    eventSourceName,
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

  // Publish and return datasets - assertions first for quick access in the Dataform UI

  if (params.transformEntityEvents) {
    return {
      eventsRaw,
      events: events(params),
      entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params),
      unhandledFieldOrEntityIsBeingStreamed: unhandledFieldOrEntityIsBeingStreamed(params),
      entityVersion: entityVersion(params),
      entityFieldUpdates: entityFieldUpdates(params),
      dataFunctions,
      analyticsYmlLatest: analyticsYmlLatest(params),
      dataSchemaJSONLatest: dataSchemaJSONLatest(params),
      flattenedEntityVersion: flattenedEntityVersion(params),
      flattenedEntityLatest: flattenedEntityLatest(params)
    }
  } else {
    return {
      eventsRaw,
      events: events(params)
    }
  }
}
