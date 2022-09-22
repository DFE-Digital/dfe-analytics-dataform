const dataFunctions = require("./includes/data_functions");
const events = require("./includes/events");
const pageviewWithFunnel = require("./includes/pageview_with_funnels");
const entityVersion = require("./includes/entity_version");
const entityFieldUpdates = require("./includes/entity_field_updates");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const flattenedEntityFieldUpdates = require("./includes/flattened_entity_field_updates");
const entityAt = require("./includes/entity_at");
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
    transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables
    urlRegex: null, // re-2 formatted regular expression to use to identify whether a URL is this service's own URL or an external one. If your service only has one domain name set this to 'www.yourdomainname.gov.uk' (without the protocol). If you have more than one use something like '(?i)(www.domain1.gov.uk|www.domain2.gov.uk|www.domain3.gov.uk)'
    socialRefererDomainRegex: "(?i)(facebook|twitter|t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a social media site
    searchEngineRefererDomainRegex: "(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a search enginer (regardless of whether paid or organic)
    funnelDepth: 10, // Number of steps forwards/backwards to analyse in funnels - higher allows deeper analysis, lower reduces CPU usage
    requestPathGroupingRegex: '[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*', // re2-formatted regular expression to replace with the string 'UID' when grouping request paths
    attributionParameters: ['utm_source','utm_campaign','utm_medium','utm_content','gclid','gcsrc'], // list of parameters to extract from the request_query array of structs at the beginning of funnels
    dataSchema: [],
    dependencies: [], // datasets generated outside dfe-analytics-dataform which should be materialised before datasets generated by dfe-analytics-dataform
    ...params
  };

  const {
    defaultConfig,
    eventSourceName,
    bqProjectName,
    bqDatasetName,
    bqEventsTableName,
    transformEntityEvents,
    urlRegex,
    socialRefererDomainRegex,
    searchEngineRefererDomainRegex,
    funnelDepth,
    requestPathGroupingRegex,
    attributionParameters,
    dependencies,
    dataSchema
  } = params;

  // Declare the source table
  const eventsRaw = declare({
    ...defaultConfig,
    database: bqProjectName,
    schema: bqDatasetName,
    name: bqEventsTableName,
    dependencies: dependencies
  });

  // Publish and return datasets - assertions first for quick access in the Dataform UI

  if (params.transformEntityEvents) {
    return {
      eventsRaw,
      events: events(params),
      pageviewWithFunnel: pageviewWithFunnel(params),
      entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params),
      unhandledFieldOrEntityIsBeingStreamed: unhandledFieldOrEntityIsBeingStreamed(params),
      entityVersion: entityVersion(params),
      entityFieldUpdates: entityFieldUpdates(params),
      dataFunctions,
      analyticsYmlLatest: analyticsYmlLatest(params),
      dataSchemaJSONLatest: dataSchemaJSONLatest(params),
      flattenedEntityVersion: flattenedEntityVersion(params),
      flattenedEntityLatest: flattenedEntityLatest(params),
      flattenedEntityFieldUpdates: flattenedEntityFieldUpdates(params),
      entityAt: entityAt(params)
    }
  } else {
    return {
      eventsRaw,
      events: events(params),
      pageviewWithFunnel: pageviewWithFunnel(params)
    }
  }
}
