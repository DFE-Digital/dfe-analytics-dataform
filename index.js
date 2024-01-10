const parameterFunctions = require("./includes/parameter_functions");

const dataFunctions = require("./includes/data_functions");
const events = require("./includes/events");
const eventsDataNotFresh = require("./includes/events_data_not_fresh");
const entityDataNotFresh = require("./includes/entity_data_not_fresh");
const entityIdsDoNotMatch = require("./includes/entity_ids_do_not_match");
const pageviewWithFunnel = require("./includes/pageview_with_funnels");
const sessions = require("./includes/sessions");
const entityVersion = require("./includes/entity_version");
const entityFieldUpdates = require("./includes/entity_field_updates");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const flattenedEntityFieldUpdates = require("./includes/flattened_entity_field_updates");
const entityAt = require("./includes/entity_at");
const entitiesAreMissingExpectedFields = require("./includes/entities_are_missing_expected_fields");
const unhandledFieldOrEntityIsBeingStreamed = require("./includes/unhandled_field_or_entity_is_being_streamed");
const referentialIntegrityCheck = require("./includes/referential_integrity_check");
const dataSchemaJSONLatest = require("./includes/data_schema_json_latest");
const dfeAnalyticsConfiguration = require("./includes/dfe_analytics_configuration");
const pseudonymiseRequestUserIds = require("./includes/pseudonymise_request_user_ids");

module.exports = (params) => {

  params = {
    disabled: false, // whether to disable dfe-analytics-dataform
    enableUnitTests: false, // whether to run dfe-analytics-dataform JS unit tests
    eventSourceName: null, // suffix to append to table names to distinguish them if this package is run more than once
    bqProjectName: dataform.projectConfig.defaultDatabase, // name of the BigQuery project that dfe-analytics streams event data into. Defaults to the same project name set in your GCP Dataform release configuration, or as the default set in dataform.json (for legacy Dataform).
    bqDatasetName: dataform.projectConfig.defaultSchema, // name of the BigQuery dataset that dfe-analytics streams event data into. Defaults to the same dataset name set in your GCP Dataform release configuration, or in dataform.json (for legacy Dataform).
    bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
    bqEventsTableNameSpace: null, // optional - value of the namespace field in the events table to filter by. Use this to distinguish between multiple applications or interfaces which stream events to the same events table.
    eventsDataFreshnessDays: 1, // Number of days after which, if no new events have been received, the events_data_not_fresh assertion will fail to alert you to this
    eventsDataFreshnessDisableDuringRange: false, // whether to disable the events_data_not_fresh assertion if today's date is currently between one of the ranges in assertionDisableDuringDateRanges
    compareChecksums: false, // whether to enable an assertion to compare checksums and row counts in entity_table_check events to checksums and row counts in BigQuery
    transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables
    urlRegex: null, // re-2 formatted regular expression to use to identify whether a URL is this service's own URL or an external one. If your service only has one domain name set this to 'www.yourdomainname.gov.uk' (without the protocol). If you have more than one use something like '(?i)(www.domain1.gov.uk|www.domain2.gov.uk|www.domain3.gov.uk)'
    socialRefererDomainRegex: "(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a social media site
    searchEngineRefererDomainRegex: "(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a search enginer (regardless of whether paid or organic)
    funnelDepth: 10, // Number of steps forwards/backwards to analyse in funnels - higher allows deeper analysis, lower reduces CPU usage
    requestPathGroupingRegex: '[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*', // re2-formatted regular expression to replace with the string 'UID' when grouping request paths
    attributionParameters: ['utm_source', 'utm_campaign', 'utm_medium', 'utm_content', 'gclid', 'gcsrc'], // list of parameters to extract from the request_query array of structs at the beginning of funnels
    attributionDomainExclusionRegex: "(?i)(signin.education.gov.uk)", //re2-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which 
    dataSchema: [],
    dependencies: [], // datasets generated outside dfe-analytics-dataform which should be materialised before datasets generated by dfe-analytics-dataform,
    assertionDisableDuringDateRanges: [{ fromMonth: 7, fromDay: 25, toMonth: 9, toDay: 1 }, { fromMonth: 3, fromDay: 29, toMonth: 4, toDay: 14 }, { fromMonth: 12, fromDay: 22, toMonth: 1, toDay: 7 }], // an array of day or date ranges between which some assertions will be disabled if other parameters are set to disable them. Each range is a hash containing either the integer values fromDay, fromMonth, toDay and toMonth *or* the date values fromDate and toDate. Defaults to an approximation to school holidays each year.
    ...params
  };

  const {
    defaultConfig,
    eventSourceName,
    bqProjectName,
    bqDatasetName,
    bqEventsTableName,
    bqEventsTableNameSpace,
    eventsDataFreshnessDays,
    compareChecksums,
    transformEntityEvents,
    urlRegex,
    socialRefererDomainRegex,
    searchEngineRefererDomainRegex,
    funnelDepth,
    requestPathGroupingRegex,
    attributionParameters,
    attributionDomainExclusionRegex,
    dependencies,
    assertionDisableDuringDateRanges,
    dataSchema
  } = params;

  if (params.disabled) {
    return true;
  }
  else if (!/^[A-Za-z0-9_]*$/.test(params.eventSourceName)) {
    throw new Error(`eventSourceName ${params.eventSourceName} contains characters that are not alphanumeric or an underscore`);
  }
  // Loop through dataSchema to handle errors and set default values
  dataSchema.forEach(tableSchema => {
    // Set default value of materialisation to 'table' for all tables in dataSchema if not set explicitly
    if (!tableSchema.materialisation) {
      tableSchema.materialisation = 'table';
    }
    else if (tableSchema.materialisation && tableSchema.materialisation != 'view' && tableSchema.materialisation != 'table') {
      throw new Error(`Value of materialisationType ${tableSchema.materialisation} for table ${tableSchema.entityTableName} in dataSchema must be either 'view' or 'table'.`);
    }
    tableSchema.keys.forEach(key => {
      if (key.dataType && !['boolean', 'timestamp', 'date', 'integer', 'integer_array', 'float', 'json', 'string'].includes(key.dataType)) {
        throw new Error(`Unrecognised dataType '${key.dataType}' for field '${key.keyName}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float, json or string or not set.`);
      } else if (['id', 'created_at', 'updated_at'].includes(key.alias || key.keyName)) {
        throw new Error(`${key.keyName}' is included as a field in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform automatically, so would produce a table with more than one column with the same name. Remove this field from your dataSchema to prevent this error. Or if you're sure that you want to include the same field more than once, use an alias by setting 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema.`);
      } else if (['valid_from', 'valid_to', 'event_type', 'request_uuid', 'request_path', 'request_user_id', 'request_method', 'request_user_agent', 'request_referer', 'request_query', 'response_content_type', 'response_status', 'anonymised_user_agent_and_ip', 'device_category', 'browser_name', 'browser_version', 'operating_system_name', 'operating_system_vendor', 'operating_system_version'].includes(key.alias || key.keyName)) {
        throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_version_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
      } else if (['new_value', 'previous_value', 'key_updated', 'update_id', 'previous_occurred_at', 'seconds_since_previous_update', 'seconds_since_created', 'previous_event_type'].includes(key.alias || key.keyName)) {
        throw new Error(`'${key.keyName}' is the same as a field name in the ${tableSchema.entityTableName}_field_updates_${params.eventSourceName} table generated by dfe-analytics-dataform, so would produce a table with two columns with the same name. Set 'alias: "alternative_name_for_${key.keyName}"' for this field in your dataSchema to prevent this error.`);
      }
    })
  });

  params.disableAssertionsNow = parameterFunctions.dateRangesToDisableAssertionsNow(params.assertionDisableDuringDateRanges, new Date());

  // Run unit tests if enableUnitTests parameter is set to true
  if (params.enableUnitTests) {
    parameterFunctions.tests();
  }

  // Publish and return datasets - assertions first for quick access in the Dataform UI

  if (params.transformEntityEvents) {
    return {
      events: events(params),
      eventsDataNotFresh: eventsDataNotFresh(params),
      entityDataNotFresh: entityDataNotFresh(params),
      entityIdsDoNotMatch: entityIdsDoNotMatch(params),
      pageviewWithFunnel: pageviewWithFunnel(params),
      sessions: sessions(params),
      dfeAnalyticsConfiguration: dfeAnalyticsConfiguration(params),
      entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params),
      unhandledFieldOrEntityIsBeingStreamed: unhandledFieldOrEntityIsBeingStreamed(params),
      referentialIntegrityCheck: referentialIntegrityCheck(params),
      entityVersion: entityVersion(params),
      entityFieldUpdates: entityFieldUpdates(params),
      dataFunctions,
      dataSchemaJSONLatest: dataSchemaJSONLatest(params),
      flattenedEntityVersion: flattenedEntityVersion(params),
      flattenedEntityLatest: flattenedEntityLatest(params),
      flattenedEntityFieldUpdates: flattenedEntityFieldUpdates(params),
      pseudonymiseRequestUserIds: pseudonymiseRequestUserIds(params),
      entityAt: entityAt(params)
    }
  } else {
    return {
      events: events(params),
      eventsDataNotFresh: eventsDataNotFresh(params),
      pageviewWithFunnel: pageviewWithFunnel(params),
      sessions: sessions(params),
      dfeAnalyticsConfiguration: dfeAnalyticsConfiguration(params)
    }
  }
}
