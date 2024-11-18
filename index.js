const version = "2.1.1";

const parameterFunctions = require("./includes/parameter_functions");

const dataFunctions = require("./includes/data_functions");

const events = require("./includes/events");
const eventsDataNotFresh = require("./includes/events_data_not_fresh");
const entityDataNotFresh = require("./includes/entity_data_not_fresh");
const customEventDataNotFresh = require("./includes/custom_event_data_not_fresh");
const entityTableCheckScheduled = require("./includes/entity_table_check_scheduled");
const entityTableCheckImport = require("./includes/entity_table_check_import");
const entityIdsDoNotMatch = require("./includes/entity_ids_do_not_match");
const entityImportIdsDoNotMatch = require("./includes/entity_import_ids_do_not_match");
const pageviewWithFunnel = require("./includes/pageview_with_funnels");
const sessions = require("./includes/sessions");
const entityVersion = require("./includes/entity_version");
const entityFieldUpdates = require("./includes/entity_field_updates");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedCustomEvent = require("./includes/flattened_custom_event");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const flattenedEntityFieldUpdates = require("./includes/flattened_entity_field_updates");
const entityAt = require("./includes/entity_at");
const entitiesAreMissingExpectedFields = require("./includes/entities_are_missing_expected_fields");
const unhandledFieldOrEntityIsBeingStreamed = require("./includes/unhandled_field_or_entity_is_being_streamed");
const hiddenPIIConfigurationDoesNotMatchEventsStreamed = require("./includes/hidden_pii_configuration_does_not_match_events_streamed");
const entitiesHaveNotBeenBackfilled = require("./includes/entities_have_not_been_backfilled");
const dataSchemaJSONLatest = require("./includes/data_schema_json_latest");
const dfeAnalyticsConfiguration = require("./includes/dfe_analytics_configuration");
const migrateHistoricEventsToCurrentHiddenPIIConfiguration = require("./includes/migrate_historic_events_to_current_hidden_pii_configuration");
const pipelineSnapshot = require("./includes/pipeline_snapshot");

module.exports = (params) => {
    // Set default values of parameters if parameters with the same name have not been passed to dfeAnalyticsDataform()
    params = {
        disabled: false, // whether to disable dfe-analytics-dataform
        eventSourceName: null, // suffix to append to table names to distinguish them if this package is run more than once
        bqProjectName: dataform.projectConfig.defaultDatabase, // name of the BigQuery project that dfe-analytics streams event data into. Defaults to the same project name set in your GCP Dataform release configuration, or as the default set in dataform.json (for legacy Dataform).
        bqDatasetName: dataform.projectConfig.defaultSchema, // name of the BigQuery dataset that dfe-analytics streams event data into. Defaults to the same dataset name set in your GCP Dataform release configuration, or in dataform.json (for legacy Dataform).
        bqEventsTableName: 'events', // name of the BigQuery table that dfe-analytics streams event data into
        bqEventsTableNameSpace: null, // optional - value of the namespace field in the events table to filter by. Use this to distinguish between multiple applications or interfaces which stream events to the same events table.
        eventsDataFreshnessDays: 1, // Number of days after which, if no new events have been received, the events_data_not_fresh assertion will fail to alert you to this
        eventsDataFreshnessDisableDuringRange: false, // whether to disable the events_data_not_fresh assertion if today's date is currently between one of the ranges in assertionDisableDuringDateRanges
        compareChecksums: false, // whether to enable an assertion to compare checksums and row counts in entity_table_check events to checksums and row counts in BigQuery
        transformEntityEvents: true, // whether to generate tables that transform entity CRUD events into flattened tables
        enableSessionTables: true, // whether to generate the sessions and pageview_with_funnels tables
        enableMonitoring: true, // whether to send summary monitoring data to the monitoring.pipeline_snapshots table in the cross-service GCP project
        urlRegex: null, // re-2 formatted regular expression to use to identify whether a URL is this service's own URL or an external one. If your service only has one domain name set this to 'www.yourdomainname.gov.uk' (without the protocol). If you have more than one use something like '(?i)(www.domain1.gov.uk|www.domain2.gov.uk|www.domain3.gov.uk)'
        socialRefererDomainRegex: "(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a social media site
        searchEngineRefererDomainRegex: "(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a search enginer (regardless of whether paid or organic)
        funnelDepth: 10, // Number of steps forwards/backwards to analyse in funnels - higher allows deeper analysis, lower reduces CPU usage
        requestPathGroupingRegex: '[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*', // re2-formatted regular expression to replace with the string 'UID' when grouping request paths
        attributionParameters: ['utm_source', 'utm_campaign', 'utm_medium', 'utm_content', 'gclid', 'gcsrc'], // list of parameters to extract from the request_query array of structs at the beginning of funnels
        attributionDomainExclusionRegex: "(?i)(signin.education.gov.uk)", //re2-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which 
        expirationDays: null, // Number of days after which all data streamed by dfe-analytics or managed by dfe-analytics-dataform for a particular eventDataSource should be deleted. Must be either an integer value or false.
        webRequestEventExpirationDays: null, // the number of days after which web_request events should be deleted, along with data in tables generated from them by dfe-analytics-dataform
        tableDeletionWarningDays: 30, // how far in the future to warn that a table will be deleted according to its current expiration time
        dataSchema: [],
        customEventSchema: [],
        dependencies: [], // datasets generated outside dfe-analytics-dataform which should be materialised before datasets generated by dfe-analytics-dataform,
        assertionDisableDuringDateRanges: [{
            fromMonth: 7,
            fromDay: 25,
            toMonth: 9,
            toDay: 1
        }, {
            fromMonth: 3,
            fromDay: 29,
            toMonth: 4,
            toDay: 14
        }, {
            fromMonth: 12,
            fromDay: 22,
            toMonth: 1,
            toDay: 7
        }], // an array of day or date ranges between which some assertions will be disabled if other parameters are set to disable them. Each range is a hash containing either the integer values fromDay, fromMonth, toDay and toMonth *or* the date values fromDate and toDate. Defaults to an approximation to school holidays each year.
        ...params
    };

    // If disabled is true, stop right here, return no action definitions, and don't try to validate parameters
    if (params.disabled) {
        return true;
    }

    // Check whether parameters are valid
    parameterFunctions.validateParams(params);

    // Set default values of parameters if not set that weren't possible to set with the spread operator above
    params = parameterFunctions.setDefaultSchemaParameters(params);

    // Work out whether to disable assertions now if eventsDataFreshnessDisableDuringRange or dataSchema.dataFreshnessDisableDuringRange is true
    params.disableAssertionsNow = parameterFunctions.dateRangesToDisableAssertionsNow(params.assertionDisableDuringDateRanges, new Date());

    // Publish and return datasets - assertions first for quick access in the Dataform UI
    if (params.transformEntityEvents) {
        return {
            events: events(params),
            eventsDataNotFresh: eventsDataNotFresh(params),
            entityDataNotFresh: entityDataNotFresh(params),
            customEventDataNotFresh: customEventDataNotFresh(params),
            entityTableCheckScheduled: entityTableCheckScheduled(params),
            entityTableCheckImport: entityTableCheckImport(params),
            entityIdsDoNotMatch: entityIdsDoNotMatch(params),
            entityImportIdsDoNotMatch: entityImportIdsDoNotMatch(params),
            pageviewWithFunnel: pageviewWithFunnel(params),
            sessions: sessions(params),
            dfeAnalyticsConfiguration: dfeAnalyticsConfiguration(params),
            entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params),
            unhandledFieldOrEntityIsBeingStreamed: unhandledFieldOrEntityIsBeingStreamed(params),
            hiddenPIIConfigurationDoesNotMatchEventsStreamed: hiddenPIIConfigurationDoesNotMatchEventsStreamed(params),
            entitiesHaveNotBeenBackfilled: entitiesHaveNotBeenBackfilled(params),
            entityVersion: entityVersion(params),
            entityFieldUpdates: entityFieldUpdates(params),
            dataFunctions,
            dataSchemaJSONLatest: dataSchemaJSONLatest(params),
            flattenedEntityVersion: flattenedEntityVersion(params),
            flattenedCustomEvent: flattenedCustomEvent(params),
            flattenedEntityLatest: flattenedEntityLatest(params),
            flattenedEntityFieldUpdates: flattenedEntityFieldUpdates(params),
            migrateHistoricEventsToCurrentHiddenPIIConfiguration: migrateHistoricEventsToCurrentHiddenPIIConfiguration(params),
            entityAt: entityAt(params),
            pipelineSnapshot: pipelineSnapshot(version, params),
            version: version
        }
    } else {
        return {
            events: events(params),
            eventsDataNotFresh: eventsDataNotFresh(params),
            customEventDataNotFresh: customEventDataNotFresh(params),
            flattenedCustomEvent: flattenedCustomEvent(params),
            hiddenPIIConfigurationDoesNotMatchEventsStreamed: hiddenPIIConfigurationDoesNotMatchEventsStreamed(params),
            pageviewWithFunnel: pageviewWithFunnel(params),
            sessions: sessions(params),
            dfeAnalyticsConfiguration: dfeAnalyticsConfiguration(params),
            pipelineSnapshot: pipelineSnapshot(version, params),
            version: version
        }
    }
}
