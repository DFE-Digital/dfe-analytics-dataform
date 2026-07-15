const version = "2.5.2";

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
const session_details = require("./includes/session_details");
const entityVersion = require("./includes/entity_version");
const entityFieldUpdates = require("./includes/entity_field_updates");
const flattenedEntityVersion = require("./includes/flattened_entity_version");
const flattenedCustomEvent = require("./includes/flattened_custom_event");
const flattenedEntityLatest = require("./includes/flattened_entity_latest");
const flattenedEntityFieldUpdates = require("./includes/flattened_entity_field_updates");
const entityAt = require("./includes/entity_at");
const entitiesAreMissingExpectedFields = require("./includes/entities_are_missing_expected_fields");
const unhandledFieldOrEntityIsBeingStreamed = require("./includes/unhandled_field_or_entity_is_being_streamed");
const unhandledCustomEventIsBeingStreamed = require("./includes/unhandled_custom_event_is_being_streamed");
const hiddenPIIConfigurationDoesNotMatchEventsStreamed = require("./includes/hidden_pii_configuration_does_not_match_events_streamed");
const entitiesHaveNotBeenBackfilled = require("./includes/entities_have_not_been_backfilled");
const dataSchemaJSONLatest = require("./includes/data_schema_json_latest");
const dfeAnalyticsConfiguration = require("./includes/dfe_analytics_configuration");
const migrateHistoricEventsToCurrentHiddenPIIConfiguration = require("./includes/migrate_historic_events_to_current_hidden_pii_configuration");
const pipelineTableSnapshot = require("./includes/pipeline_table_snapshot");
const pipelineSnapshot = require("./includes/pipeline_snapshot");

// Airbyte modules
const airbyteGlobalDataFreshness = require("./includes/airbyte_global_data_freshness");
const airbyteSchemaAssertions = require("./includes/airbyte_schema_assertions");
const airbyteEntityLatest = require("./includes/airbyte_entity_latest");
const airbyteEntityVersion = require("./includes/airbyte_entity_version");
const airbyteEntityDataNotFresh = require("./includes/airbyte_entity_data_not_fresh");
const airbyteReconciliation = require("./includes/airbyte_reconciliation");

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
        enableSessionDetailsTable: true, // whether to generate the session_details table
        enableMonitoring: true, // whether to send summary monitoring data to the monitoring.pipeline_snapshots table in the cross-service GCP project
        urlRegex: null, // re-2 formatted regular expression to use to identify whether a URL is this service's own URL or an external one. If your service only has one domain name set this to 'www.yourdomainname.gov.uk' (without the protocol). If you have more than one use something like '(?i)(www.domain1.gov.uk|www.domain2.gov.uk|www.domain3.gov.uk)'
        socialRefererDomainRegex: "(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|quora|wechat|weibo|whatsapp|tumblr|reddit|x.com|instagram|tiktok|messenger|telegram|douyin|qq|qzone|josh|teams|skype|tieba|threads|viber|imo|xiaohongshu|line|picsart|bluesky)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a social media site
        searchEngineRefererDomainRegex: "(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|entireweb|exalead|gigablast|hotbot|info.com|lycos|metacrawler|mojeek|qwant|searx|startpage|swisscows|webcrawler|yandex|yippy)", // re-2 formatted regular expression to use to work out whether an HTTP referer is a search enginer (regardless of whether paid or organic)
        funnelDepth: 10, // Number of steps forwards/backwards to analyse in funnels - higher allows deeper analysis, lower reduces CPU usage
        requestPathGroupingRegex: '[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*', // re2-formatted regular expression to replace with the string 'UID' when grouping request paths
        attributionParameters: ['utm_source', 'utm_campaign', 'utm_medium', 'utm_content', 'gclid', 'gcsrc', 'fbclid', 'dclid', 'gad_source', 'ds_rl', 'gbraid', 'msclkid'], // list of parameters to extract from the request_query array of structs at the beginning of funnels
        attributionDomainExclusionRegex: "(?i)(signin.education.gov.uk)", //re2-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which 
        expirationDays: null, // Number of days after which all data streamed by dfe-analytics or managed by dfe-analytics-dataform for a particular eventDataSource should be deleted. Must be either an integer value or false.
        webRequestEventExpirationDays: null, // the number of days after which web_request events should be deleted, along with data in tables generated from them by dfe-analytics-dataform
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

        enableAirbyteSource: false, // Master switch for Airbyte processing
        hasTimestamps: true,       // global default; set to false for non-Rails services without created_at/updated_at

        airbyteConfig: {
            datasetName: null, // name of the BigQuery dataset that Airbyte streams data into
            tablePrefix: '', // prefix for Airbyte table names (e.g., '_airbyte_raw_')
            tableSuffix: '_airbyte', // Suffix for Airbyte output tables (to distinguish from dfe-analytics)
            defaultPrimaryKeyField: 'id', // Default primary key field name (can be overridden per entity via tableSchema.primaryKey)
            
        },

        enabledAirbyteLegacyMerge: false,
        airbyteLegacyMergeCutoff: null,

        airbyteHeartbeat: {
                freshnessHours: 12, // Number of hours to wait before triggering an assertion failure, if no new data has been received from Airbyte
                datasetName: null, // name of the BigQuery dataset for heartbeat data.
                tableName: 'airbyte_heartbeat', // name of the heartbeat table
                disableFreshnessCheckDuringRange: false // Boolean. If true, disables the heartbeat freshness check assertion during the date ranges specified in assertionDisableDuringDateRanges
        },

        airbyteReconciliation: {
                enabled: false,                       // opt-in, like enableAirbyteSource itself
                minLiveFraction: 0.8,                 // snapshot mass threshold vs live rows
                maxDeleteFraction: 0.2,               // circuit breaker trip level
                minSnapshotAgeMinutes: 60,            // in-flight snapshot guard
                detectionWindowDays: 7,               // raw-table scan window (partition pruning)
                forceReconcileSnapshotLsn: null       // one-shot guard override; set, run, remove
        },

        ...params
    };

    params.airbyteReconciliation = {
        enabled: false,
        minLiveFraction: 0.8,
        maxDeleteFraction: 0.2,
        minSnapshotAgeMinutes: 60,
        detectionWindowDays: 7,
        forceReconcileSnapshotLsn: null,
        ...(params.airbyteReconciliation)
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

    // Build result object
    let result = {
        events: events(params),
        eventsDataNotFresh: eventsDataNotFresh(params),
        customEventDataNotFresh: customEventDataNotFresh(params),
        dfeAnalyticsConfiguration: dfeAnalyticsConfiguration(params),
        version
        };

    // EXISTING: dfe-analytics processing
    // Publish and return datasets - assertions first for quick access in the Dataform UI
    if (params.transformEntityEvents) {
        Object.assign(result, {
            entityDataNotFresh: entityDataNotFresh(params),
            entityTableCheckScheduled: entityTableCheckScheduled(params),
            entityTableCheckImport: entityTableCheckImport(params),
            entityIdsDoNotMatch: entityIdsDoNotMatch(params),
            entityImportIdsDoNotMatch: entityImportIdsDoNotMatch(params),
            pageviewWithFunnel: pageviewWithFunnel(params),
            sessions: sessions(params),
            session_details: session_details(params),
            entitiesAreMissingExpectedFields: entitiesAreMissingExpectedFields(params),
            unhandledFieldOrEntityIsBeingStreamed: unhandledFieldOrEntityIsBeingStreamed(params),
            unhandledCustomEventIsBeingStreamed: unhandledCustomEventIsBeingStreamed(params),
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
            pipelineTableSnapshot: pipelineTableSnapshot(version, params),
            pipelineSnapshot: pipelineSnapshot(version, params)
        });
    } else {
        Object.assign(result, {
            flattenedCustomEvent: flattenedCustomEvent(params),
            hiddenPIIConfigurationDoesNotMatchEventsStreamed: hiddenPIIConfigurationDoesNotMatchEventsStreamed(params),
            pageviewWithFunnel: pageviewWithFunnel(params),
            sessions: sessions(params),
            session_details: session_details(params),
            pipelineSnapshot: pipelineSnapshot(version, params)
        });
    }

    // Airbyte processing (only if enabled)
    if (params.enableAirbyteSource) {
        Object.assign(result, {
            airbyteEntityVersion: airbyteEntityVersion(params),
            airbyteEntityLatest: airbyteEntityLatest(params),
            airbyteEntityDataNotFresh: airbyteEntityDataNotFresh(params),
            airbyteGlobalDataFreshness: airbyteGlobalDataFreshness(params),
            airbyteSchemaAssertions: airbyteSchemaAssertions(params),
            airbyteReconciliation: airbyteReconciliation(params)
        });
    }

    return result;
}
