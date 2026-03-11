/* Test definition for Airbyte dual-run with data. Reads from rtt_airbyte_production (Airbyte source)
   NOTE: This runs ALONGSIDE the existing dfe-analytics pipeline, not instead of it.
*/

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
    disabled: false,
    eventSourceName: "register",
    bqDatasetName: "register_events_production",
    bqEventsTableName: "events",
    urlRegex: "register-trainee-teachers.service.gov.uk",
    transformEntityEvents: true,
    compareChecksums: false,
    enableSessionTables: false,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
    expirationDays: false,
    
    // --- NEW: Enable Airbyte ---
    enableAirbyteSource: true,
    
    airbyteConfig: {
        datasetName: "rtt_airbyte_production",
        tablePrefix: "",                        
        outputSuffix: "_airbyte",               
        primaryKeyField: "id",                   
        changeDetectionStrategy: "content_hash",
    },
    
    airbyteEnableVersioning: true,
    airbyteEnableFieldUpdates: true,
    airbyteEnableAssertions: true,
    
    // --- Data schema (start with a small subset for testing!) ---
    dataSchema: [{
            entityTableName: "academic_cycles",
            description: "",
            keys: [{
                keyName: "start_date",
                dataType: "date",
                description: "Start date of academic cycle"
            }, {
                keyName: "end_date",
                dataType: "date",
                description: "End date of academic cycle"
            }]
        },
        {
            entityTableName: "activities",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "action_name",
                dataType: "string",
                description: "action activity applies to"
            }, {
                keyName: "controller_name",
                dataType: "string",
                description: "controller activity applies to"
            }, {
                keyName: "metadata",
                dataType: "string",
                description: "contains action, format, subject, controller & training_route"
            }, {
                keyName: "user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "users"
            }]
        },
        {
            entityTableName: "allocation_subjects",
            description: "",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "Name of the allocation subject"
            }]
        },
        {
            entityTableName: "apply_applications",
            description: "Link between a trainee and an application form in Apply, if and only if a SCITT successfully imported data from Apply when they started registering this trainee in Register. Because of this this table is *not* suitable for use to join all trainees in Register on to corresponding candidates/application forms in Apply.",
            keys: [{
                keyName: "accredited_body_code",
                dataType: "string",
                description: "accredited body code for the Apply application"
            }, {
                keyName: "recruitment_cycle_year",
                dataType: "integer",
                description: "recruitment cycle year the application relates to"
            }, {
                keyName: "state",
                dataType: "string",
                description: "state code for the application"
            }, {
                keyName: "apply_id",
                dataType: "string",
                description: "Application form ID in Apply, if and only if a SCITT successfully imported data from Apply when they started registering this trainee in Register. Because of this this field is *not* suitable for use to join all trainees in Register on to corresponding candidates/application forms in Apply."
            }]
        }
    ]
});