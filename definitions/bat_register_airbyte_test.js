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
    transformEntityEvents: false,
    compareChecksums: false,
    enableSessionTables: false,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
    expirationDays: false,
    enableMonitoring: false,
    
    // NEW: Enable Airbyte
    enableAirbyteSource: true,
    
    airbyteConfig: {
        datasetName: "rtt_airbyte_production",
        tablePrefix: "",                        
        outputSuffix: "_airbyte",               
        primaryKeyField: "id",                   
        changeDetectionStrategy: "content_hash",
    },
    
    airbyteEnableVersioning: true,
    airbyteEnableAssertions: true,
    
    // Data schema (small subset for testing)
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
        },
        {
            entityTableName: "courses",
            description: "",
            keys: [{
                keyName: "accredited_body_code",
                dataType: "string",
                description: "code for the accredited body of a course"
            }, {
                keyName: "code",
                dataType: "string",
                description: "course code",
                alias: "course_code"
            }, {
                keyName: "course_length",
                dataType: "string",
                description: "length of the course"
            }, {
                keyName: "duration_in_years",
                dataType: "integer",
                description: "duration of the course in years"
            }, {
                keyName: "level",
                dataType: "string",
                description: "level of the course"
            }, {
                keyName: "min_age",
                dataType: "integer",
                description: "minimum teaching age"
            }, {
                keyName: "max_age",
                dataType: "integer",
                description: "maximum teaching age"
            }, {
                keyName: "qualification",
                dataType: "string",
                description: "qualification aim of the course"
            }, {
                keyName: "name",
                dataType: "string",
                description: "name of the course"
            }, {
                keyName: "recruitment_cycle_year",
                dataType: "integer",
                description: "recruitment cycle year for a course"
            }, {
                keyName: "route",
                dataType: "string",
                description: "ITT route"
            }, {
                keyName: "full_time_start_date",
                dataType: "date",
                description: "Full time start date"
            }, {
                keyName: "full_time_end_date",
                dataType: "date",
                description: "Full time end date"
            }, {
                keyName: "part_time_start_date",
                dataType: "date",
                description: "Part time start date"
            }, {
                keyName: "part_time_end_date",
                dataType: "date",
                description: "Part time end date"
            }, {
                keyName: "published_start_date",
                dataType: "date",
                description: "Published start date"
            }, {
                keyName: "study_mode",
                dataType: "string",
                description: "study mode, e.g. full-time or part-time"
            }, {
                keyName: "summary",
                dataType: "string",
                description: "summary description of the course"
            }, {
                keyName: "uuid",
                dataType: "string",
                description: "unique identifier for a course"
            }]
        }
    ]
});