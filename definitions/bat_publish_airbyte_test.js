/* Test definition for Airbyte dual-run with data. Reads from ptt_airbyte_production (Airbyte source)
   NOTE: This runs ALONGSIDE the existing dfe-analytics pipeline, not instead of it.

   Covers Publish teacher training courses / Find postgraduate teacher training
   (one Rails app, one database, so one definitions file).
*/

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
    disabled: false,
    eventSourceName: "publish",
    bqDatasetName: "publish_api_events_production",
    bqEventsTableName: "events",
    urlRegex: "(?i)(publish-teacher-training-courses.service.gov.uk|find-postgraduate-teacher-training.service.gov.uk)",
    requestPathGroupingRegex: '[a-zA-Z0-9]{3}/[a-zA-Z0-9]{4}$',
    /* Find UIDs in URLs look like /course/3RT/5X3R and don't necessarily contain numbers, so override the default dfe-analytics-dataform request path grouping logic to look for three non-special characters followed by a forward slash followed by 4 non-special characters, and replace them with the string UID i.e. /course/UID */
    transformEntityEvents: true,
    compareChecksums: false,
    enableSessionTables: false,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
    expirationDays: false,
    enableMonitoring: false,

    // NEW: Enable Airbyte
    enableAirbyteSource: true,
    hasTimestamps: true,

    airbyteConfig: {
        datasetName: "ptt_airbyte_production",
        tablePrefix: "",
        tableSuffix: "_airbyte",
        defaultPrimaryKeyField: "id"
    },

    enabledAirbyteLegacyMerge: true,
    airbyteLegacyMergeCutoff: '2026-07-01',

    airbyteHeartbeat: {
        datasetName: "ptt_airbyte_production",
        freshnessHours: 12,
        tableName: 'airbyte_heartbeat',
        disableFreshnessCheckDuringRange: false
    },

    airbyteReconciliation: {
        enabled: true,
        minLiveFraction: 0.8,
        maxDeleteFraction: 0.2,
        minSnapshotAgeMinutes: 60,
        detectionWindowDays: 60
    },

    dataSchema: [{
            entityTableName: "contact",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "provider_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "provider"
            }, {
                keyName: "type",
                dataType: "string",
                description: ""
            }, {
                keyName: "permission_given",
                dataType: "boolean",
                description: ""
            }]
        },
        {
            entityTableName: "course",
            description: "",
            keys: [{
                keyName: "a_level_subject_requirements",
                dataType: "string",
                description: ""
            }, {
                keyName: "accept_a_level_equivalency",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accept_pending_a_level",
                dataType: "string",
                description: ""
            }, {
                keyName: "accept_english_gcse_equivalency",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accept_gcse_equivalency",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accept_maths_gcse_equivalency",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accept_pending_gcse",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accept_science_gcse_equivalency",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accredited_provider_code",
                pastKeyNames: ["accredited_body_code"],
                dataType: "string",
                description: ""
            }, {
                keyName: "additional_a_level_equivalencies",
                dataType: "string",
                description: ""
            }, {
                keyName: "additional_degree_subject_requirements",
                dataType: "string",
                description: ""
            }, {
                keyName: "additional_gcse_equivalencies",
                dataType: "string",
                description: ""
            }, {
                keyName: "age_range_in_years",
                dataType: "string",
                description: ""
            }, {
                keyName: "applications_open_from",
                dataType: "date",
                description: "Date that this course could accept applications from as set by the provider on the 'Applications open date' field within the basic details of the Publish course page. This does not necessarily mean that this course was findable on the Find postgraduate teacher training service on this date."
            }, {
                keyName: "application_status",
                dataType: "string",
                description: "Status of whether the course is open or closed. Note that this does not necessarily mean that the course is appliable - an appliable course must have an 'open' application status, be findable on Find (has been published and is not withdrawn), and has an applications_open_from date that has been met.",
                valueMappings: {
                    "0": "closed",
                    "1": "open"
                }
            }, {
                keyName: "changed_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "course_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "degree_type",
                dataType: "string",
                description: "Categorises the degree awarded on completion of a course as either postgraduate or undergraduate.",
                pastKeyNames: ["course_type"]
            }, {
                keyName: "degree_grade",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "two_one",
                    "1": "two_two",
                    "2": "third_class",
                    "9": "not_required"
                }
            }, {
                keyName: "degree_subject_requirements",
                dataType: "string",
                description: ""
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "english",
                dataType: "string",
                description: "",
                valueMappings: {
                    "1": "must_have_qualification_at_application_time",
                    "2": "expect_to_achieve_before_training_begins",
                    "3": "equivalence_test",
                    "9": "not_required"
                }
            }, {
                keyName: "funding",
                dataType: "string",
                description: "Funding type for the course. Either 'not set', 'fee', 'salary', 'apprenticeship',"
            }, {
                keyName: "is_send",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "level",
                dataType: "string",
                description: ""
            }, {
                keyName: "maths",
                dataType: "string",
                description: "",
                valueMappings: {
                    "1": "must_have_qualification_at_application_time",
                    "2": "expect_to_achieve_before_training_begins",
                    "3": "equivalence_test",
                    "9": "not_required"
                }
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "profpost_flag",
                dataType: "string",
                description: ""
            }, {
                keyName: "program_type",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "provider"
            }, {
                keyName: "publish_without_schools_allowed",
                dataType: "boolean",
                description: "TRUE if support has approved this course to be published without any schools attached, on the basis that candidates arrange their own placement. Only ever applies to salaried or apprenticeship courses, never fee-paying ones."
            }, {
                keyName: "school_experience_required",
                dataType: "boolean",
                description: "TRUE if the course requires the candidate to have prior school experience. Can only be set for salaried or apprenticeship courses, and is only surfaced to candidates from the 2027 recruitment cycle onwards."
            }, {
                keyName: "school_experience_required_content",
                dataType: "string",
                description: "Free text set by the provider describing the school experience the candidate needs. Must be present when school_experience_required is TRUE, and must be blank when it is FALSE."
            }, {
                keyName: "schools_validated",
                dataType: "boolean",
                description: "TRUE if the provider has validated the attached schools for the course (rolled-over period only)"
            }, {
                keyName: "qualification",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "qts",
                    "1": "pgce_with_qts",
                    "2": "pgde_with_qts",
                    "3": "pgce",
                    "4": "pgde",
                    "5": "undergraduate_degree_with_qts"
                }
            }, {
                keyName: "science",
                dataType: "string",
                description: "",
                valueMappings: {
                    "1": "must_have_qualification_at_application_time",
                    "2": "expect_to_achieve_before_training_begins",
                    "3": "equivalence_test",
                    "9": "not_required"
                }
            }, {
                keyName: "start_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "study_mode",
                dataType: "string",
                description: ""
            }, {
                keyName: "uuid",
                dataType: "string",
                description: "UUID of the course. Differs from course_id in that the course_id for a given course in Publish data will not match that of the same course in Apply data. UUID serves as a universal identified to link the the two."
            }, {
                keyName: "can_sponsor_skilled_worker_visa",
                dataType: "boolean",
                description: "TRUE if the provider of this course offers to sponsor a skilled worker visa for a candidate to do this course."
            }, {
                keyName: "can_sponsor_student_visa",
                dataType: "boolean",
                description: "TRUE if the provider of this course offers to sponsor a student visa for a candidate to do this course."
            }, {
                keyName: "visa_sponsorship_application_deadline_at",
                dataType: "timestamp",
                description: "Deadline for submitting a visa sponsorship application."
            }, {
                keyName: "first_published_at",
                pastKeyNames: ["first_published_date", "first_published_datetime"],
                dataType: "timestamp",
                description: "The timestamp that the course was first published at."
            
            }]
        },
        {
            entityTableName: "course_enrichment",
            description: "",
            keys: [{
                keyName: "course_id",
                dataType: "string",
                description: "UID of this course from the database. To match up with Apply data, use UUID.",
                foreignKeyTable: "course"
            }, {
                keyName: "created_by_user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "user"
            }, {
                keyName: "json_data",
                dataType: "string",
                description: ""
            }, {
                keyName: "last_published_timestamp_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "status",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "draft",
                    "1": "published",
                    "2": "rolled_over",
                    "3": "withdrawn"
                }
            }, {
                keyName: "updated_by_user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "user"
            }, {
                keyName: "version",
                dataType: "string",
                description: "Version of the enrichment schema"
            }]
        },
        {
            entityTableName: "course_site",
            description: "",
            keys: [{
                keyName: "course_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "course"
            }, {
                keyName: "publish",
                dataType: "string",
                description: ""
            }, {
                keyName: "site_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "site"
            }, {
                keyName: "status",
                dataType: "string",
                description: ""
            }, {
                keyName: "vac_status",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "course_subject",
            description: "",
            keys: [{
                keyName: "course_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "course"
            }, {
                keyName: "position",
                dataType: "integer",
                description: ""
            }, {
                keyName: "subject_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "subject"
            }]
        },
        {
            entityTableName: "financial_incentive",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "subject_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "subject"
            }, {
                keyName: "bursary_amount",
                dataType: "integer",
                description: ""
            }, {
                keyName: "displayed",
                dataType: "boolean",
                description: "TRUE if this financial incentive is shown to candidates on Find."
            }, {
                keyName: "early_career_payments",
                dataType: "string",
                description: ""
            }, {
                keyName: "non_uk_bursary_eligible",
                dataType: "boolean",
                description: "TRUE if candidates without UK residency are eligible for the bursary on this subject."
            }, {
                keyName: "non_uk_scholarship_eligible",
                dataType: "boolean",
                description: "TRUE if candidates without UK residency are eligible for the scholarship on this subject."
            }, {
                keyName: "scholarship",
                dataType: "integer",
                description: ""
            }, {
                keyName: "subject_knowledge_enhancement_course_available",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "year",
                dataType: "integer",
                description: "Recruitment cycle year that this financial incentive applies to e.g. 2025 for ITT2025-26."
            }]
        },
        {
            entityTableName: "provider",
            description: "",
            keys: [{
                keyName: "accredited",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "accredited_provider_number",
                dataType: "string",
                description: ""
            }, {
                keyName: "address1",
                dataType: "string",
                description: ""
            }, {
                keyName: "address2",
                dataType: "string",
                description: ""
            }, {
                keyName: "address3",
                dataType: "string",
                description: ""
            }, {
                keyName: "address4",
                dataType: "string",
                description: ""
            }, {
                keyName: "town",
                dataType: "string",
                description: ""
            }, {
                keyName: "can_sponsor_skilled_worker_visa",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "can_sponsor_student_visa",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "changed_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "latitude",
                dataType: "float",
                description: ""
            }, {
                keyName: "longitude",
                dataType: "float",
                description: ""
            }, {
                keyName: "postcode",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_name",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_type",
                dataType: "string",
                description: ""
            }, {
                keyName: "value_proposition",
                dataType: "string",
                description: "How a provider describes their value to a candidate",
            }, {
                keyName: "recruitment_cycle_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "recruitment_cycle"
            }, {
                keyName: "region_code",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "no_region",
                    "1": "london",
                    "2": "south_east",
                    "3": "south_west",
                    "4": "wales",
                    "5": "west_midlands",
                    "6": "east_midlands",
                    "7": "eastern",
                    "8": "north_west",
                    "9": "yorkshire_and_the_humber",
                    "10": "north_east",
                    "11": "scotland"
                }
            }, {
                keyName: "selectable_school",
                dataType: "boolean",
                description: "",
            }, {
                keyName: "about_us",
                dataType: "string",
                description: "How a provider describes itself to a candidate",
            }, {
                keyName: "train_with_disability",
                dataType: "string",
                description: ""
            }, {
                keyName: "train_with_us",
                dataType: "string",
                description: ""
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: ""
            }, {
                keyName: "urn",
                dataType: "string",
                description: ""
            }, {
                keyName: "website",
                dataType: "string",
                description: ""
            }, {
                keyName: "year_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "synonyms",
                dataType: "string",
                isArray: true,
                description: "Comma-separated list of alternative names by which this provider is known."
            }
            ]
        },
        {
            entityTableName: "recruitment_cycle",
            description: "",
            keys: [{
                keyName: "year",
                dataType: "integer",
                description: "First year that falls within this recruitment cycle e.g. 2090 for ITT2090-91",
                alias: "recruitment_cycle_year"
            }, {
                keyName: "application_start_date",
                dataType: "date",
                description: "First date in this recruitment cycle year.",
                alias: "recruitment_cycle_start_date"
            }, {
                keyName: "application_end_date",
                dataType: "date",
                description: "Last date in this recruitment cycle year.",
                alias: "recruitment_cycle_end_date"
            }, {
                keyName: "available_in_publish_from",
                dataType: "date",
                description: "The date that the recruitment cycle is available in publish"
            }, {
                keyName: "available_for_support_users_from",
                dataType: "date",
                description: "The date when next-cycle course data becomes accessible to support users in both the Support and Publish interfaces. This early access allows support users to manage and review course information during the course rollover process, before it is made available to all Publish users."
            }]
        },
        {
            entityTableName: "site",
            description: "",
            keys: [{
                keyName: "added_via",
                dataType: "string",
                description: "Interface used to create sites. 'publish_interface' for sites created with standard interface. 'register_import' for sites created using Register school importer"
            }, {
                keyName: "address1",
                dataType: "string",
                description: ""
            }, {
                keyName: "address2",
                dataType: "string",
                description: ""
            }, {
                keyName: "address3",
                dataType: "string",
                description: ""
            }, {
                keyName: "address4",
                dataType: "string",
                description: ""
            }, {
                keyName: "discarded_via_script",
                dataType: "string",
                description: "Discarded schools in Publish UI for users during the rollover period."
            }, {
                keyName: "code",
                dataType: "string",
                description: ""
            }, {
                keyName: "latitude",
                dataType: "float",
                description: ""
            }, {
                keyName: "location_name",
                dataType: "string",
                description: ""
            }, {
                keyName: "longitude",
                dataType: "float",
                description: ""
            }, {
                keyName: "postcode",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "provider"
            }, {
                keyName: "region_code",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "no_region",
                    "1": "london",
                    "2": "south_east",
                    "3": "south_west",
                    "4": "wales",
                    "5": "west_midlands",
                    "6": "east_midlands",
                    "7": "eastern",
                    "8": "north_west",
                    "9": "yorkshire_and_the_humber",
                    "10": "north_east",
                    "11": "scotland"
                }
            }, {
                keyName: "town",
                dataType: "string",
                description: ""
            }, {
                keyName: "urn",
                dataType: "string",
                description: ""
            }, {
                keyName: "site_type",
                dataType: "string",
                description: "",
                valueMappings: {
                    "0": "school",
                    "1": "study_site"
                }
            }]
        },
        {
            entityTableName: "statistic",
            description: "",
            dataFreshnessDays: 2,
            keys: [{
                keyName: "json_data",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "study_site_placement",
            description: "",
            dataFreshnessDays: 3,
            dataFreshnessDisableDuringRange: true,
            keys: [{
                keyName: "course_id",
                dataType: "string",
                description: "UID of the trainees chosen course",
                foreignKeyTable: "course"
            }, {
                keyName: "site_id",
                dataType: "string",
                description: "UID of the trainees study site",
                foreignKeyTable: "site"
            }]
        },
        {
            entityTableName: "subject_area",
            description: "",
            materialisation: "view",
            primaryKey: "typename",
            keys: [{
                keyName: "typename",
                dataType: "string",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "subject",
            description: "",
            keys: [
            {
                keyName: "match_synonyms",
                dataType: "string",
                isArray: true,
                description: "Comma-separated list used to match a list of aliases for a subject - the aliases could be abbreviation, other names, even codes or any significant value that was heavily searched on Find - for any given subject. Some subjects might have empty others many values."
            },
            {
                keyName: "type",
                dataType: "string",
                description: ""
            }, {
                keyName: "subject_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "subject_name",
                dataType: "string",
                description: ""
            }, {
                keyName: "subject_group_id",
                dataType: "string",
                description: "UID of the subject group for this subject",
                foreignKeyTable: "subject_group"
            }]
        },
        {
            entityTableName: "subject_group",
            description: "Table of subject groups associated with each subject in the Publish database",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "Name of the subject group"
            }]
        },
        {
            entityTableName: "user_notification",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "user"
            }, {
                keyName: "provider_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "course_update",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "course_publish",
                dataType: "boolean",
                description: ""
            }]
        },
        {
            entityTableName: "user_permission",
            description: "Many to many relationship between user and provider. If a row is present then a user has access to manage/publish courses on behalf of a provider.",
            keys: [{
                keyName: "user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "user"
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "provider"
            }]
        },
        {
            entityTableName: "user",
            description: "",
            keys: [{
                keyName: "accept_terms_date_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "admin",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "first_login_date_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "invite_date_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "last_login_date_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "magic_link_token_sent_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "state",
                dataType: "string",
                description: ""
            }, {
                keyName: "welcome_email_date_utc",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "sign_in_user_id",
                dataType: "string",
                description: "DfE Sign-in UID. In its raw un-anonymised state it can be joined on to the dfe_sign_in_uid fields in the Register users table and Apply provider_users and support_users tables.",
                hidden: true
            }, {
                keyName: "first_name",
                dataType: "string",
                description: "First name of the user.",
                hidden: true
            }, {
                keyName: "last_name",
                dataType: "string",
                description: "Last name of the user",
                hidden: true
            }, {
                keyName: "email",
                dataType: "string",
                description: "Email address of the user.",
                hidden: true
            }]
        },
        {
            entityTableName: "gias_school",
            description: "School data imported from Get Information About Schools.",
            materialisation: "view",
            keys: [{
                keyName: "address1",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "address2",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "address3",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "county",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "group_code",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "latitude",
                dataType: "float",
                description: "Geographical latitude of the school"
            }, {
                keyName: "longitude",
                dataType: "float",
                description: "Geographical longitude of the school"
            }, {
                keyName: "maximum_age",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "minimum_age",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "name",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "phase_code",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "status_code",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "telephone",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "town",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "type_code",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "urn",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "website",
                dataType: "string",
                description: "See https://www.get-information-schools.service.gov.uk/glossary"
            }, {
                keyName: "region_code",
                dataType: "string",
                description: "",
            }]
        },
        {
            entityTableName: "provider_partnership",
            description: "This table maintains the relationships between training providers and accredited providers.",
            keys: [{
                keyName: "accredited_provider_id",
                dataType: "string",
                description: "UID of the accredited provider"
            }, {
                keyName: "training_provider_id",
                dataType: "string",
                description: "UID of the training provider"
            }]
        },
        {
            entityTableName: "feedback",
            description: "This table stores user-submitted feedback about their experience using Find service. Each record captures a user's opinion of ease of use and their qualitative experience, along with timestamps.",
            keys: [{
                keyName: "ease_of_use",
                dataType: "string",
                description: "User's opinion or comment on how easy the service was to use It can be (Very easy, Easy, Neither easy nor difficult, Difficult, Very difficult)."
            }, {
                keyName: "experience",
                dataType: "string",
                description: "User's written feedback or comments about their overall experience."
            }]
        },
        {
            entityTableName: "data_hub_process_summary",
            description: "This table stores a summary of each data process run (e.g. imports, syncs)It is used to track, audit, and report on the outcome of key data operations like Register School Imports or GIAS sync tasks and any other big data process we do to improve our traceability and investigations of the incoming data.",
            keys: [{
                keyName: "finished_at",
                dataType: "timestamp",
                description: "Process completed at"
            }, {
                keyName: "started_at",
                dataType: "timestamp",
                description: "Process started at"
            }, {
                keyName: "status",
                dataType: "string",
                description: "Marks job outcome (e.g. completed, failed) - more will be added over time"
            }, {
                keyName: "short_summary",
                dataType: "string",
                description: "Key metrics (counts, highlights)"
            }, {
                keyName: "type",
                dataType: "string",
                description: "Enables STI- identifies what kind of process"
            }]
        },
        {
            entityTableName: "candidate",
            description: "This table stores records representing users who have created an account in Find. Each record represents one candidate account.",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "Unique identifier for the candidate.",
                alias: "candidate_id"
            }, {
                keyName: "email_address",
                dataType: "string",
                description: "Email address for candidate",
                hidden: true
            }]
        },
        {
            entityTableName: "authentication",
            description: "This table stores records of users authentication.",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "UID for the authentication record.",
                alias: "authentication_id"
            }, {
                keyName: "provider",
                dataType: "string",
                description: "Authentication provider used",
                valueMappings: {
                    "0": "developer",
                    "1": "govuk_one_login",
                    "2": "dfe_signin"
                }
            }, {
                keyName: "subject_key",
                dataType: "string",
                description: "Subject identifier from the provider",
                hidden: true
            }, {
                keyName: "authenticable_id",
                dataType: "integer",
                description: "UID of the authenticable entity"
            }, {
                keyName: "authenticable_type",
                dataType: "string",
                description: "Type of authenticable entity"
            }]
        },
        {
            entityTableName: "saved_course",
            description: "This table tracks which courses have been saved (bookmarked) by which candidates.",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "UID for the saved course record.",
                alias: "saved_course_id"
            }, {
                keyName: "course_id",
                dataType: "string",
                description: "UID for the course",
                foreignKeyTable: "course"
            }, {
                keyName: "candidate_id",
                dataType: "string",
                description: "UID for the candidate"
            }, {
                keyName: "note",
                dataType: "string",
                description: "A free text field up to 100 words which allows candidates to add personal notes against a saved course "
            }]
        },
        {
            entityTableName: "candidate_email_alerts",
            description: "This table stores email alert preferences and records for candidates who have signed up to receive notifications about courses matching their criteria.",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "UID for the email alert record.",
                alias: "candidate_email_alert_id"
            }, {
                keyName: "candidate_id",
                dataType: "string",
                description: "UID of the candidate who set up the alert.",
                foreignKeyTable: "candidate"
            }, {
                keyName: "subjects",
                dataType: "string",
                description: "Subjects the candidate has selected to receive alerts for."
            }, {
                keyName: "longitude",
                dataType: "float",
                description: "Geographical longitude of the candidate's chosen search location."
            }, {
                keyName: "latitude",
                dataType: "float",
                description: "Geographical latitude of the candidate's chosen search location."
            }, {
                keyName: "radius",
                dataType: "float",
                description: "Search radius in miles around the candidate's chosen location."
            }, {
                keyName: "search_attributes",
                dataType: "string",
                description: "Additional search filter attributes used to match courses for this alert."
            }, {
                keyName: "location_name",
                dataType: "string",
                description: "Name of the candidate's chosen search location."
            }, {
                keyName: "last_sent_at",
                dataType: "timestamp",
                description: "Timestamp when the alert was last sent to the candidate."
            }, {
                keyName: "unsubscribed_at",
                dataType: "timestamp",
                description: "Current status of the Timestamp when the candidate unsubscribed from this alert, if applicable."

            }]
        },
        {
            entityTableName: "session",
            description: "This table stores session records for authenticated entities.",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "UID for the session record.",
                alias: "session_id"
            }, {
                keyName: "user_agent",
                dataType: "string",
                description: "The browser or client used to establish the session.",
                foreignKeyTable: "course"
            }, {
                keyName: "ip_address",
                dataType: "string",
                description: "IP address from which the session was initiated.",
                hidden: true
            }, {
                keyName: "session_key",
                dataType: "string",
                description: "Unique key for identifying the session."
            }, {
                keyName: "sessionable_id",
                dataType: "integer",
                description: "Reference ID of the entity associated with the session."
            }, {
                keyName: "sessionable_type",
                dataType: "string",
                description: "Type of entity the session belongs to."
            }]
        },
        {
            entityTableName: "providers_onboarding_form_request",
            description: "Table collecting organisation details for the onboarding of new providers",
            keys: [{
                keyName: "status",
                dataType: "string",
                description: "The onboarding request can have the following statuses. Pending means the form has been created and sent to the provider. Submitted means the provider has completed and returned the form. Expired means the form is no longer valid. Closed means the support team has actioned the request. Rejected means the support team has declined the request."
            }, {
                keyName: "form_name",
                dataType: "string",
                description: "Name given to the request/form by the support team when generating an onboarding form",
            }, {
                keyName: "zendesk_link",
                dataType: "string",
                description: "Zendesk link to the provider request to be onboarded"
            }, {
                keyName: "uuid",
                dataType: "string",
                description: "Provider UUID",
            }, {
                keyName: "provider_metadata",
                dataType: "string",
                description: "Provider metadata",
            }, {
                keyName: "email_address",
                dataType: "string",
                description: "email of the first user for Publish for this provider",
                hidden: true
            }, {
                keyName: "first_name",
                dataType: "string",
                description: "First name of initial user",
                hidden: true
            }, {
                keyName: "last_name",
                dataType: "string",
                description: "Last name of initial user",
                hidden: true
            }, {
                keyName: "provider_name",
                dataType: "string",
                description: "Name of provider to be onboarded (operating name)"
            }, {
                keyName: "address_line_1",
                dataType: "string",
                description: "Address line 1 of organisation"
            }, {
                keyName: "address_line_2",
                dataType: "string",
                description: "Address line 2 of organisation"
            }, {
                keyName: "address_line_3",
                dataType: "string",
                description: "Address line 3 of organisation"
            }, {
                keyName: "town_or_city",
                dataType: "string",
                description: "Town of organisation"
            }, {
                keyName: "county",
                dataType: "string",
                description: "County of organisation"
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "Postcode of organisation"
            }, {
                keyName: "telephone",
                dataType: "string",
                description: "Telephone number of organisation"
            }, {
                keyName: "contact_email_address",
                dataType: "string",
                description: "Contact email of the organisation"
            }, {
                keyName: "website",
                dataType: "string",
                description: "Website of the provider"
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: "UK provider reference number"
            }, {
                keyName: "accredited_provider",
                dataType: "boolean",
                description: "True if the provider is accredited"
            }, {
                keyName: "urn",
                dataType: "string",
                description: "Unique reference number",
            }, {
                keyName: "support_agent_id",
                dataType: "string",
                description: "Admin user who is assigned to the request during form creation (linked to User table)"
            }]
        }
    ],

    customEventSchema: [{
            eventType: "search_results",
            description: "",
            keys: []
        }, {
            eventType: "removed_saved_course",
            description: "Occasions when a candidate removes a saved course",
            keys: []
        }, {
            eventType: "saved_course",
            description: "Occasions when a candidate saves a course",
            keys: []
        },
        {
            eventType: "track_click",
            description: "This table captures custom event data related to the number of tracked clicks within the Find and Publish services. Developers have specified which links are tracked in the code base. If you require specific links to be tracked, please reach out to the developers to enable tracking for those links in this table. Each row represents a single tracked click on a page in Find or Publish.",
            keys: []
        },
        {
            eventType: "candidate_applies",
            description: "Captures a custom event related to candidates clicking on a button which sends them to the Apply service from the Find service. Developers have specified which links are tracked in the code base. ",
            keys: []
        },
        {
            eventType: "candidate_note_created",
            description: "Custom table event of each candidate note created",
            keys: []
        },
        {
            eventType: "candidate_note_deleted",
            description: "Custom table event of each candidate note deleted",
            keys: []
        },
        {
            eventType: "candidate_note_updated",
            description: "Custom table event of each candidate note updated",
            keys: []
        },
        {
            eventType: "candidate_note_undone",
            description: "Custom table event of each candidate note deletion that is undone by the candidate",
            keys: []
        }
    ]
});