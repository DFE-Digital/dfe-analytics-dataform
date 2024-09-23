/* For use by dfe-analytics-dataform developers working with Register test data only - for example code to use in your project, see definitions/example.js */

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
    disabled: true,
    eventSourceName: "register",
    bqDatasetName: "register_events_production",
    bqEventsTableName: "events",
    urlRegex: "register-trainee-teachers.service.gov.uk",
    transformEntityEvents: true,
    compareChecksums: false,
    enableSessionTables: false,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
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
        },
        {
            entityTableName: "bulk_update_row_errors",
            materialisation: "view",
            description: "Errors returned to provider users when they attempted to upload a spreadsheet of updates to trainees' data when making recommendations about whether and when they should be awarded Qualified Teacher Status (QTS). Each row represents one error with one row in the uploaded spreadsheet. Each row in the spreadsheet could have multiple errors.",
            keys: [{
                keyName: "errored_on_id",
                dataType: "string",
                description: "ID of the row in the uploaded spreadsheet that this error refers to. Foreign key for the bulk_update_recommendations_uploads table which is not currently available in BigQuery."
            }, {
                keyName: "errored_on_type",
                dataType: "string",
                description: "The type of the error."
            }, {
                keyName: "message",
                dataType: "string",
                description: "Message describing the error in detail."
            }]
        },
        {
            entityTableName: "course_subjects",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "subject_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "subjects"
            }, {
                keyName: "course_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "courses"
            }]
        },
        {
            entityTableName: "courses",
            description: "",
            hidePrimaryKey: false,
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
                description: "level of the course",
                hidden: false
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
                description: "Full time start date",
                hidden: false
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
                keyName: "start_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "study_mode",
                dataType: "string",
                description: "study mode, e.g. full-time or part-time"
            }, {
                keyName: "summary",
                dataType: "string",
                description: "summary description of the course",
                hidden: false
            }, {
                keyName: "uuid",
                dataType: "string",
                description: "unique identifier for a course"
            }]
        },
        {
            entityTableName: "degrees",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "country",
                dataType: "string",
                description: ""
            }, {
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "grade",
                dataType: "string",
                description: ""
            }, {
                keyName: "grade_uuid",
                dataType: "string",
                description: "A universal unique identifier that will map to the reference data on grades"
            }, {
                keyName: "graduation_year",
                dataType: "integer",
                description: ""
            }, {
                keyName: "institution",
                dataType: "string",
                description: ""
            }, {
                keyName: "institution_uuid",
                dataType: "string",
                description: ""
            }, {
                keyName: "locale_code",
                dataType: "string",
                description: ""
            }, {
                keyName: "non_uk_degree",
                dataType: "string",
                description: ""
            }, {
                keyName: "other_grade",
                dataType: "string",
                description: ""
            }, {
                keyName: "slug",
                dataType: "string",
                description: ""
            }, {
                keyName: "subject",
                dataType: "string",
                description: ""
            }, {
                keyName: "subject_uuid",
                dataType: "string",
                description: "A universal unique identifier that will map to the reference data on subjects"
            }, {
                keyName: "trainee_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "trainees"
            }, {
                keyName: "uk_degree",
                dataType: "string",
                description: ""
            }, {
                keyName: "uk_degree_uuid",
                dataType: "string",
                description: "A universal unique identifier that will map to the reference data on UK degrees"
            }]
        },
        {
            entityTableName: "disabilities",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "id",
                dataType: "string",
                description: "Same as id",
                alias: "disability_id"
            }]
        },
        {
            entityTableName: "dqt_trn_requests",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "request_id",
                dataType: "string",
                description: "unique request id"
            }, {
                keyName: "response",
                dataType: "string",
                description: "a JSON field in Register, this contains the key / value pair responses from DQT"
            }, {
                keyName: "state",
                dataType: "string",
                description: "state of the trn request. 0 = requested, 1 = received"
            }, {
                keyName: "trainee_id",
                dataType: "string",
                description: "trainee id that this request relates to",
                foreignKeyTable: "trainees"
            }]
        },
        {
            entityTableName: "dttp_providers",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "dttp_schools",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "status_code",
                dataType: "integer",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "urn",
                dataType: "string",
                description: ""
            }, {
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "funding_method_subjects",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "allocation_subject_id",
                dataType: "string",
                description: "allocation subject identifier",
                foreignKeyTable: "allocation_subjects"
            }, {
                keyName: "funding_method_id",
                dataType: "string",
                description: "funding method identifier",
                foreignKeyTable: "funding_methods"
            }]
        },
        {
            entityTableName: "funding_methods",
            description: "",
            keys: [{
                keyName: "amount",
                dataType: "integer",
                description: "amount of funding"
            }, {
                keyName: "training_route",
                dataType: "string",
                description: "training_route funding applies to"
            }, {
                keyName: "funding_type",
                dataType: "string",
                description: "type of funding"
            }, {
                keyName: "academic_cycle_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "academic_cycles"
            }]
        },
        {
            entityTableName: "hesa_metadata",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "course_programme_title",
                dataType: "string",
                description: "HESA course_programme_title"
            }, {
                keyName: "fundability",
                dataType: "string",
                description: "HESA fundability"
            }, {
                keyName: "itt_aim",
                dataType: "string",
                description: "HESA itt_aim"
            }, {
                keyName: "itt_qualification_aim",
                dataType: "string",
                description: "HESA itt_qualification_aim"
            }, {
                keyName: "placement_school_urn",
                dataType: "string",
                description: "HESA placement_school_urn"
            }, {
                keyName: "study_length",
                dataType: "integer",
                description: "HESA study_length"
            }, {
                keyName: "service_leaver",
                dataType: "string",
                description: "HESA service_leaver"
            }, {
                keyName: "pg_apprenticeship_start_date",
                dataType: "date",
                description: "HESA pg_apprenticeship_start_date"
            }, {
                keyName: "study_length_unit",
                dataType: "string",
                description: "HESA study_length_unit"
            }, {
                keyName: "trainee_id",
                dataType: "string",
                description: "HESA trainee_id",
                foreignKeyTable: "trainees"
            }, {
                keyName: "year_of_course",
                dataType: "string",
                description: "HESA year_of_course"
            }]
        },
        {
            entityTableName: "hesa_trn_submissions",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "submitted_at",
                dataType: "timestamp",
                description: ""
            }]
        },
        {
            entityTableName: "hesa_collection_requests",
            description: "",
            materialisation: "view",
            keys: [{
                keyName: "requested_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "state",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "lead_school_users",
            description: "",
            keys: [{
                keyName: "lead_school_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "schools"
            }, {
                keyName: "user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "users"
            }]
        },
        {
            entityTableName: "nationalisations",
            description: "",
            keys: [{
                keyName: "nationality_id",
                dataType: "string",
                description: "allocation subject identifier",
                foreignKeyTable: "nationalities"
            }, {
                keyName: "trainee_id",
                dataType: "string",
                description: "trainee identifier",
                foreignKeyTable: "trainees"
            }]
        },
        {
            entityTableName: "nationalities",
            description: "",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "name of nationality"
            }]
        },
        {
            entityTableName: "placements",
            description: "",
            keys: [{
                keyName: "address",
                dataType: "string",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "postcode",
                dataType: "string",
                description: ""
            }, {
                keyName: "school_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "schools"
            }, {
                keyName: "trainee_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "trainees"
            }, {
                keyName: "urn",
                dataType: "string",
                description: ""
            }, {
                keyName: "slug",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "provider_users",
            description: "",
            keys: [{
                keyName: "provider_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "providers"
            }, {
                keyName: "user_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "users"
            }]
        },
        {
            entityTableName: "providers",
            description: "",
            keys: [{
                keyName: "accreditation_id",
                dataType: "string",
                description: "accreditation id allocated when a body becomes accredited"
            }, {
                keyName: "code",
                dataType: "string",
                description: ""
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: "Timestamp at which a provider was discarded"
            }, {
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: ""
            }, {
                keyName: "apply_sync_enabled",
                dataType: "boolean",
                description: ""
            }]
        },
        {
            entityTableName: "schools",
            description: "",
            keys: [{
                keyName: "open_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "close_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "town",
                dataType: "string",
                description: ""
            }, {
                keyName: "postcode",
                dataType: "string",
                description: ""
            }, {
                keyName: "urn",
                dataType: "string",
                description: ""
            }, {
                keyName: "lead_school",
                dataType: "boolean",
                description: ""
            }]
        },
        {
            entityTableName: "subject_specialisms",
            description: "",
            keys: [{
                keyName: "allocation_subject_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "allocation_subjects"
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }, {
                keyName: "hecos_code",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "subjects",
            description: "",
            keys: [{
                keyName: "code",
                dataType: "string",
                description: ""
            }, {
                keyName: "name",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "trainees",
            description: "",
            dataFreshnessDays: 3,
            keys: [{
                keyName: "apply_application_id",
                dataType: "string",
                description: "Foreign key to apply_applications identifier register_apply_applications .id",
                foreignKeyTable: "apply_applications"
            }, {
                keyName: "application_choice_id",
                dataType: "string",
                description: "The id of application choice. Foreign key connecting to apply_applications.apply_id",
                foreignKeyTable: "apply_applications",
                foreignKeyName: "apply_id"
            }, {
                keyName: "applying_for_bursary",
                dataType: "string",
                description: "Trainee is applying for a bursary (true) or not (false)"
            }, {
                keyName: "applying_for_grant",
                dataType: "string",
                description: "Trainee is applying for a grant (true) or not (false)"
            }, {
                keyName: "applying_for_scholarship",
                dataType: "string",
                description: "Trainee is applying for a scholarship (true) or not (false)"
            }, {
                keyName: "awarded_at",
                dataType: "timestamp",
                description: "Date QTS or EYTS was awarded"
            }, {
                keyName: "bursary_tier",
                dataType: "string",
                description: "Bursary tier. Only available for years where bursaries were paid on a tiered basis"
            }, {
                keyName: "commencement_status",
                dataType: "string",
                description: "Indicates if trainee started on time (0), late (1), or have not started yet (2)"
            }, {
                keyName: "trainee_start_date",
                dataType: "date",
                description: "Date the trainee started their ITT course"
            }, {
                keyName: "course_allocation_subject_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "allocation_subjects"
            }, {
                keyName: "course_education_phase",
                dataType: "string",
                description: "Indicates if a course is primary (0) or secondary (1)"
            }, {
                keyName: "course_min_age",
                dataType: "string",
                description: "Lower age range for course"
            }, {
                keyName: "course_max_age",
                dataType: "string",
                description: "Upper age range for course"
            }, {
                keyName: "course_subject_one",
                dataType: "string",
                description: "Course subject on which allocation subject is based"
            }, {
                keyName: "course_subject_two",
                dataType: "string",
                description: "Additional course subject"
            }, {
                keyName: "course_subject_three",
                dataType: "string",
                description: "Additional course subject"
            }, {
                keyName: "course_uuid",
                dataType: "string",
                description: "Foreign key to courses entity uuid, register_courses.uuid"
            }, {
                keyName: "defer_date",
                dataType: "date",
                description: "Date trainee was deferred"
            }, {
                keyName: "disability_disclosure",
                dataType: "string",
                description: "",
                hidden: false
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: "Timestamp at which a trainee record was discarded"
            }, {
                keyName: "diversity_disclosure",
                dataType: "string",
                description: ""
            }, {
                keyName: "dormancy_dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "dttp_update_sha",
                dataType: "string",
                description: ""
            }, {
                keyName: "ebacc",
                dataType: "string",
                description: ""
            }, {
                keyName: "employing_school_id",
                dataType: "string",
                description: "Employing school urn",
                foreignKeyTable: "schools"
            }, {
                keyName: "employing_school_not_applicable",
                dataType: "boolean",
                description: "Employing school not applicable, true or false"
            }, {
                keyName: "end_academic_cycle_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "academic_cycles"
            }, {
                keyName: "hesa_id",
                dataType: "string",
                description: "HESA unique student identifier. Presence of this implies an HEI trainee."
            }, {
                keyName: "hesa_editable",
                dataType: "boolean",
                description: "TRUE if this trainee is editable in HESA"
            }, {
                keyName: "hesa_updated_at",
                dataType: "timestamp",
                description: "Timestamp of last HESA update"
            }, {
                keyName: "iqts_country",
                dataType: "string",
                description: "Country where training is being undertaken for trainees on iQTS route"
            }, {
                keyName: "itt_end_date",
                dataType: "date",
                description: "ITT course end date"
            }, {
                keyName: "itt_start_date",
                dataType: "date",
                description: "ITT course start date"
            }, {
                keyName: "lead_school_id",
                dataType: "string",
                description: "Lead school urn",
                foreignKeyTable: "schools",
                hidden: false
            }, {
                keyName: "lead_school_not_applicable",
                dataType: "boolean",
                description: "Lead school not applicable, true or false"
            }, {
                keyName: "outcome_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "placement_assignment_dttp_id",
                dataType: "string",
                description: "",
                alias: "placement_assignment_dttp_id_uuid"
            }, {
                keyName: "placement_detail",
                dataType: "string",
                description: "",
            }, {
                keyName: "progress",
                dataType: "string",
                description: "progress - various JSON pairs",
                hidden: false
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "Registers unique provider identifier. Not used by other services.",
                foreignKeyTable: "providers"
            }, {
                keyName: "recommended_for_award_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "record_source",
                dataType: "string",
                description: "Source of where the trainee record originated from i.e. manual, apply, dttp, hesa_collection, hesa_trn_data"
            }, {
                keyName: "region",
                dataType: "string",
                description: "Trainee's region"
            }, {
                keyName: "reinstate_date",
                dataType: "date",
                description: ""
            }, {
                keyName: "slug",
                dataType: "string",
                description: ""
            }, {
                keyName: "start_academic_cycle_id",
                dataType: "string",
                description: "",
                foreignKeyTable: "academic_cycles"
            }, {
                keyName: "sex",
                dataType: "string",
                description: "Trainee sex",
                hidden: false
            }, {
                keyName: "state",
                dataType: "string",
                description: "Current status of trainee - draft(0), submitted_for_trn (1), trn_received (2), recommended_for_award (3), withdrawn (4), deferred (5), awarded(6)"
            }, {
                keyName: "study_mode",
                dataType: "string",
                description: ""
            }, {
                keyName: "submission_ready",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "submitted_for_trn_at",
                dataType: "timestamp",
                description: "submitted for teacher reference number at"
            }, {
                keyName: "training_initiative",
                dataType: "string",
                description: ""
            }, {
                keyName: "training_route",
                dataType: "string",
                description: "training route - assessment_only (0),provider_led_postgrad (1), early_years_undergrad (2), school_direct_tuition_fee (3),school_direct_salaried (4), pg_teaching_apprenticeship (5), early_years_assessment_only (6), early_years_salaried (7), early_years_postgrad (8), provider_led_undergrad (9), opt_in_undergrad (10), hpitt_postgrad (11), iqts (12)"
            }, {
                keyName: "trn",
                dataType: "string",
                description: "Trainees's Teacher Reference Number",
                hidden: false
            }, {
                keyName: "withdraw_date",
                dataType: "date",
                description: "Date trainee withdrew from course"
            }, {
                keyName: "withdraw_reasons_details",
                dataType: "string",
                description: "Details of the reason a trainee withdrew from course"
            }, {
                keyName: "withdraw_reasons_dfe_details",
                dataType: "string",
                description: ""
            }, {
                keyName: "hesa_trn_submission_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "created_from_hesa",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "created_from_dttp",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "slug_sent_to_dqt_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "previous_hesa_id",
                dataType: "string",
                description: "Value immediately before this update of: HESA unique student identifier. Presence of this implies an HEI trainee."
            }]
        },
        {
            entityTableName: "trainee_withdrawal_reasons",
            description: "",
            dataFreshnessDays: 7,
            keys: [{
                keyName: "trainee_id",
                dataType: "string",
                description: "UID of the trainee who withdrew from a course",
                foreignKeyTable: "trainees"
            }, {
                keyName: "withdrawal_reason_id",
                dataType: "string",
                description: "UID of the reason a trainee withdrew from a course",
                foreignKeyTable: "withdrawal_reasons"
            }]
        },
        {
            entityTableName: "withdrawal_reasons",
            description: "",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "users",
            description: "",
            keys: [{
                keyName: "dfe_sign_in_uid",
                dataType: "string",
                description: "Anonymised DfE Sign-in UID. Can be joined on to the anonymised sign_in_uid fields in the Publish user table and Apply provider_users and support_users tables."
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: "Timestamp at which a user was discarded"
            }, {
                keyName: "dttp_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "last_signed_in_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "system_admin",
                dataType: "boolean",
                description: ""
            }, {
                keyName: "welcome_email_sent_at",
                dataType: "timestamp",
                description: ""
            }]
        }
    ]
});
