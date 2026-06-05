const dfeAnalyticsDataform = require("../");

const itt_mentorDatasetName = "itt_mentor_events_production";

dfeAnalyticsDataform({
    eventSourceName: "itt_mentor",
    bqDatasetName: itt_mentorDatasetName,
    bqEventsTableName: "events",
    urlRegex: "funding-mentors.service.gov.uk",
    transformEntityEvents: true,
    enableWebRequestIdentityResolution: true,
    enableSessionTables: false,
    expirationDays: false,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
    dataSchema: [{
            entityTableName: "claims",
            description: "Records details of claims submitted to the Funding Mentors service, including status and association with schools and providers.",
            keys: [{
                keyName: "school_id",
                dataType: "string",
                description: "UUID of the school associated with the claim",
                foreignKeyTable: "schools"
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "UUID of the provider associated with the claim",
                foreignKeyTable: "providers"
            }, {
                keyName: "claim_window_id",
                dataType: "string",
                description: "UUID of the claim window. This field is for linking the to claim_windows table.",
                foreignKeyTable: "claim_windows"
            }, {
                keyName: "reference",
                dataType: "string",
                description: "Claim reference"
            }, {
                keyName: "submitted_at",
                dataType: "timestamp",
                description: "Timestamp when the claim was submitted"
            }, {
                keyName: "created_by_type",
                dataType: "string",
                description: "Type of entity that created the claim. This will be a school user or a dfe support user."
            }, {
                keyName: "created_by_id",
                dataType: "string",
                description: "UUID of the entity that created the claim"
            }, {
                keyName: "status",
                dataType: "string",
                description: "Current status of the claim"
            }, {
                keyName: "submitted_by_id",
                dataType: "string",
                description: "UUID of the entity that submitted the claim"
            }, {
                keyName: "submitted_by_type",
                dataType: "string",
                description: "Type of entity that submitted the claim. This should always be a school user."
            }, {
                keyName: "reviewed",
                dataType: "boolean",
                description: "TRUE if the claim has been reviewed by the user, defined as the user visiting the 'check' page."
            }, {
                keyName: "payment_in_progress_at",
                dataType: "timestamp",
                description: "Timestamp indicating when ESFA began payment process."
            }, {
                keyName: "sampling_reason",
                dataType: "string",
                description: "Reason that the claim was selected for sampling."
            }, {
                keyName: "unpaid_reason",
                dataType: "string",
                description: "Reason that a claim has not been paid."
            }, {
                keyName: "clawback_approved_by_id",
                dataType: "string",
                description: "UUID of the user that approves the clawback."
            }, {
                keyName: "clawback_requested_by_id",
                dataType: "string",
                description: "UUID of the user that requests the clawback."
            }]
        },
        {
            entityTableName: "mentor_memberships",
            description: "Stores records of the relationships between mentors and schools",
            keys: [{
                keyName: "type",
                dataType: "string",
                description: "Type of mentor membership",
            }, {
                keyName: "mentor_id",
                dataType: "string",
                description: "UUID of the mentor associated with the membership"
            }, {
                keyName: "school_id",
                dataType: "string",
                description: "UUID of the school associated with the mentorship",
                foreignKeyTable: "schools"
            }]
        },
        {
            entityTableName: "mentor_trainings",
            description: "Records details of trainings completed by mentors, including types of training and hours completed.",
            keys: [{
                keyName: "training_type",
                dataType: "string",
                description: "Type of training undertaken by the mentor. This will be initial mentor training for a duration of 20 hours or a refresher training course for a duration of 1 hour."
            }, {
                keyName: "hours_completed",
                dataType: "integer",
                description: "Total number of training hours completed by the mentor"
            }, {
                keyName: "date_completed",
                dataType: "timestamp",
                description: "Timestamp when the training was completed"
            }, {
                keyName: "claim_id",
                dataType: "string",
                description: "UUID of the claim associated with the training",
                foreignKeyTable: "claims"
            }, {
                keyName: "mentor_id",
                dataType: "string",
                description: "UUID of the mentor who completed the training"
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "UUID of the provider who delivered the training",
                foreignKeyTable: "providers"
            }, {
                keyName: "rejected",
                dataType: "boolean",
                description: "A boolean indicating that the provider rejected the training hours claimed."
            }, {
                keyName: "reason_rejected",
                dataType: "string",
                description: "The reason the claim for this training has been rejected by the provider."
            }, {
                keyName: "not_assured",
                dataType: "boolean",
                description: "A boolean indicating that the school rejected the training hours claimed."
            }, {
                keyName: "reason_not_assured",
                dataType: "string",
                description: "The reason the claim for this training has been rejected by the school."
            }, {
                keyName: "hours_clawed_back",
                dataType: "integer",
                description: "The number of hours of training that have had been clawed back due to the school not having suitable evidence of the mentor training when sampled."
            }, {
                keyName: "reason_clawed_back",
                dataType: "string",
                description: "The reason that the clawback has been initiated against this claim."
            }, {
                keyName: "reason_clawback_rejected",
                dataType: "string",
                description: "The reason that the clawback initiated against this claim was rejected."
            }]
        },
        {
            entityTableName: "placements",
            description: "Records information about ITT placements that are posted by schools on the School Placements service, including school, status and dates.",
            keys: [{
                keyName: "school_id",
                dataType: "string",
                description: "UUID of the school where the placement is located.",
                foreignKeyTable: "schools"
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "UUID of the provider that the placement is assigned to."
            }, {
                keyName: "subject_id",
                dataType: "string",
                description: "UUID of the subject of the placement."
            }, {
                keyName: "year_group",
                dataType: "string",
                description: "The school year group of the placement."
            }, {
                keyName: "academic_year_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "key_stage_id",
                dataType: "string",
                description: "UUID of the key stage of the placement."
            }, {
                keyName: "send_specific",
                dataType: "boolean",
                description: "TRUE if the placement is a SEND placement."
            }, {
                keyName: "creator_id",
                dataType: "string",
                description: "UUID of the user that created the placement."
            }, {
                keyName: "creator_type",
                dataType: "string",
                description: "Type of user that created the placement."
            }]
        },
        {
            entityTableName: "providers",
            description: "Stores information about ITT providers.",
            keys: [{
                keyName: "code",
                dataType: "string",
                description: "Unique code assigned to each provider",
            }, {
                keyName: "placements_service",
                dataType: "boolean",
                description: "Indicates if the provider data is from the School Placements service, default value is FALSE"
            }, {
                keyName: "provider_type",
                dataType: "string",
                description: "Type of provider",
            }, {
                keyName: "name",
                dataType: "string",
                description: "Name of the provider",
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: "UK Provider Reference Number"
            }, {
                keyName: "urn",
                dataType: "string",
                description: "Unique Reference Number for school"
            }, {
                keyName: "website",
                dataType: "string",
                description: "Website URL of the provider"
            }, {
                keyName: "address1",
                dataType: "string",
                description: "Primary address line for the provider"
            }, {
                keyName: "address2",
                dataType: "string",
                description: "Secondary address line for the provider"
            }, {
                keyName: "address3",
                dataType: "string",
                description: "Tertiary address line for the provider"
            }, {
                keyName: "town",
                dataType: "string",
                description: "Town where the provider is located"
            }, {
                keyName: "city",
                dataType: "string",
                description: "City where the provider is located"
            }, {
                keyName: "county",
                dataType: "string",
                description: "County where the provider is located"
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "Post code for the provider"
            }, {
                keyName: "accredited",
                dataType: "boolean",
                description: "Indicates whether the provider is accredited by DfE to deliver ITT teacher training"
            }]
        },
        {
            entityTableName: "regions",
            description: "Stores information about funding regions, relevant to claims funding available per hour. The funding regions are inner London, outer London, London fringe and the Rest of England. ",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "Name of the region"
            }, {
                keyName: "claims_funding_available_per_hour_pence",
                dataType: "integer",
                description: "Claims funding available per hour, expressed in pence"
            }, {
                keyName: "claims_funding_available_per_hour_currency",
                dataType: "string",
                description: "Currency for claims funding available per hour"
            }]
        },
        {
            entityTableName: "schools",
            description: "Stores information about schools from GIAS data. GIAS data is updated daily.",
            keys: [{
                keyName: "urn",
                dataType: "string",
                description: "Unique Reference Number for the school."
            }, {
                keyName: "placements_service",
                dataType: "boolean",
                description: "Indicates if the school data is from the School Placements service, default value is FALSE"
            }, {
                keyName: "claims_service",
                dataType: "boolean",
                description: "Indicates if the school data is from the Funding Mentors service, default value is FALSE"
            }, {
                keyName: "expression_of_interest_completed",
                dataType: "boolean",
                description: "True if the school has expressed interest in hosting a placement in the next academic year"
            }, {
                keyName: "name",
                dataType: "string",
                description: "Name of the school"
            }, {
                keyName: "postcode",
                dataType: "string",
                description: "Postcode of the school"
            }, {
                keyName: "town",
                dataType: "string",
                description: "Town where the school is located"
            }, {
                keyName: "ukprn",
                dataType: "string",
                description: "UK Provider Reference Number"
            }, {
                keyName: "telephone",
                dataType: "string",
                description: "Telephone number of the school"
            }, {
                keyName: "website",
                dataType: "string",
                description: "Website of the school"
            }, {
                keyName: "address1",
                dataType: "string",
                description: "Primary address line of the school"
            }, {
                keyName: "address2",
                dataType: "string",
                description: "Secondary address line of the school"
            }, {
                keyName: "address3",
                dataType: "string",
                description: "Tertiary address line of the school"
            }, {
                keyName: "group",
                dataType: "string",
                description: "Establishment type of the school (GIAS groupings)"
            }, {
                keyName: "type_of_establishment",
                dataType: "string",
                description: "Establishment type of the school"
            }, {
                keyName: "phase",
                dataType: "string",
                description: "Educational phase of the school. This will be Primary or Secondary."
            }, {
                keyName: "gender",
                dataType: "string",
                description: "Indicates if the school is single-gender and which gender."
            }, {
                keyName: "minimum_age",
                dataType: "integer",
                description: "Minimum age of students at the school"
            }, {
                keyName: "maximum_age",
                dataType: "integer",
                description: "Maximum age of students at the school"
            }, {
                keyName: "religious_character",
                dataType: "string",
                description: "Religious character of the school. A school designated with a religious character has to follow the national curriculum, but it can choose what to teach in religious studies."
            }, {
                keyName: "admissions_policy",
                dataType: "string",
                description: "All establishments have an admissions policy to decide which children get places. For state-maintained schools, these may be set by the local council. Many establishments, for example independent schools, have their own admissions policies."
            }, {
                keyName: "urban_or_rural",
                dataType: "string",
                description: "Urban or rural location of the school"
            }, {
                keyName: "school_capacity",
                dataType: "integer",
                description: "Total student capacity"
            }, {
                keyName: "total_pupils",
                dataType: "integer",
                description: "Total number of pupils currently at the school."
            }, {
                keyName: "total_girls",
                dataType: "integer",
                description: "Total number of female pupils currently enrolled at the school."
            }, {
                keyName: "total_boys",
                dataType: "integer",
                description: "Total number of male pupils currently enrolled at the school."
            }, {
                keyName: "percentage_free_school_meals",
                dataType: "integer",
                description: "Percentage of students receiving free school meals"
            }, {
                keyName: "special_classes",
                dataType: "string",
                description: "Special classes offered by the school"
            }, {
                keyName: "send_provision",
                dataType: "string",
                description: "Details about support offered by schools for pupils with special educational needs and disabilities"
            }, {
                keyName: "rating",
                dataType: "string",
                description: "OFSTED rating of the school"
            }, {
                keyName: "last_inspection_date",
                dataType: "date",
                description: "Date of the last OFTSED inspection of the school"
            }, {
                keyName: "district_admin_name",
                dataType: "string",
                description: "Name of the administrative district that the school is located in"
            }, {
                keyName: "district_admin_code",
                dataType: "string",
                description: "Code of the administrative district that the school is located in"
            }, {
                keyName: "region_id",
                dataType: "string",
                description: "UUID of the funding region the school is located in for linking with the itt mentor region table.",
                foreignKeyTable: "regions"
            }, {
                keyName: "trust_id",
                dataType: "string",
                description: "UUID of the trust associated with the school for linking with the itt mentors trusts table.",
                foreignKeyTable: "trusts"
            }, {
                keyName: "vendor_number",
                dataType: "string",
                description: "For organisations that do not have URN's, it's used as the primary id for those organisations"
            }, {
                keyName: "longitude",
                dataType: "float",
                description: "Geographical longitude of the school"
            }, {
                keyName: "latitude",
                dataType: "float",
                description: "Geographical latitude of the school"
            }, {
                keyName: "local_authority_name",
                dataType: "string",
                description: "Name of the local authority the school is located in. More information can be found at https://get-information-schools.service.gov.uk/Guidance/LaNameCodes."
            }, {
                keyName: "local_authority_code",
                dataType: "string",
                description: "Code of the local authority the school is located in. More information can be found at https://get-information-schools.service.gov.uk/Guidance/LaNameCodes."
            }, {
                keyName: "claims_grant_conditions_accepted_at",
                dataType: "timestamp",
                description: "The timestamp at which the school accepted the Funding Mentors terms and conditions."
            }, {
                keyName: "claims_grant_conditions_accepted_by_id",
                dataType: "string",
                description: "UUID of the user who accepted the Funding Mentors terms and conditions. This can be linked with the users table."
            }, {
                keyName: "potential_placement_details",
                dataType: "string",
                description: "Contains data on placements providers have stated they could potentially host. JSONB format. Free text column for additional details may contain PII so this field is considered as PII.",
                hidden: true
            }]
        },
        {

            entityTableName: "subjects",
            description: "Stores information about academic subjects of placements.",
            keys: [{
                keyName: "subject_area",
                dataType: "string",
                description: "Indicates if the subject is Primary or Secondary"
            }, {
                keyName: "name",
                dataType: "string",
                description: "Name of the subject",

            }, {
                keyName: "code",
                dataType: "string",
                description: "HESA code of the subject"
            }, {
                keyName: "parent_subject_id",
                dataType: "string",
                description: "Higher level grouping of the subject ID. Currently, this only groups languages into a 'Modern Foreign Languages' subject."
            }]
        },
        {
            entityTableName: "trusts",
            description: "Stores information about trusts.",
            keys: [{
                keyName: "uid",
                dataType: "string",
                description: "Unique identifier for the trust",

            }, {
                keyName: "name",
                dataType: "string",
                description: "Name of the trust",
            }]
        },
        {
            entityTableName: "user_memberships",
            description: "Stores membership relationships between users and organisations.",
            keys: [{
                keyName: "user_id",
                dataType: "string",
                description: "UUID of the user.",
                foreignKeyTable: "users"
            }, {
                keyName: "organisation_type",
                dataType: "string",
                description: "Type of organisation to which the user belongs",

            }, {
                keyName: "organisation_id",
                dataType: "string",
                description: "UUID of the organisation",

            }]
        },
        {
            entityTableName: "users",
            description: "Stores user information.",
            keys: [{
                keyName: "type",
                dataType: "string",
                description: "Type of user."
            }, {
                keyName: "dfe_sign_in_uid",
                dataType: "string",
                description: "Unique identifier from the DfE sign-in system"
            }, {
                keyName: "first_name",
                dataType: "string",
                description: "The first name of the user.",
                hidden: true
            }, {
                keyName: "last_name",
                dataType: "string",
                description: "The last name of the user.",
                hidden: true
            }, {
                keyName: "last_signed_in_at",
                dataType: "timestamp",
                description: "Timestamp for the last sign-in of the user"
            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: "Timestamp when the user was deactivated or discarded"
            }, {
                keyName: "email",
                dataType: "string",
                description: "Email address of the user",
                hidden: true
            }]
        },
        {
            entityTableName: "partnerships",
            description: "Stores partnership information between schools and providers. This is a many to many relationship.",
            keys: [{
                keyName: "school_id",
                dataType: "string",
                description: "UUID of the school in the partnershop",
                foreignKeyTable: "schools"
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: "UUID of the provider in the partnership",
                foreignKeyTable: "providers"
            }]
        },
        {
            entityTableName: "placement_mentor_joins",
            description: "Stores information on which mentor is associated with each placement.",
            keys: [{
                keyName: "mentor_id",
                dataType: "string",
                description: "UUID of the mentor assigned to the placement."
            }, {
                keyName: "placement_id",
                dataType: "string",
                description: "UUID of the placement.",
                foreignKeyTable: "placements"
            }]
        },
        {
            entityTableName: "placement_subject_joins",
            description: "Stores information on the subject of each placement.",
            keys: [{
                keyName: "subject_id",
                dataType: "string",
                description: "UUID of the subject of the placement.",
                foreignKeyTable: "subjects"
            }, {
                keyName: "placement_id",
                dataType: "string",
                description: "UUID of the placement.",
                foreignKeyTable: "placements"
            }]
        }, {
            entityTableName: "placement_windows",
            description: "",
            keys: [{
                keyName: "placement_id",
                dataType: "string",
                description: "",
            }, {
                keyName: "term_id",
                dataType: "string",
                description: "",
            }]
        },
        {
            entityTableName: "school_contacts",
            description: "Stores contact information for contacts associated with schools, including names, email addresses, and related school ID.",
            keys: [{
                keyName: "school_id",
                dataType: "string",
                description: "UUID of the school to which the contact belongs.",
                foreignKeyTable: "schools"
            }]
        },
        {
            entityTableName: "placement_additional_subjects",
            description: "Stores additional subject associations for placements.",
            keys: [{
                keyName: "subject_id",
                dataType: "string",
                description: "UUID of the subject associated with the placement.",
                foreignKeyTable: "subjects"
            }, {
                keyName: "placement_id",
                dataType: "string",
                description: "UUID of the placement that the subject is associated with.",
                foreignKeyTable: "placements"
            }]
        },
        {
            entityTableName: "academic_years",
            description: "Stores academic year dates. This table is only used for linking with the claim_windows table.",
            keys: [{
                keyName: "starts_on",
                dataType: "date",
                description: "Start date of the academic year.",
            }, {
                keyName: "ends_on",
                dataType: "date",
                description: "End date of the academic year.",
            }, {
                keyName: "name",
                dataType: "date",
                description: "Display name of the academic year e.g. 2024 to 2025.",
            }]
        },
        {
            entityTableName: "claim_windows",
            description: "Stores data on the claim windows within each academic year. There will be 3 claim windows in a single academic year.",
            keys: [{
                keyName: "academic_year_id",
                dataType: "string",
                description: "UUID of the academic year associated with this claim window.",
                foreignKeyTable: "academic_years"
            }, {
                keyName: "starts_on",
                dataType: "date",
                description: "Start date of the academic year.",
            }, {
                keyName: "ends_on",
                dataType: "date",
                description: "End date of the academic year.",

            }, {
                keyName: "discarded_at",
                dataType: "timestamp",
                description: "Timestamp indicating when this claim window was discarded.",
            }]
        }, {
            entityTableName: "clawback_claims",
            description: "A table that connects each audit to the relevant clawback claim. Each row is a clawback claim.",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "UID of the claim that links to the clawback ID",
            }, {
                keyName: "clawback_id",
                dataType: "string",
                description: "UID of the clawback ID that links to the clawback claim",
            }]
        }, {
            entityTableName: "clawbacks",
            description: "An audit trail of each action a user takes when requesting a clawback for a claim. Each row is a clawback action.",
            keys: [{
                keyName: "downloaded_at",
                dataType: "timestamp",
                description: "Timestamp that the clawback action was downloaded at",
            }]
        }, {
            entityTableName: "terms",
            description: "School terms e.g. Autumn term",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "Name of the term e.g. 'Autumn term'"
            }]
        },
        {
            entityTableName: "mentors",
            description: "Stores mentor information.",
            keys: [{
                keyName: "first_name",
                dataType: "string",
                description: "The first name of the user.",
                hidden: true
            }, {
                keyName: "last_name",
                dataType: "string",
                description: "The last name of the user.",
                hidden: true
            }, {
                keyName: "trn",
                dataType: "string",
                description: "The trn of the mentor.",
                hidden: true
            }]
        },
        {
            entityTableName: "provider_samplings",
            description: "This table stores data on the CSV filed that were uploaded to the service by Support. The id field in this table can be linked to the record_id in the claim_activities table.",
            keys: [{
                keyName: "downloaded_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "provider_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "sampling_id",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "provider_sampling_claims",
            description: "This table stores data on the CSV filed that were uploaded to the service by Support. The id field in this table can be linked to the record_id in the claim_activities table.",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "provider_sampling_id",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "payment_responses",
            description: "This table stores data on the CSV filed that were uploaded to the service by Support. The id field in this table can be linked to the record_id in the claim_activities table.",
            keys: [{
                keyName: "user_id",
                dataType: "string",
                description: ""
            }, {
                keyName: "downloaded_at",
                dataType: "timestamp",
                description: ""
            }, {
                keyName: "processed",
                dataType: "string",
                description: ""
            }]
        },
        {
            entityTableName: "claim_activities",
            description: "This table serves as an audit log of everything that has been done by the support team. This record_id fields in this table can be linked to the id field in the samplings, provider_samplings, provider_sampling_claims and payment_responses tables for additional information.",
            keys: [{
                keyName: "record_id",
                dataType: "string",
                description: "This id can be linked with the id field in the samplings, provider_samplings, provider_sampling_claims and payment_responses tables for additional information."
            }, {
                keyName: "record_type",
                dataType: "string",
                description: "Indicates the type of record as classified by the Support team."
            }, {
                keyName: "user_id",
                dataType: "string",
                description: "The id of the user who added this record to the log."
            }, {
                keyName: "action",
                dataType: "string",
                description: "The action taken in response to this record."
            }]
        },
        {
            entityTableName: "hosting_interests",
            description: "This table contains details on each schools expression of interest. This feature enables schools to record their 'appetite' for hosting a placement in the next academic year.",
            keys: [{
                keyName: "school_id",
                dataType: "string",
                description: "The id of the school that submitted the expression of interest."
            }, {
                keyName: "academic_year_id",
                dataType: "string",
                description: "This academic year for which the expression of interest was submitted."
            }, {
                keyName: "appetite",
                dataType: "string",
                description: "The appetite of the school to host a placement in the corresponding academic year."
            }, {
                keyName: "reasons_not_hosting",
                dataType: "string",
                description: "The categorical reason selected for not hosting a placement."
            }, {
                keyName: "other_reason_not_hosting",
                dataType: "string",
                description: "Free text resons for not hosting a placement if 'Other' is selected as the categorical reason."
            }]
        },
        {
            entityTableName: "key_stages",
            description: "This is a reference data table that contains the name of the key stage of the placement. This should be joined to the key_stage_id field.",
            keys: [{
                keyName: "name",
                dataType: "string",
                description: "The name of the key stage that the placement is for."
            }]
        },
        {
            entityTableName: "payment_claims",
            description: "This table contains the reference id of claims and the id of the associatiated payments made for that claim.",
            keys: [{
                keyName: "claim_id",
                dataType: "string",
                description: "The UUID of the claim."
            },
            {
                keyName: "payment_id",
                dataType: "string",
                description: "The UUID of the payment."
            }]
        },
        {
            entityTableName: "payments",
            description: "This table is for contains records of the ESFA payment lists stored as CSV files in the service. Each claim can be linked to one of these payment_id using the claim_id in the payment_claims table.",
            keys: [{
                keyName: "downloaded_at",
                dataType: "timestamp",
                description: "Timestamp indicating when the CSV was downloaded."
            },
            {
                keyName: "sent_by_id",
                dataType: "string",
                description: "UUID of the user that sent the ESFA payment list CSV."
            }]
        }
    ]
});
