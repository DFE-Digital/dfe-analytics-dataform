const dfeAnalyticsDataform = require("../");

const find_placementsDatasetName = "fps_events_production";

dfeAnalyticsDataform({
    eventSourceName: "find_placements",
    bqDatasetName: find_placementsDatasetName,
    bqEventsTableName: "events",
    urlRegex: "find-placement-schools.education.gov.uk",
    transformEntityEvents: true,
    enableSessionTables: true,
    enableWebRequestIdentityResolution: true,
    enableSessionDetailsTable: true,
    hiddenPolicyTagLocation: "projects/rugged-abacus-218110/locations/europe-west2/taxonomies/69524444121704657/policyTags/6523652585511281766",
    expirationDays: 2555, // days in 7 years
    dataSchema: [
      {
        entityTableName: "academic_years",
        description: "Stores academic year records including start and end dates",
        keys: [{
          keyName: "name",
          dataType: "string",
          description: "Name of the academic year"
        }, {
          keyName: "starts_on",
          dataType: "date",
          description: "Start date of the academic year"
        }, {
          keyName: "ends_on",
          dataType: "date",
          description: "End date of the academic year"
        }]
      },
      {
        entityTableName: "organisation_addresses",
        description: "Stores address details for organisations",
        keys: [{
          keyName: "organisation_id",
          dataType: "string",
          description: "UUID of the associated organisation",
        }, {
          keyName: "address_1",
          dataType: "string",
          description: "First line of the address"
        }, {
          keyName: "address_2",
          dataType: "string",
          description: "Second line of the address"
        }, {
          keyName: "address_3",
          dataType: "string",
          description: "Third line of the address"
        }, {
          keyName: "town",
          dataType: "string",
          description: "Town"
        }, {
          keyName: "city",
          dataType: "string",
          description: "City"
        }, {
          keyName: "county",
          dataType: "string",
          description: "County"
        }, {
          keyName: "postcode",
          dataType: "string",
          description: "Postal code of the address"
        }]
      },
      {
        entityTableName: "organisation_contacts",
        description: "Stores contact details of the contacts at organisations. For schools this is the placement coordinator.",
        keys: [{
          keyName: "organisation_id",
          dataType: "string",
          description: "UUID of the associated organisation",
        }, {
          keyName: "first_name",
          dataType: "string",
          description: "First name of the contact",
          hidden: true
        }, {
          keyName: "last_name",
          dataType: "string",
          description: "Last name of the contact",
          hidden: true
        }, {
          keyName: "email_address",
          dataType: "string",
          description: "Email address of the contact",
          hidden: true
        }, {
          keyName: "role",
          dataType: "string",
          description: "Role of the contact within the organisation"
        }, {
          keyName: "telephone",
          dataType: "string",
          description: "Telephone number of the contact",
          hidden: true
        }]
      },
      {
        entityTableName: "organisations",
        description: "Stores details of organisations. These could be either Schools or Providers.",
        keys: [{
          keyName: "name",
          dataType: "string",
          description: "Name of the organisation"
        }, {
          keyName: "urn",
          dataType: "string",
          description: "Unique Reference Number for schools"
        }, {
          keyName: "ukprn",
          dataType: "string",
          description: "UK Provider Reference Number"
        }, {
          keyName: "code",
          dataType: "string",
          description: "Organisation code"
        }, {
          keyName: "longitude",
          dataType: "float",
          description: "Longitude coordinate"
        }, {
          keyName: "latitude",
          dataType: "float",
          description: "Latitude coordinate"
        }, {
          keyName: "email_address",
          dataType: "string",
          description: "Organisation email address",
          hidden: true
        }, {
          keyName: "type",
          dataType: "string",
          description: "Type of organisation (e.g., School, Provider)"
        }, {
          keyName: "admissions_policy",
          dataType: "string",
          description: "All establishments have an admissions policy to decide which children get places. For state-maintained schools, these may be set by the local council. Many establishments, for example independent schools, have their own admissions policies."
        }, {
          keyName: "district_admin_code",
          dataType: "string",
          description: "Code of the administrative district that the school is located in"
        }, {
          keyName: "district_admin_name",
          dataType: "string",
          description: "Name of the administrative district that the school is located in"
        }, {
          keyName: "gender",
          dataType: "string",
          description: "Gender admission policy"
        }, {
          keyName: "group",
          dataType: "string",
          description: "Type group of the establishment. This is a higher level categorisation of the establishment type."
        }, {
          keyName: "last_inspection_date",
          dataType: "date",
          description: "Date of last inspection"
        }, {
          keyName: "local_authority_code",
          dataType: "string",
          description: "Local authority code"
        }, {
          keyName: "local_authority_name",
          dataType: "string",
          description: "Local authority name"
        }, {
          keyName: "maximum_age",
          dataType: "integer",
          description: "Maximum age of pupils"
        }, {
          keyName: "minimum_age",
          dataType: "integer",
          description: "Minimum age of pupils"
        }, {
          keyName: "percentage_free_school_meals",
          dataType: "integer",
          description: "Percentage of pupils eligible for free school meals"
        }, {
          keyName: "phase",
          dataType: "string",
          description: "Phase of education"
        }, {
          keyName: "rating",
          dataType: "string",
          description: "Last OSTED inspection rating"
        }, {
          keyName: "religious_character",
          dataType: "string",
          description: "Religious character of the school"
        }, {
          keyName: "school_capacity",
          dataType: "integer",
          description: "Capacity of the school"
        }, {
          keyName: "send_provision",
          dataType: "string",
          description: "Special Educational Needs and Disabilities provision"
        }, {
          keyName: "special_classes",
          dataType: "string",
          description: "Special classes availability"
        }, {
          keyName: "telephone",
          dataType: "string",
          description: "Organisation telephone number"
        }, {
          keyName: "total_boys",
          dataType: "integer",
          description: "Total number of boys"
        }, {
          keyName: "total_girls",
          dataType: "integer",
          description: "Total number of girls"
        }, {
          keyName: "total_pupils",
          dataType: "integer",
          description: "Total number of pupils"
        }, {
          keyName: "type_of_establishment",
          dataType: "string",
          description: "Type of establishment. This is only for schools."
        }, {
          keyName: "urban_or_rural",
          dataType: "string",
          description: "Urban or rural classification"
        }, {
          keyName: "website",
          dataType: "string",
          description: "School Website URL"
        }]
      },
      {
        entityTableName: "placement_preferences",
        description: "Stores placement preferences for a school in a given academic year",
        keys: [{
          keyName: "academic_year_id",
          dataType: "string",
          description: "UUID of the associated academic year",
        }, {
          keyName: "organisation_id",
          dataType: "string",
          description: "UUID of the school that is hosting the placement.",
        }, {
          keyName: "created_by_id",
          dataType: "string",
          description: "UUID of the user who created the record",
        }, {
          keyName: "appetite",
          dataType: "string",
          description: "Preference for hosting placements; This will be one of 'actively_looking', 'interested', 'not_open'."
        }, {
          keyName: "placement_details",
          dataType: "string",
          description: "Structured JSON with additional placement details"
        }]
      },
      {
        entityTableName: "placement_subjects",
        description: "Reference dataset containing details of the potential subjects a placement can be.",
        keys: [{
          keyName: "name",
          dataType: "string",
          description: "Name of the placement subject"
        }, {
          keyName: "code",
          dataType: "string",
          description: "Unique code for the subject"
        }, {
          keyName: "phase",
          dataType: "string",
          description: "Phase of education for the subject."
        }, {
          keyName: "parent_subject_id",
          dataType: "string",
          description: "UUID of the parent subject.",
        }]
      },
      {
        entityTableName: "previous_placements",
        description: "Stores records of previous placements for schools. This data is from the Register Service and is loaded into the Find School Placements service via the API.",
        keys: [{
          keyName: "school_id",
          dataType: "string",
          description: "UUID of the associated school",
        }, {
          keyName: "academic_year_id",
          dataType: "string",
          description: "UUID of the associated academic year",
        }, {
          keyName: "subject_name",
          dataType: "string",
          description: "Name of the subject of the placement. This is the allocation subject from the Register service.",
        }]
      },
      {
        entityTableName: "user_memberships",
        description: "Stores relationships between users and organisations",
        keys: [{
          keyName: "organisation_id",
          dataType: "string",
          description: "UUID of the associated organisation",
        }, {
          keyName: "user_id",
          dataType: "string",
          description: "UUID of the associated user",
        }]
      },
      {
        entityTableName: "users",
        description: "Stores information on users of the service.",
        keys: [{
          keyName: "first_name",
          dataType: "string",
          description: "First name of the user",
          hidden: true
        }, {
          keyName: "last_name",
          dataType: "string",
          description: "Last name of the user",
          hidden: true
        }, {
          keyName: "email_address",
          dataType: "string",
          description: "Email address of the user",
          hidden: true
        }, {
          keyName: "admin",
          dataType: "boolean",
          description: "Whether the user has administrative privileges for the service (This will be DfE Users e.g. Developers, Support Users)."
        }, {
          keyName: "dfe_sign_in_uid",
          dataType: "string",
          description: "DFE Sign-in UID",
        }, {
          keyName: "last_signed_in_at",
          dataType: "timestamp",
          description: "Timestamp of the user's last sign-in"
        }, {
          keyName: "selected_organisation_id",
          dataType: "string",
          description: "UUID of the organisation currently selected by the user",
        }]
      }
    ]

  });
