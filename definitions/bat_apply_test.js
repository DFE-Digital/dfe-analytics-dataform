/* For use by dfe-analytics-dataform developers working with Apply for ITT test data only - for example code to use in your project, see definitions/example.js */

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({ 
  eventSourceName: "apply",
  bqProjectName: "rugged-abacus-218110",
  bqDatasetName: "apply_events_production",
  bqEventsTableName: "events",
  urlRegex: "apply-for-teacher-training.service.gov.uk",
  compareChecksums: true,
  enableSessionTables: false,
  dataSchema: [{
    entityTableName: "application_choices",
    description: "",
    keys: [{
      keyName: "accepted_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "application_form_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "course_changed_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "current_course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "decline_by_default_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "decline_by_default_days",
      dataType: "integer",
      description: ""
    }, {
      keyName: "declined_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "declined_by_default",
      dataType: "boolean",
      description: ""
    }, {
      keyName: "offer_changed_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "offer_deferred_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "offered_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "original_course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "personal_statement",
      dataType: "string",
      description: ""
    }, {
      keyName: "recruited_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "reject_by_default_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "reject_by_default_days",
      dataType: "integer",
      description: ""
    }, {
      keyName: "rejected_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "rejected_by_default",
      dataType: "boolean",
      description: ""
    }, {
      keyName: "rejection_reason",
      dataType: "string",
      description: ""
    }, {
      keyName: "rejection_reasons_type",
      dataType: "string",
      description: ""
    }, {
      keyName: "sent_to_provider_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "status",
      dataType: "string",
      description: ""
    }, {
      keyName: "status_before_deferral",
      dataType: "string",
      description: ""
    }, {
      keyName: "structured_rejection_reasons",
      dataType: "string",
      description: ""
    }, {
      keyName: "structured_withdrawal_reasons",
      dataType: "string",
      isArray: true,
      description: ""
    }, {
      keyName: "withdrawal_feedback",
      dataType: "string",
      description: ""
    }, {
      keyName: "withdrawn_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "withdrawn_or_declined_for_candidate_by_provider",
      dataType: "boolean",
      description: ""
    }]
  },
  {
    entityTableName: "application_feedback",
    description: "Testing whether the view materialisation works",
    materialisation: "view",
    keys: [{
      keyName: "application_form_id",
      dataType: "string",
      description: ""
    }, {
      keyName: "consent_to_be_contacted",
      dataType: "boolean",
      description: ""
    }, {
      keyName: "feedback",
      dataType: "string",
      description: ""
    }, {
      keyName: "page_title",
      dataType: "string",
      description: ""
    }, {
      keyName: "path",
      dataType: "string",
      description: ""
    }]
  }]
});

dfeAnalyticsDataform({ 
  eventSourceName: "apply_again",
  bqProjectName: "rugged-abacus-218110",
  bqDatasetName: "apply_events_production",
  bqEventsTableName: "events",
  bqEventsTableNameSpace: "namespace",
  urlRegex: "apply-for-teacher-training.service.gov.uk",
  dataSchema: [{
    entityTableName: "application_choices",
    description: "",
    keys: [{
      keyName: "accepted_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "application_form_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "course_changed_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "current_course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "decline_by_default_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "decline_by_default_days",
      dataType: "integer",
      description: ""
    }, {
      keyName: "declined_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "declined_by_default",
      dataType: "boolean",
      description: ""
    }, {
      keyName: "offer_changed_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "offer_deferred_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "offered_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "original_course_option_id",
      dataType: "integer",
      description: ""
    }, {
      keyName: "personal_statement",
      dataType: "string",
      description: ""
    }, {
      keyName: "recruited_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "reject_by_default_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "reject_by_default_days",
      dataType: "integer",
      description: ""
    }, {
      keyName: "rejected_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "rejected_by_default",
      dataType: "boolean",
      description: ""
    }, {
      keyName: "rejection_reason",
      dataType: "string",
      description: ""
    }, {
      keyName: "rejection_reasons_type",
      dataType: "string",
      description: ""
    }, {
      keyName: "sent_to_provider_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "status",
      dataType: "string",
      description: ""
    }, {
      keyName: "status_before_deferral",
      dataType: "string",
      description: ""
    }, {
      keyName: "structured_rejection_reasons",
      dataType: "string",
      description: ""
    }, {
      keyName: "structured_withdrawal_reasons",
      dataType: "string",
      isArray: true,
      description: ""
    }, {
      keyName: "withdrawal_feedback",
      dataType: "string",
      description: ""
    }, {
      keyName: "withdrawn_at",
      dataType: "timestamp",
      description: ""
    }, {
      keyName: "withdrawn_or_declined_for_candidate_by_provider",
      dataType: "boolean",
      description: ""
    }]
  }]
});