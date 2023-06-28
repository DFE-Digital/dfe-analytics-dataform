/* For use by dfe-analytics-dataform developers working with Apply for ITT test data only - for example code to use in your project, see definitions/example.js */

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
  eventSourceName: "apply",
  bqProjectName: "rugged-abacus-218110",
  bqDatasetName: "apply_events_production",
  bqEventsTableName: "events_dad_test",
  urlRegex: "apply-for-teacher-training.service.gov.uk",
  dataSchema: [{
      entityTableName: "candidates",
      description: "",
      dataFreshnessDays: 3,
      keys: [{
        keyName: "candidate_api_updated_at",
        dataType: "timestamp",
        description: ""
      }, {
        keyName: "course_from_find_id",
        dataType: "string",
        description: ""
      }, {
        keyName: "hide_in_reporting",
        dataType: "boolean",
        description: ""
      }, {
        keyName: "last_signed_in_at",
        dataType: "timestamp",
        description: ""
      }, {
        keyName: "magic_link_token_sent_at",
        dataType: "timestamp",
        description: ""
      }, {
        keyName: "sign_up_email_bounced",
        dataType: "boolean",
        description: ""
      }]
    }]
});
