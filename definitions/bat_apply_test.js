/* For use by dfe-analytics-dataform developers working with Apply for ITT test data only - for example code to use in your project, see definitions/example.js */

const dfeAnalyticsDataform = require("../");

dfeAnalyticsDataform({
  eventSourceName: "apply",
  bqProjectName: "rugged-abacus-218110",
  bqDatasetName: "apply_events_production",
  bqEventsTableName: "events",
  urlRegex: "apply-for-teacher-training.service.gov.uk",
  forceRequestUserIdPseudonymisation: true,
  dataSchema: [{
      entityTableName: "subjects",
      description: "",
      keys: [{
        keyName: "name",
        dataType: "string",
        description: ""
      }, {
        keyName: "code",
        dataType: "string",
        description: ""
      }]
    }]
});
