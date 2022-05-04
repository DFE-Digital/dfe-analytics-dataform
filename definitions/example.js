const dfeAnalyticsDataform = require("../"); // change ../ to dfe-analytics-dataform when using this in your own Dataform project

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project
dfeAnalyticsDataform({
  tableSuffix: "Your table suffix here",
  bqProjectName: "Your BigQuery project name here",
  bqDatasetName: "Your BigQuery dataset name here",
  bqEventsTableName: "Your BigQuery events table name here - usually just 'events'",
  dataSchema: [{
    entityTableName: "Your entity table name here from your production database analytics.yml",
    keys: [{
      keyName: "Your string field name here",
      dataType: "string"
    }, {
      keyName: "Yoour boolean field name here",
      dataType: "boolean"
    }, {
      keyName: "Your timestamp field name here (when it actually contains a date!)",
      dataType: "date_as_timestamp"
    }]
  }],
  analyticsYmlFileLatest: `
  # Copy your analytics.yml file here from your production repository that uses dfe-analytics
shared:
  entity_name:
    - field_name
    - other_field_name
    - and_so_on
    `
});
