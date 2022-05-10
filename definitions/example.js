const dfeAnalyticsDataform = require("../"); // change ../ to dfe-analytics-dataform when using this in your own Dataform project

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project
dfeAnalyticsDataform({
  tableSuffix: "Your table suffix here",
  bqProjectName: "Your BigQuery project name here",
  bqDatasetName: "Your BigQuery dataset name here",
  bqEventsTableName: "Your BigQuery events table name here - usually just 'events'",
  dataSchema: [{
    entityTableName: "Your entity table name here from your production database analytics.yml",
    description: "Description of this entity to include in metadata of denormalised tables produced for this entity.",
    keys: [{
      keyName: "Your string field name here",
      dataType: "string",
      description: "Description of this field to include in metadata here."
    }, {
      keyName: "Yoour boolean field name here",
      dataType: "boolean",
      description: "Description of this field to include in metadata here."
    }, {
      keyName: "Your timestamp field name here (when it actually contains a date!)",
      dataType: "date_as_timestamp",
      description: "Description of this field to include in metadata here."
    }]
  }]
});
