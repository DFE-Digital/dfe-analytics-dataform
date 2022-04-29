const dfeAnalyticsDataform = require("../"); // change ../ to dfe-analytics-dataform when using this in your own Dataform project

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project
dfeAnalyticsDataform({
    tableSuffix: "Your table suffix here",
    bqProjectName: "Your BigQuery project name here",
    bqDatasetName: "Your BigQuery dataset name here",
    bqEventsTableName: "Your BigQuery events table name here - usually just 'events'"
});