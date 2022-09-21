const dfeAnalyticsDataform = require("../"); // change ../ to dfe-analytics-dataform when using this in your own Dataform project

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project - distinguish between them by giving each one a different eventSourceName. This will cause all the tables produced automatically by dfe-analytics-dataform to have your suffix included in them to allow users to tell the difference between them.
dfeAnalyticsDataform({
  eventSourceName: "Short name for your event source here - this might be a short name for your service, for example",
  bqProjectName: "Your BigQuery project name here",
  bqDatasetName: "Your BigQuery dataset name here",
  bqEventsTableName: "Your BigQuery events table name here - usually just 'events'",
  urlRegex: "www.yourdomainname.gov.uk", // re-2 formatted regular expression to use to identify whether a URL is this service's own URL or an external one. If your service only has one domain name set this to 'www.yourdomainname.gov.uk' (without the protocol). If you have more than one use something like '(?i)(www.domain1.gov.uk|www.domain2.gov.uk|www.domain3.gov.uk)'
  dataSchema: [{
    entityTableName: "Your entity table name here from your production database analytics.yml",
    description: "Description of this entity to include in metadata of denormalised tables produced for this entity.",
    keys: [{
      keyName: "Your string field name here",
      dataType: "string",
      description: "Description of this field to include in metadata here."
    }, {
      keyName: "Your boolean field name here",
      dataType: "boolean",
      description: "Description of this field to include in metadata here.",
      alias: "Alternative name for this field"
    }, {
      keyName: "Your date field name here",
      dataType: "date",
      description: "Description of this field to include in metadata here."
    }]
  }]
});