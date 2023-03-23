const dfeAnalyticsDataform = require("../"); // change ../ to dfe-analytics-dataform when using this in your own Dataform project

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project - distinguish between them by giving each one a different eventSourceName. This will cause all the tables produced automatically by dfe-analytics-dataform to have your suffix included in them to allow users to tell the difference between them.
dfeAnalyticsDataform({
  eventSourceName: "Short name for your event source here - this might be a short name for your service, for example",
  bqDatasetName: "The BigQuery dataset your events table is in",
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
    }, {
      keyName: "related_thing_id",
      dataType: "string",
      description: "Description of this field to include in metadata here.",
      // Example of how to configure a referential integrity check that all related_thing_ids in this table relate to an id in the related_thing_latest_eventsource table
      foreignKeyTable: "related_thing"
    }, {
      keyName: "related_thing_other_id",
      dataType: "string",
      description: "Description of this field to include in metadata here.",
      // Example of how to configure a referential integrity check that all related_thing_ids in this table relate to an other_id in the related_thing_latest_eventsource table
      foreignKeyName: "other_id",
      foreignKeyTable: "related_thing"
    }]
  },
  {
    entityTableName: "related_thing",
    description: "Description of this field to include in metadata here.",
    dataFreshnessDays: 7, // Example of how to configure an entity-level data freshness assertion
    keys: [{
      keyName: "other_id",
      dataType: "string",
      description: "Description of this field to include in metadata here."
    }]
  }]
});
