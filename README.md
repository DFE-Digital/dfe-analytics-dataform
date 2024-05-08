# dfe-analytics-dataform
Dataform package containing commonly used SQL functions and table definitions, for use with event data streamed to BigQuery using DfE-Digital/dfe-analytics: https://github.com/DFE-Digital/dfe-analytics.

## How to install
1. Set up your Dataform project - see [Google documentation](https://cloud.google.com/dataform/docs/overview).
2. Ensure that it is connected to your BigQuery project.
3. Ensure that it is synchronised with its own dedicated Github repository.
4. Set up a production release configuration - see [Google documentation](https://cloud.google.com/dataform/docs/release-configurations).
5. Add the following line within the dependencies block of the package.json file in your Dataform project:
```
"dfe-analytics-dataform": "https://github.com/DFE-Digital/dfe-analytics-dataform/archive/refs/tags/v1.10.0.tar.gz"
```
It should now look something like:
```
{
    "dependencies": {
        "@dataform/core": "2.9.0",
        "dfe-analytics-dataform": "https://github.com/DFE-Digital/dfe-analytics-dataform/archive/refs/tags/v1.10.0.tar.gz"
    }
}
```
6. Click the 'Install Packages' button on the right hand side of the package.json screen. This will also update package-lock.json automatically.
7. Create a file called ```includes/data_functions.js``` containing the following line:
```
module.exports = require("dfe-analytics-dataform/includes/data_functions");
```
8. Create a second file called ```definitions/dfe_analytics_dataform.js``` that looks like the following:
```
const dfeAnalyticsDataform = require("dfe-analytics-dataform");

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project - distinguish between them by giving each one a different eventSourceName. This will cause all the tables produced automatically by dfe-analytics-dataform to have your suffix included in them to allow users to tell the difference between them.
dfeAnalyticsDataform({
  eventSourceName: "Short name for your event source here - this might be a short name for your service, for example",
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
      alias: "Name to give this field in queries"
    }, {
      keyName: "Your date field name here",
      dataType: "date",
      description: "Description of this field to include in metadata here."
    }]
  }]
});
```

9. Replace the values of the ```eventSourceName```, ```bqEventsTableName``` and ```urlRegex``` parameters in this file with the values you need. At this stage compilation errors are normal because you haven't yet run the pipeline for the first time, which creates the output dataset. You will do this later.

10. Execute the ```events_{eventSourceName}``` from your development workspace with the 'Full Refresh' option enabled. This may take some time to complete. See [Google documentation](https://cloud.google.com/dataform/docs/trigger-execution) for more information.

11. If you are using dfe-analytics (not dfe-analytics-dotnet), do the same for the ```dataSchema``` parameter - a JSON value which specifies the schema of the tables and field names and types in your database which are being streamed to BigQuery. Don't include the ```id```, ```created_at``` and ```updated_at``` fields - they are included automatically. If you're starting from scratch, *don't* type this out. Instead, to save time, generate a blank ```dataSchema``` JSON to paste in to this file by running the ```...data_schema_latest``` query in Dataform (in the same way that you did in the previous step for the ```events``` table). You can do this by copying and pasting the output from the preview of the table this produces in BigQuery. This will automatically attempt to work out table names, field names and data types for each field using data streamed the previous day and today - although you might need to add in any tables that didn't have entity events streamed yesterday/today manually, or tweak some data types.

12. If you are using dfe-analytics-dotnet, replace the entire ```dataSchema: [ ... ],``` parameter with ```transformEntityEvents: false,``` - since the .NET port of dfe-analytics does not yet stream entity events into BigQuery.

13. In the unlikely event that your ```events``` table is stored in a different BigQuery project to the project named in your ```defaultDatabase``` parameter in your GCP Dataform release configuration (or the ```dataform.json``` file in your legacy Dataform project), add the line ```bqProjectName: 'name_of_the_bigquery_project_your_events_table_is_in',``` just before the line which sets ```bqDatasetName``` parameter in ```dfe_analytics_dataform.js```.

14. If your ```events``` table is stored in a different dataset to the dataset configured in your GCP Dataform release configuration (or the ```defaultSchema``` parameter in the ```dataform.json``` file in your legacy Dataform project), add the line ```bqDatasetName: "name_of_the_bigquery_project_your_events_table_is_in",``` just before the line which sets ```bqDatasetName``` parameter in ```dfe_analytics_dataform.js```.

15. In the unlikely event that one or more of the tables in your database being streamed by ```dfe-analytics``` has a primary key that is not ```id```, add the line ```primary_key: 'name_of_primary_key_for_this_table'``` to the element of the dataSchema that represents this table, alongside the ```entityTableName```.

16. If your dfe-analytics implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table, add the line ```bqEventsTableNameSpace: 'your_namespace_here'``` after the line that sets the ```bqEventsTableName``` parameter. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per value of ```namespace``` - this allows configuration options to differ between namespaces.

17. Optionally, for each foreign key, configure referential integrity assertions following the instructions in the section below. These check that the value of each foreign key matches a valid primary key in another table.

18. Commit your changes and merge to the ```main```/```master``` branch of the Github repository linked to your Dataform repository.

19. Leave your development workspace. Run a one-off [manual compilation](https://cloud.google.com/dataform/docs/release-configurations#manual-compilation) of your production release configuration.

20. Run a 'full refresh' execution of your entire pipeline using this release configuration, and resolve any configuration errors this flags (e.g. omissions made when specifying a ```dataSchema```).

## Additional configuration options
You may in addition to step 8 of the setup instructions wish to configure the following options by adding them to the JSON passed to the ```dfeAnalyticsDataform()``` JavaScript function.

- ```bqProjectName``` - name of the BigQuery project that your events table is located in. Defaults to the same BigQuery project configured in your GCP Dataform release configuration (or named in your ```defaultDatabase``` parameter in the ```dataform.json``` file in your legacy Dataform project).
- ```bqDatasetName``` - name of the BigQuery dataset that your events table is located in. Defaults to the same BigQuery dataset configured in your GCP Dataform release configuration (or named in your ```defaultSchema``` parameter in the ```dataform.json``` file in your legacy Dataform project).
- ```bqEventsTableNameSpace``` - value of the ```namespace``` field in the ```events``` table to filter all data being transformed by before it enters the pipeline. Use this if your dfe-analytics implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table in BigQuery. ```null``` by default. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per possible value of ```namespace``` - this allows configuration options to differ between namespaces.
- ```transformEntityEvents``` - whether to generate queries that transform entity CRUD events into flattened tables. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```enableSessionTables``` - whether to generate the ```sessions``` and ```pageviews_with_funnels``` tables. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```eventsDataFreshnessDays``` - number of days after which, if no new events have been received, the events_data_not_fresh assertion will fail to alert you to this. Defaults to ```1``` if not specified.
- ```eventsDataFreshnessDisableDuringRange``` - if set to ```true```, disables the events_data_not_fresh assertion if today's date is currently between one of the ranges in assertionDisableDuringDateRanges
- ```assertionDisableDuringDateRanges``` - an array of day or date ranges between which some assertions will be disabled if other parameters are set to disable them. Each range is a hash containing either the integer values fromDay, fromMonth, toDay and toMonth *or* the date values fromDate and toDate. Defaults to an approximation to school holidays each year: ```[{fromMonth: 7, fromDay: 25, toMonth: 9, toDay: 1}, {fromMonth: 3, fromDay: 29, toMonth: 4, toDay: 14}, {fromMonth: 12, fromDay: 22, toMonth: 1, toDay: 7}]```
- ```compareChecksums``` - ```true``` or ```false```. Defaults to ```false```. Enables a currently experimental assertion which fails if checksums and row counts in entity_table_check events do not match checksums and row counts in BigQuery 
- ```funnelDepth``` - number of steps forwards/backwards to analyse in pageview funnels - higher allows deeper analysis, lower reduces CPU usage and cost. Defaults to ```10``` if not specified.
- ```requestPathGroupingRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to replace with the string 'UID' when grouping web request paths in funnel analysis. Defaults to ```'[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*'``` if not specified (i.e. replaces unbroken strings of alphanumeric characters that include one or more numeric characters with 'UID')
- ```dependencies``` - array of strings listing the names of Dataform datasets generated outside dfe-analytics-dataform which should be materialised before datasets generated by dfe-analytics-dataform. Defaults to ```[]``` (an empty array) if not specified.
- ```attributionParameters``` - list of parameters to extract from the request query as individual fields in funnel analysis (for funnels which are new traffic to the site only). Defaults to ```['utm_source','utm_campaign','utm_medium','utm_content','gclid','gcsrc']``` if not specified.
- ```attributionDomainExclusionRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which never constitutes an external referral to your service. Defaults to ```"(?i)(signin.education.gov.uk)"``` if not specified.
- ```socialRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a social media site. Defaults to ```'(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)'``` if not specified.
- ```searchEngineRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a search engine (regardless of whether paid or organic). Defaults to ```'(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)'``` if not specified.
- ```disabled``` - ```true``` or ```false```. Defaults to ```false```. If set to ```true``` then calling the package will not do anything.
- ```checkReferentialIntegrity``` - ```true``` or ```false```. Defaults to ```false```. See section "Referential integrity assertions" below.

## Updating to a new version
Users are notified through internal channels when a new version of dfe-analytics-dataform is released. To update:
1. In your Dataform project, modify your ```package.json``` file to change the version number in this line from the version number you are currently using to the version number you wish to update to:
```
"dfe-analytics-dataform": "https://github.com/DFE-Digital/dfe-analytics-dataform/archive/refs/tags/vX.Y.Z.tar.gz"
```
2. Click on 'Install packages' on the right hand side of this screen in Dataform.
3. Commit and merge your changes to your main/master branch.
4. Read the [release notes](https://github.com/DFE-Digital/dfe-analytics-dataform/releases) provided for each new release between the release you were using and the release you are updating to. These may include additional instructions to follow to update to this version - for example, running a full refresh on particular tables. Follow these instructions for all these releases, in the order that they were released.
5. If you haven't already run the whole pipeline in the previous step, run the whole pipeline now in Dataform. A full refresh should not be required unless specified in the release notes.
6. Ensure you - and your colleagues - pull the latest version of your code from Github and click 'Install packages' in every Dataform development workspace.

## Updating your ```dataSchema``` configuration
Update your ```dataSchema``` configuration whenever you wish to change the form that data is transformed into in any of the flattened entity tables generated by dfe-analytics-dataform.

Once you have updated your dataSchema, commit your changes, merge to main/master and then re-run your pipeline in Dataform.

### Format
```dataSchema``` is a JSON array of objects: ```dataSchema: [{}, {}, {}...]```. Each of these objects represents a table in your schema. It has the following attributes:
- ```entityTableName``` - the name of the table in your database; a string; mandatory
- ```description``` - a meta description of the table, which can be a blank string: ```''```; mandatory
- ```keys``` - an array of objects which determines how dfe-analytics-dataform will transform each of the fields in the table. Each table listed within ```dataSchema``` has its own ```keys``` i.e. : ```dataSchema: [{entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}...]```.
- ```primary_key``` - optional; if the table has a primary key that is not ```id```, then this should contain the name of the field that is the primary key, if it is not ```'id'```.
- ```dataFreshnessDays``` - optional; if set, creates an assertion which fails if no Create, Update or Delete events have been received in the last ```dataFreshnessDays``` days for this entity.
- ```dataFreshnessDisableDuringRange``` - optional; if set to ```true```, disables this assertion if today's date is currently between one of the ranges in ```assertionDisableDuringDateRanges```
- ```materialisation``` - optional; may be ```'view'``` or ```'table'```. Defaults to 'table' if not set. Determines whether the ```entity_version```, ```entity_latest``` and ```entity_field_updates``` tables for this entity will be materialised by Dataform as views or tables. Recommended usage is to set this to ```'table'``` if these tables will be used more than once a day, or ```'view'``` if not to save query costs.

Each object within each table's set of ```keys``` determines how dfe-analytics-dataform will transform a field within a table in your schema. It has the following attributes:
- ```keyName``` - name of the field in your database; mandatory
- ```dataType``` - determines the output data type for this field, which can be ```'string'```, ```'boolean'```, ```'integer'```, ```'float'```, ```'date'```, ```'timestamp'``` or ```'integer_array'```, but defaults to ```'string'``` if not present)
- ```isArray``` - ```true``` or ```false```; defaults to ```false```. If ```true``` then the data type will be a ```REPEATED``` field of data type ```dataType```. May not be used with ```dataType: 'integer_array'```.),
- ```description``` - a meta description of the field
- ```alias``` - a name to give the field in outputs instead of ```keyName```
- ```pastKeyNames``` - an array of strings, see section "Retaining access to data in renamed fields" below
- ```historic``` - a boolean, see section "Retaining access to historic fields" below
- ```foreignKeyName``` - a string, see section "Referential integrity assertions" below
- ```foreignKeyTable``` - a string, see section "Referential integrity assertions" below
- ```checkReferentialIntegrity``` - ```true``` or ```false```, see section "Referential integrity assertions" below

An example of a ```dataSchema``` is included in the installation instructions above and in [example.js](https://github.com/DFE-Digital/dfe-analytics-dataform/blob/master/definitions/example.js).

### Detecting times when you *must* update the ```dataSchema```
You must update ```dataSchema``` whenever a field or table is added or removed from your dfe-analytics ```analytics.yml``` file (often because it has been added or removed from your database), changes data type (for example, from ```timestamp``` to ```date```), or changes name.

dfe-analytics-dataform contains 2 [assertions](https://cloud.google.com/dataform/docs/assertions) which will cause your scheduled pipeline runs in Dataform to fail if:
- Data has been received about an entity in the database that is missing a field it was expecting. This failure will generate an error and prevent further queries in your pipeline from running.
- Data has been received about an entity in the database that contains a field (or entire table) it was not expecting. This failure will generate an error but will *not* prevent further queries in your pipeline from running. However, the new field(s) or table will not be included in dfe-analytics-dataform output until you update your configuration, and the error will continue to reoccur. 

The output from the assertions in the run logs for the failed run in Dataform will list which field(s) and/or tables are missing or new. You may also wish to check with the rest of your team that the fields you are adding or removing are fields that they were expecting to start or stop streaming, rather than being unexpected behaviour.

In either case, you should update your ```dataSchema``` configuration in ```dfe_analytics_dataform.js``` in Dataform to add or remove configuration for that field, following the JSON format above.

You should not usually need to run a full refresh in this scenario. The only exception to this is if you have added, removed or updated the ```primary_key``` attribute for a table in the ```dataSchema```. If you have done this then you will need to run a full refresh on the ```entity_version``` table in Dataform.

### Retaining access to data in renamed fields
If a field in your database has been renamed one or more times, and if the data type of that field has not changed, you may merge data from that field stored under its previous names by adding the configuration ```pastKeyNames: ['past_key_name_1','past_key_name_2']``` to the key configuration for that field. The value of that field will be handled as if it were called ```keyName``` if a field with that name is present. If it is not present then the value will be set to the first value in ```pastKeyNames```. If no field with that name is present then the value will be set to the second value in ```pastKeyNames```, and so on. (Behind the scenes, this functions as a SQL ```COALESCE()```.)

### Retaining access to historic fields
If a field used to be included in streamed entity event data for a particular table, but is no longer streamed, you may retain access to that data in historic versions of entities in that table by adding the configuration ```historic: true``` to the key configuration for that field.

This may be useful if a field has been deleted from a table in the database, but you wish to continue to analyse past versions of that table which do contain that field, or changes between past versions of that table. The field will contain a ```NULL``` value for the latest version of the table.

### Monitoring and correcting incomplete entity data received from dfe-analytics
On rare occasions an entity in the application database may be created, updated or deleted, but no event streamed to your events table. This will result in missing or out of date data in tables generated by dfe-analytics.

To detect incomplete or out of date tables, follow dfe-analytics documentation to enable nightly checksums to be streamed to your events table and set the ```compareChecksums: true``` parameter in the JSON passed to ```dfeAnalyticsDataform()```. If the entity event stream in BigQuery appears to be missing, then the checksum in the event will not match the checksum calculated from the entity event stream in BigQuery. This will cause the ```foo_entity_ids_do_not_match``` assertion generated by dfe-analytics-dataform to fail.

To monitor whether tables have been loaded accurately, query the ```entity_table_check_scheduled``` table in your output dataset. To monitor whether imports have completed accurately, query the ```entity_table_check_import``` table in your output dataset.

To correct incomplete or out of date tables, ask your developers to follow dfe-analytics documentation to import the latest table contents to BigQuery. It is recommended that you limit this import to tables where checksums do not match.

An import will also cause dfe-analytics-dataform to correct occasions when there is no entity deletion event present in the events table for entities which have in fact been deleted from the application database. Entities with IDs which were not included in the import but which still exist as latest versions of the entity in the event stream will be assumed to have been deleted at the time that the checksum for the earliest complete import that did not contain the entity was calculated.

### Referential integrity assertions
Referential integrity means that each value of a foreign key stored in a table matches a value of a primary key in a related table. dfe-analytics-dataform provides optional Dataform assertions which you may use to monitor referential integrity automatically. If a referential integrity assertion fails, the likely cause is that some data has not been streamed correctly to your ```events``` table by ```dfe-analytics```. A short term fix might be to run a ```dfe-analytics``` backfill on that table. Please report persistent referential integrity assertion failures, however, so that they can be investigated.

To configure a referential integrity assertion for a particular field (key), add parameters to the configuration for that key in your dataSchema so that it looks like:
```
    {
      keyName: "other_thing_id",
      dataType: "string",
      description: "Description of this field to include in metadata here.",
      foreignKeyTable: "other_things", // The name of the table the other_thing_id foreign key in this table relates *to* in your application database
      foreignKeyName: "other_thing_id", // The name of the primary key in the foreignKeyTable table the other_thing_id foreign key relates to.
      checkReferentialIntegrity: true
    }
```

If ```foreignKeyName``` is omitted, it defaults to ```id```.

If ```foreignKeyTable``` is set and ```foreignKeyName``` is ```id``` or is omitted, then foreign key constraints will be created where possible to accelerate JOIN performance. See [here](https://cloud.google.com/blog/products/data-analytics/join-optimizations-with-bigquery-primary-and-foreign-keys) for more information.

As an alternative to including ```checkReferentialIntegrity: true``` at key level like this, you may set ```checkReferentialIntegrity: true``` at the top level of the JSON passed to the ```dfeAnalyticsDataform()``` JavaScript function. If this is set then assertions will be created to check referential integrity for all keys in ```dataSchema``` which have ```foreignKeyTable``` set.

## Tables, assertions, and declarations this will create
For each occurrence of ```dfeAnalyticsDataform()``` in ```definitions/dfe_analytics_dataform.js``` this package will create the following automatically in your Dataform project. You can view and manage these within the Dataform UI by opening ```definitions/dfe_analytics_dataform.js```.

The names of these will vary depending on the ```eventSourceName``` you have specified. For example if your ```eventSourceName``` was ```foo``` then the following will be created:
- An incremental table called ```events_foo```, which you can access within a Dataform declaration using something like ```SELECT * FROM ${ref("events_foo")}```. This table will include all the events dfe-analytics streamed into the raw events table, filtered by namespace (if you configured this). It will also include details of the browser and operating system of the user who caused these events to be streamed, and will attach web request data (like the request path) to all events in the table, not just the web requests.
- An assertion called ```foo_events_data_not_fresh``` which will fail if no new events have been streamed in the last day (or number of days specified in your ```eventsDataFreshnessDays``` parameter).
- An incremental table called ```foo_entity_version```, containing each version of every entity in the database over time, with a ```valid_from``` and ```valid_to``` timestamp.
- A table called ```foo_data_schema_json_latest```, which is a default dataSchema JSON you could use to get started specifying this in dfe_analytics_dataform.js
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, tables or views (depending on the ```materialisation``` set) called something like ```bar_version_foo``` and ```bar_latest_foo```. ```bar_version_foo``` is a denormalised ('flattened') version of ```foo_version```, flattened according to the schema for ```foo``` you specified in ```dataSchema```. ```bar_latest_foo``` is the same as ```bar_version_foo``` except that it only includes the latest version of each entity (i.e. with ```valid_to IS NULL```). Both tables and fields within them will have metadata set to match the descriptions set in ```dataSchema```.
- Table valued functions called something like ```bar_at_foo``` which can be used to generate a past version of a table at a specified timestamp.
- If you have configured referential integrity assertions (see above), assertions called something like ```bar_foreign_key_lacks_referential_integrity_foo``` which fail if a foreign key in the ```bar_latest_foo``` table lacks referential integrity.
- If you have configured an entity-level data freshness assertion by including an entity-level ```dataFreshnessDays``` parameter in your ```dataSchema```, an assertion called something like ```bar_data_not_fresh_foo``` which fails if no new Create, Update or Delete events have been received in the last ```dataFreshnessDays``` days.
- If you have set ```compareChecksums: true```, an assertion called something like ```foo_entity_ids_do_not_match``` which fails if a checksum or a row count in an ```entity_table_check``` event streamed yesterday does not match the corresponding checksum and/or row count in the corresponding data in BigQuery.
- An assertion called something like ```foo_entity_import_ids_do_not_match``` which fails if a checksum or a row count in an ```import_entity_table_check``` event streamed yesterday does not match the corresponding checksum and/or row count in the corresponding batch of import events.
- Tables called ```entity_table_check_import_foo``` and ```entity_table_check_scheduled_foo``` which allow you to monitor data load completeness by comparing checksums and row counts sent from dfe-analytics to checksums and row counts calculated from your events table.
- Assertions to help spot when your ```dataSchema``` has become out of date or has a problem. These will tell you if ```foo_entities_are_missing_expected_fields``` or if ```foo_unhandled_field_or_entity_is_being_streamed```. The former will halt your pipeline from executing, while the latter will just alert you to the assertion failure.
- A table called ```foo_entity_field_updates```, which contains one row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to any ```updated_at``` fields are not included, but ```NULL``` values are.
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, a table called something like ```bar_field_updates_foo```. ```bar_field_updates_foo``` is a denormalised ('flattened') version of ```foo_entity_field_updates```, filtered down to the entity ```bar```, and with the new and previous values of that entity flattened according to the schema for ```foo``` you specified in ```dataSchema```. Fields will have metadata set to match the descriptions set in ```dataSchema```.
- An incremental table called ```pageview_with_funnels_foo```, which contains pageview events from the events table, along with two ARRAYs of STRUCTs containing a number of pageviews in either direction for use in funnel analysis. This number of pageviews is determined by the ```funnelDepth``` parameter you may optionally call ```dfeAnalyticsDataform()``` with. By default ```funnelDepth``` is 10. Disabled if ```enableSessionTables``` is ```false```.
- A table called ```sessions_foo```, which contains rows representing user sessions with attribution fields (e.g. medium, referer_domain) for each session. Includes the session_started_at and next_session_started_at timestamps to allow attribution modelling of a goal conversion that occurred between those timestamps. Disabled if ```enableSessionTables``` is ```false```.
- A table called ```dfe_analytics_configuration_foo``` which contains details of the configuration of dfe-analytics for ```foo``` over time, with ```valid_from``` and ```valid_to``` fields
- A stored procedure called ```pseudonymise_request_user_ids``` in the same dataset as the events table dfe-analytics streams data into (not necessarily the same dataset that Dataform outputs to). You can invoke this to convert raw user_ids in the events table to pseudonymised user IDs following the instructions in the procedure metadata.
