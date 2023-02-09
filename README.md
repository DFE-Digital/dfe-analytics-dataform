# dfe-analytics-dataform
Dataform package containing commonly used SQL functions and table definitions, for use with event data streamed to BigQuery using DfE-Digital/dfe-analytics: https://github.com/DFE-Digital/dfe-analytics.

## How to install
1. Set up your Dataform project using the [legacy Dataform web interface](https://app.dataform.co). Do not use the version of Dataform which is included with GCP and BigQuery - this is still in development and not feature complete, so is not yet supported by dfe-analytics-dataform.
2. Ensure that it is connected to your BigQuery project.
3. Ensure that it is synchronised with its own dedicated Github repository.
4. Add the following line within the dependencies block of the package.json file in your Dataform project:
```
"dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git#v1.2.0"
```
It should now look something like:
```
{
    "dependencies": {
        "@dataform/core": "2.3.0",
        "dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git#v1.2.0"
    }
}
```
5. Click the 'Install Packages' button on the right hand side of the package.json screen. This will also update package-lock.json automatically.
6. Create a file called ```includes/data_functions.js``` containing the following line:
```
module.exports = require("dfe-analytics-dataform/includes/data_functions");
```
7. Create a second file called ```definitions/dfe_analytics_dataform.js``` that looks like the following:
```
const dfeAnalyticsDataform = require("dfe-analytics-dataform");

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
      alias: "Name to give this field in queries"
    }, {
      keyName: "Your date field name here",
      dataType: "date",
      description: "Description of this field to include in metadata here."
    }]
  }]
});
```

8. Replace the values of the ```eventSourceName```, ```bqDatasetName```, ```bqEventsTableName``` and ```urlRegex``` parameters in this file with the values you need. At this stage compilation errors like ```Error: Not found: Dataset your-project-name:output-dataset-name was not found in location europe-west2 at [1:44]``` are normal because you haven't yet run the pipeline for the first time, which creates the output dataset.

9. Using the right hand side panel of the Dataform UI with ```dfe_analytics_dataform.js``` open, find the ```events_{eventSourceName}``` query and click 'Update table'. Enable the 'Full Refresh' option and run the query. This may take some time to complete.

10. If you are using dfe-analytics (not dfe-analytics-dotnet), do the same for the ```dataSchema``` parameter - a JSON value which specifies the schema of the tables and field names and types in your database which are being streamed to BigQuery. Don't include the ```id```, ```created_at``` and ```updated_at``` fields - they are included automatically. If you're starting from scratch, *don't* type this out. Instead, to save time, generate a blank ```dataSchema``` JSON to paste in to this file by running the ```data_schema_json_latest``` query in Dataform (in the same way that you did in the previous step for the ```events``` table). You can do this from the right hand sidebar when you open ```dfe_analytics_dataform``` in the Dataform web client, and then copying and pasting the output from the table this produces in BigQuery (don't copy and paste from Dataform as it doesn't handle newlines well). This will automatically attempt to work out table names, field names and data types for each field using data streamed the previous day and today - although you might need to add in any tables that didn't have entity events streamed yesterday/today manually, or tweak some data types.

11. If you are using dfe-analytics-dotnet, replace the entire ```dataSchema: [ ... ],``` parameter with ```transformEntityEvents: false,``` - since the .NET port of dfe-analytics does not yet stream entity events into BigQuery.

12. In the unlikely event that your ```events``` table is stored in a different BigQuery project to the project named in your ```defaultDatabase``` parameter in the ```dataform.json``` file in your Dataform project, add the line ```bqProjectName: 'name_of_the_bigquery_project_your_events_table_is_in',``` just before the line which sets ```bqDatasetName``` parameter in ```dfe_analytics_dataform.js```.

13. In the unlikely event that one or more of the tables in your database being streamed by ```dfe-analytics``` has a primary key that is not ```id```, add the line ```primary_key: 'name_of_primary_key_for_this_table'``` to the element of the dataSchema that represents this table, alongside the ```entityTableName```.

14. If your dfe-analytics implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table, add the line ```bqEventsTableNameSpace: 'your_namespace_here'``` after the line that sets the ```bqEventsTableName``` parameter. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per value of ```namespace``` - this allows configuration options to differ between namespaces.

15. Commit your changes and merge to the ```main```/```master``` branch of your Dataform project.

16. Run a 'full refresh' on your entire pipeline, and resolve any configuration errors this flags (e.g. omissions made when specifying a ```dataSchema```).

## Additional configuration options
You may in addition to step 8 of the setup instructions wish to configure the following options by adding them to the JSON passed to the ```dfeAnalyticsDataform()``` JavaScript function.

- ```bqProjectName``` - name of the BigQuery project that your events table is located in. Defaults to the same BigQuery project named in your ```defaultDatabase``` parameter in the ```dataform.json``` file in your Dataform project.
- ```bqEventsTableNameSpace``` - value of the ```namespace``` field in the ```events``` table to filter all data being transformed by before it enters the pipeline. Use this if your dfe-analytics implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table in BigQuery. ```null``` by default. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per possible value of ```namespace``` - this allows configuration options to differ between namespaces.
- ```transformEntityEvents``` - whether to generate queries that transform entity CRUD events into flattened tables. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```funnelDepth``` - number of steps forwards/backwards to analyse in pageview funnels - higher allows deeper analysis, lower reduces CPU usage and cost. Defaults to ```10``` if not specified.
- ```requestPathGroupingRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to replace with the string 'UID' when grouping web request paths in funnel analysis. Defaults to ```'[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*'``` if not specified (i.e. replaces unbroken strings of alphanumeric characters that include one or more numeric characters with 'UID')
- ```dependencies``` - array of strings listing the names of Dataform datasets generated outside dfe-analytics-dataform which should be materialised before datasets generated by dfe-analytics-dataform. Defaults to ```[]``` (an empty array) if not specified.
- ```attributionParameters``` - list of parameters to extract from the request query as individual fields in funnel analysis (for funnels which are new traffic to the site only). Defaults to ```['utm_source','utm_campaign','utm_medium','utm_content','gclid','gcsrc']``` if not specified.
- ```attributionDomainExclusionRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which never constitutes an external referral to your service. Defaults to ```"(?i)(signin.education.gov.uk)"``` if not specified.
- ```socialRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a social media site. Defaults to ```'(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)'``` if not specified.
- ```searchEngineRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a search engine (regardless of whether paid or organic). Defaults to ```'(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)'``` if not specified.

## Updating to a new version
Users are notified through internal channels when a new version of dfe-analytics-dataform is released. To update:
1. In your Dataform project, modify your ```package.json``` file to change the version number in this line from the version number you are currently using to the version number you wish to update to:
```
"dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git#vX.Y.Z"
```
2. Click on 'Install packages' on the right hand side of this screen in Dataform.
3. Commit and merge your changes to your main/master branch.
4. Read the [release notes](https://github.com/DFE-Digital/dfe-analytics-dataform/releases) provided for each new release between the release you were using and the release you are updating to. These may include additional instructions to follow to update to this version - for example, running a full refresh on particular tables. Follow these instructions for all these releases, in the order that they were released.
5. If you haven't already run the whole pipeline in the previous step, run the whole pipeline now in Dataform. A full refresh should not be required unless specified in the release notes. 

## Updating your ```dataSchema``` configuration
Update your ```dataSchema``` configuration whenever you wish to change the form that data is transformed into in any of the flattened entity tables generated by dfe-analytics-dataform.

Once you have updated your dataSchema, commit your changes, merge to main/master and then re-run your pipeline in Dataform.

### Format
```dataSchema``` is a JSON array of objects: ```dataSchema: [{}, {}, {}...]```:
- Each of these objects represents a table in your schema. It must have the attributes ```entityTableName``` (the name of the table in your database, a string), ```description``` (a meta description of the table, which can be a blank string: ```''```) and ```keys```. If the table has a primary key that is not ```id```, then this object may optionally have the attribute ```primary_key``` (containing the name of the field that is the primary key, if it is not ```'id'```.)
- ```keys``` is an array of objects. Each table listed within ```dataSchema``` has its own ```keys``` i.e. : ```dataSchema: [{entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}...]```.
- Each object within each set of ```keys``` determines how dfe-analytics-dataform will transform a field within a table in your schema. Each field object must have within it the attribute ```keyName``` (name of the field in your database). It *may* also have the attributes ```dataType``` (determines the output data type for this field, which can be ```string```, ```boolean```, ```integer```, ```float```, ```date```, ```timestamp``` or ```integer_array```, but defaults to ```string``` if not present), ```description``` (a meta description of the field), ```alias``` (a name to give the field in outputs instead of ```entityTableName```), ```pastKeyNames``` (an array of strings, see below) and/or ```historic``` (a boolean, see below).
- An example of a ```dataSchema``` is included in the installation instructions above and in [example.js](https://github.com/DFE-Digital/dfe-analytics-dataform/blob/master/definitions/example.js).

### Detecting times when you *must* update the ```dataSchema``
You must update ```dataSchema``` whenever a field or table is added or removed from your dfe-analytics ```analytics.yml``` file (often because it has been added or removed from your database), changes data type (for example, from ```timestamp``` to ```date```), or changes name.

dfe-analytics-dataform contains 2 [assertions](https://docs.dataform.co/guides/assertions) which will cause your scheduled pipeline runs in Dataform to fail if:
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

## Tables, assertions, and declarations this will create
For each occurrence of ```dfeAnalyticsDataform()``` in ```definitions/dfe_analytics_dataform.js``` this package will create the following automatically in your Dataform project. You can view and manage these within the Dataform UI by opening ```definitions/dfe_analytics_dataform.js```.

The names of these will vary depending on the ```eventSourceName``` you have specified. For example if your ```eventSourceName``` was ```foo``` then the following will be created:
- A declaration of your events table, which you can use within a Dataform declaration using something like ```SELECT * FROM ${ref("bqDatasetName","bqEventsTableName")}``` (replacing those values with your own).
- An incremental table called ```events_foo```, which you can access within a Dataform declaration using something like ```SELECT * FROM ${ref("events_foo")}```. This table will include all the events dfe-analytics streamed into the raw events table, filtered by namespace (if you configured this). It will also include details of the browser and operating system of the user who caused these events to be streamed, and will attach web request data (like the request path) to all events in the table, not just the web requests.
- An incremental table called ```foo_entity_version```, containing each version of every entity in the database over time, with a ```valid_from``` and ```valid_to``` timestamp.
- A table called ```foo_data_schema_json_latest```, which is a default dataSchema JSON you could use to get started specifying this in dfe_analytics_dataform.js
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, tables called something like ```bar_version_foo``` and ```bar_latest_foo```. ```bar_version_foo``` is a denormalised ('flattened') version of ```foo_version```, flattened according to the schema for ```foo``` you specified in ```dataSchema```. ```bar_latest_foo``` is the same as ```bar_version_foo``` except that it only includes the latest version of each entity (i.e. with ```valid_to IS NULL```). Both tables and fields within them will have metadata set to match the descriptions set in ```dataSchema```.
- Table valued functions called something like ```bar_at_foo``` which can be used to generate a past version of a table at a specified timestamp.
- Assertions to help spot when your ```dataSchema``` has become out of date or has a problem. These will tell you if ```foo_entities_are_missing_expected_fields``` or if ```foo_unhandled_field_or_entity_is_being_streamed```. The former will halt your pipeline from executing, while the latter will just alert you to the assertion failure.
- A table called ```foo_entity_field_updates```, which contains one row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to any ```updated_at``` fields are not included, but ```NULL``` values are.
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, a table called something like ```bar_field_updates_foo```. ```bar_field_updates_foo``` is a denormalised ('flattened') version of ```foo_entity_field_updates```, filtered down to the entity ```bar```, and with the new and previous values of that entity flattened according to the schema for ```foo``` you specified in ```dataSchema```. Fields will have metadata set to match the descriptions set in ```dataSchema```.
- An incremental table called ```pageview_with_funnels_foo```, which contains pageview events from the events table, along with two ARRAYs of STRUCTs containing a number of pageviews in either direction for use in funnel analysis. This number of pageviews is determined by the ```funnelDepth``` parameter you may optionally call ```dfeAnalyticsDataform()``` with. By default ```funnelDepth``` is 10.
- A table called ```sessions_foo```, which contains rows representing user sessions with attribution fields (e.g. medium, referer_domain) for each session. Includes the session_started_at and next_session_started_at timestamps to allow attribution modelling of a goal conversion that occurred between those timestamps.
