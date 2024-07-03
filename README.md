# dfe-analytics-dataform
Dataform package containing commonly used SQL functions and table definitions, for use with event data streamed to BigQuery using DfE-Digital/dfe-analytics: https://github.com/DFE-Digital/dfe-analytics.

## How to install
1. Set up your Dataform project - see [Google documentation](https://cloud.google.com/dataform/docs/overview).
2. Ensure that it is connected to your BigQuery project.
3. Ensure that it is synchronised with its own dedicated Github repository.
4. Set up a production release configuration - see [Google documentation](https://cloud.google.com/dataform/docs/release-configurations).
5. Add the following line within the dependencies block of the package.json file in your Dataform project, replacing the Xs with the [latest version number of this package](https://github.com/DFE-Digital/dfe-analytics-dataform/releases).:
```
"dfe-analytics-dataform": "https://github.com/DFE-Digital/dfe-analytics-dataform/archive/refs/tags/vX.X.X.tar.gz"
```
It should now look something like:
```
{
    "dependencies": {
        "@dataform/core": "X.X.X",
        "dfe-analytics-dataform": "https://github.com/DFE-Digital/dfe-analytics-dataform/archive/refs/tags/vX.X.X.tar.gz"
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

11. If you are using ```dfe-analytics``` (not ```dfe-analytics-dotnet```), do the same for the ```dataSchema``` parameter - a JSON value which specifies the schema of the tables and field names and types in your database which are being streamed to BigQuery. Don't include the ```id```, ```created_at``` and ```updated_at``` fields - they are included automatically. If you're starting from scratch, *don't* type this out. Instead, to save time, generate a blank ```dataSchema``` JSON to paste in to this file by running the ```...data_schema_latest``` query in Dataform (in the same way that you did in the previous step for the ```events``` table). You can do this by copying and pasting the output from the preview of the table this produces in BigQuery. This will automatically attempt to work out table names, field names and data types for each field using data streamed the previous day and today - although you might need to add in any tables that didn't have entity events streamed yesterday/today manually, or tweak some data types.

12. If you are using ```dfe-analytics-dotnet```, replace the entire ```dataSchema: [ ... ],``` parameter with ```transformEntityEvents: false,``` - since the .NET port of ```dfe-analytics``` does not yet stream entity events into BigQuery.

13. In the unlikely event that your ```events``` table is stored in a different BigQuery project to the project named in your ```defaultDatabase``` parameter in your GCP Dataform release configuration (or the ```dataform.json``` file in your legacy Dataform project), add the line ```bqProjectName: 'name_of_the_bigquery_project_your_events_table_is_in',``` just before the line which sets ```bqDatasetName``` parameter in ```dfe_analytics_dataform.js```.

14. If your ```events``` table is stored in a different dataset to the dataset configured in your GCP Dataform release configuration (or the ```defaultSchema``` parameter in the ```dataform.json``` file in your legacy Dataform project), add the line ```bqDatasetName: "name_of_the_bigquery_project_your_events_table_is_in",``` just before the line which sets ```bqDatasetName``` parameter in ```dfe_analytics_dataform.js```.

15. In the unlikely event that one or more of the tables in your database being streamed by ```dfe-analytics``` has a primary key that is not ```id```, add the line ```primaryKey: 'name_of_primary_key_for_this_table'``` to the element of the dataSchema that represents this table, alongside the ```entityTableName```.

16. If your ```dfe-analytics``` implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table, add the line ```bqEventsTableNameSpace: 'your_namespace_here'``` after the line that sets the ```bqEventsTableName``` parameter. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per value of ```namespace``` - this allows configuration options to differ between namespaces.

17. Commit your changes and merge to the ```main```/```master``` branch of the Github repository linked to your Dataform repository.

18. Leave your development workspace. Run a one-off [manual compilation](https://cloud.google.com/dataform/docs/release-configurations#manual-compilation) of your production release configuration.

19. Run a 'full refresh' execution of your entire pipeline using this release configuration, and resolve any configuration errors this flags (e.g. omissions made when specifying a ```dataSchema```).

## Additional configuration options
You may in addition to step 8 of the setup instructions wish to configure the following options by adding them to the JSON passed to the ```dfeAnalyticsDataform()``` JavaScript function.

- ```bqProjectName``` - name of the BigQuery project that your events table is located in. Defaults to the same BigQuery project configured in your GCP Dataform release configuration (or named in your ```defaultDatabase``` parameter in the ```dataform.json``` file in your legacy Dataform project).
- ```bqDatasetName``` - name of the BigQuery dataset that your events table is located in. Defaults to the same BigQuery dataset configured in your GCP Dataform release configuration (or named in your ```defaultSchema``` parameter in the ```dataform.json``` file in your legacy Dataform project).
- ```bqEventsTableNameSpace``` - value of the ```namespace``` field in the ```events``` table to filter all data being transformed by before it enters the pipeline. Use this if your ```dfe-analytics``` implementation uses the ```namespace``` field to distinguish between multiple interfaces or applications that result in data streamed to the same ```events``` table in BigQuery. ```null``` by default. To use ```dfe-analytics-dataform``` with more than one ```bqEventsTableNameSpace```, call ```dfeAnalyticsDataform();``` once per possible value of ```namespace``` - this allows configuration options to differ between namespaces.
- ```transformEntityEvents``` - whether to generate queries that transform entity CRUD events into flattened tables. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```hiddenPolicyTagLocation``` - a string, see section "Hidden fields" below.
- ```enableSessionTables``` - whether to generate the ```sessions``` and ```pageviews_with_funnels``` tables. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```enableMonitoring``` - whether to send summary monitoring data to the monitoring.pipeline_snapshots table in the cross-service GCP project. Boolean (```true``` or ```false```, without quotes). Defaults to ```true``` if not specified.
- ```eventsDataFreshnessDays``` - number of days after which, if no new events have been received, the events_data_not_fresh assertion will fail to alert you to this. Defaults to ```1``` if not specified.
- ```eventsDataFreshnessDisableDuringRange``` - if set to ```true```, disables the events_data_not_fresh assertion if today's date is currently between one of the ranges in assertionDisableDuringDateRanges
- ```assertionDisableDuringDateRanges``` - an array of day or date ranges between which some assertions will be disabled if other parameters are set to disable them. Each range is a hash containing either the integer values fromDay, fromMonth, toDay and toMonth *or* the date values fromDate and toDate. Defaults to an approximation to school holidays each year: ```[{fromMonth: 7, fromDay: 25, toMonth: 9, toDay: 1}, {fromMonth: 3, fromDay: 29, toMonth: 4, toDay: 14}, {fromMonth: 12, fromDay: 22, toMonth: 1, toDay: 7}]```
- ```compareChecksums``` - ```true``` or ```false```. Defaults to ```false```. Enables a currently experimental assertion which fails if checksums and row counts in entity_table_check events do not match checksums and row counts in BigQuery 
- ```funnelDepth``` - number of steps forwards/backwards to analyse in pageview funnels - higher allows deeper analysis, lower reduces CPU usage and cost. Defaults to ```10``` if not specified.
- ```requestPathGroupingRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to replace with the string 'UID' when grouping web request paths in funnel analysis. Defaults to ```'[0-9a-zA-Z]*[0-9][0-9a-zA-Z]*'``` if not specified (i.e. replaces unbroken strings of alphanumeric characters that include one or more numeric characters with 'UID')
- ```dependencies``` - array of strings listing the names of Dataform datasets generated outside ```dfe-analytics-dataform``` which should be materialised before datasets generated by ```dfe-analytics-dataform```. Defaults to ```[]``` (an empty array) if not specified.
- ```attributionParameters``` - list of parameters to extract from the request query as individual fields in funnel analysis (for funnels which are new traffic to the site only). Defaults to ```['utm_source','utm_campaign','utm_medium','utm_content','gclid','gcsrc']``` if not specified.
- ```attributionDomainExclusionRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to detect domain names which should be excluded from attribution modelling - for example, the domain name of an authentication service which never constitutes an external referral to your service. Defaults to ```"(?i)(signin.education.gov.uk)"``` if not specified.
- ```socialRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a social media site. Defaults to ```'(?i)(facebook|twitter|^t.co|linkedin|youtube|pinterest|whatsapp|tumblr|reddit)'``` if not specified.
- ```searchEngineRefererDomainRegex``` - [re2](https://github.com/google/re2/wiki/Syntax)-formatted regular expression to use to work out whether an HTTP referer's domain name is a search engine (regardless of whether paid or organic). Defaults to ```'(?i)(google|bing|yahoo|aol|ask.co|baidu|duckduckgo|dogpile|ecosia|exalead|gigablast|hotbot|lycos|metacrawler|mojeek|qwant|searx|swisscows|webcrawler|yandex|yippy)'``` if not specified.
- ```disabled``` - ```true``` or ```false```. Defaults to ```false```. If set to ```true``` then calling the package will not do anything.

## Updating to a new version
Users are notified through internal channels when a new version of ```dfe-analytics-dataform``` is released. To update:
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
Update your ```dataSchema``` configuration whenever you wish to change the form that data is transformed into in any of the flattened entity tables generated by ```dfe-analytics-dataform```.

Once you have updated your dataSchema, commit your changes, merge to main/master and then re-run your pipeline in Dataform.

### Format
```dataSchema``` is a JSON array of objects: ```dataSchema: [{}, {}, {}...]```. Each of these objects represents a table in your schema. It has the following attributes:
- ```entityTableName``` - the name of the table in your database; a string; mandatory
- ```description``` - a meta description of the table, which can be a blank string: ```''```; mandatory
- ```keys``` - an array of objects which determines how ```dfe-analytics-dataform``` will transform each of the fields in the table. Each table listed within ```dataSchema``` has its own ```keys``` i.e. : ```dataSchema: [{entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}, {entityTableName: '', description: '', keys: {}}...]```.
- ```primaryKey``` - optional; if the table has a primary key that is not ```id```, then this should contain the name of the field that is the primary key, if it is not ```'id'```.
- ```hidePrimaryKey``` - boolean, see section "Hidden fields" below
- ```dataFreshnessDays``` - optional; if set, creates an assertion which fails if no Create, Update or Delete events have been received in the last ```dataFreshnessDays``` days for this entity.
- ```dataFreshnessDisableDuringRange``` - optional; if set to ```true```, disables this assertion if today's date is currently between one of the ranges in ```assertionDisableDuringDateRanges```
- ```materialisation``` - optional; may be ```'view'``` or ```'table'```. Defaults to 'table' if not set. Determines whether the ```entity_version```, ```entity_latest``` and ```entity_field_updates``` tables for this entity will be materialised by Dataform as views or tables. Recommended usage is to set this to ```'table'``` if these tables will be used more than once a day, or ```'view'``` if not to save query costs.

Each object within each table's set of ```keys``` determines how ```dfe-analytics-dataform``` will transform a field within a table in your schema. It has the following attributes:
- ```keyName``` - name of the field in your database; mandatory
- ```dataType``` - determines the output data type for this field, which can be ```'string'```, ```'boolean'```, ```'integer'```, ```'float'```, ```'date'```, ```'timestamp'``` or ```'integer_array'```, but defaults to ```'string'``` if not present)
- ```isArray``` - ```true``` or ```false```; defaults to ```false```. If ```true``` then the data type will be a ```REPEATED``` field of data type ```dataType```. May not be used with ```dataType: 'integer_array'```.),
- ```description``` - a meta description of the field
- ```hidden``` - a boolean, see section "Hidden fields" below
- ```hiddenPolicyTagLocation``` - a string, see section "Hidden fields" below
- ```alias``` - a name to give the field in outputs instead of ```keyName```
- ```pastKeyNames``` - an array of strings, see section "Retaining access to data in renamed fields" below
- ```historic``` - a boolean, see section "Retaining access to historic fields" below
- ```foreignKeyName``` - a string, see section "Primary and foreign key constraints" below
- ```foreignKeyTable``` - a string, see section "Primary and foreign key constraints" below

An example of a ```dataSchema``` is included in the installation instructions above and in [example.js](https://github.com/DFE-Digital/dfe-analytics-dataform/blob/master/definitions/example.js).

### Detecting times when you *must* update the ```dataSchema```
You must update ```dataSchema``` whenever a field or table is added or removed from your ```dfe-analytics``` ```analytics.yml``` file (often because it has been added or removed from your database), changes data type (for example, from ```timestamp``` to ```date```), or changes name.

dfe-analytics-dataform contains 2 [assertions](https://cloud.google.com/dataform/docs/assertions) which will cause your scheduled pipeline runs in Dataform to fail if:
- Data has been received about an entity in the database that is missing a field it was expecting. This failure will generate an error and prevent further queries in your pipeline from running.
- Data has been received about an entity in the database that contains a field (or entire table) it was not expecting. This failure will generate an error but will *not* prevent further queries in your pipeline from running. However, the new field(s) or table will not be included in ```dfe-analytics-dataform``` output until you update your configuration, and the error will continue to reoccur. 

The output from the assertions in the run logs for the failed run in Dataform will list which field(s) and/or tables are missing or new. You may also wish to check with the rest of your team that the fields you are adding or removing are fields that they were expecting to start or stop streaming, rather than being unexpected behaviour.

In either case, you should update your ```dataSchema``` configuration in ```dfe_analytics_dataform.js``` in Dataform to add or remove configuration for that field, following the JSON format above.

You should not usually need to run a full refresh in this scenario. The only exception to this is if you have added, removed or updated the ```primaryKey``` attribute for a table in the ```dataSchema```. If you have done this then you will need to run a full refresh on the ```entity_version``` table in Dataform.

### Retaining access to data in renamed fields
If a field in your database has been renamed one or more times, and if the data type of that field has not changed, you may merge data from that field stored under its previous names by adding the configuration ```pastKeyNames: ['past_key_name_1','past_key_name_2']``` to the key configuration for that field. The value of that field will be handled as if it were called ```keyName``` if a field with that name is present. If it is not present then the value will be set to the first value in ```pastKeyNames```. If no field with that name is present then the value will be set to the second value in ```pastKeyNames```, and so on. (Behind the scenes, this functions as a SQL ```COALESCE()```.)

### Retaining access to historic fields
If a field used to be included in streamed entity event data for a particular table, but is no longer streamed, you may retain access to that data in historic versions of entities in that table by adding the configuration ```historic: true``` to the key configuration for that field.

This may be useful if a field has been deleted from a table in the database, but you wish to continue to analyse past versions of that table which do contain that field, or changes between past versions of that table. The field will contain a ```NULL``` value for the latest version of the table.

### Monitoring and correcting incomplete entity data received from ```dfe-analytics```
On rare occasions an entity in the application database may be created, updated or deleted, but no event streamed to your events table. This will result in missing or out of date data in tables generated by ```dfe-analytics```.

To detect incomplete or out of date tables, follow ```dfe-analytics``` documentation to enable nightly checksums to be streamed to your events table and set the ```compareChecksums: true``` parameter in the JSON passed to ```dfeAnalyticsDataform()```. If the entity event stream in BigQuery appears to be missing, then the checksum in the event will not match the checksum calculated from the entity event stream in BigQuery. This will cause the ```foo_entity_ids_do_not_match``` assertion generated by ```dfe-analytics-dataform``` to fail.

To monitor whether tables have been loaded accurately, query the ```entity_table_check_scheduled``` table in your output dataset. To monitor whether imports have completed accurately, query the ```entity_table_check_import``` table in your output dataset.

To correct incomplete or out of date tables, ask your developers to follow ```dfe-analytics``` documentation to import the latest table contents to BigQuery. It is recommended that you limit this import to tables where checksums do not match.

An import will also cause ```dfe-analytics-dataform``` to correct occasions when there is no entity deletion event present in the events table for entities which have in fact been deleted from the application database. Entities with IDs which were not included in the import but which still exist as latest versions of the entity in the event stream will be assumed to have been deleted at the time that the checksum for the earliest complete import that did not contain the entity was calculated.

Unless ```enableMonitoring: false``` is set, ```dfe-analytics-dataform``` will automatically send a small packet of summary monitoring data to the ```monitoring.pipeline_snapshots``` table in the cross-service GCP project. This will include details of how many tables in your project have matching checksums, and how many rows are missing/extra in BigQuery when compared to the database.

### Hidden fields
```dfe-analytics-dataform``` is intended for use with the ```dfe-analytics``` Ruby gem. When used together they have the capability to use [BigQuery column-level access control](https://cloud.google.com/bigquery/docs/column-level-security-intro) to restrict access to certain 'hidden' fields. ```dfe-analytics``` is responsible for ensuring that hidden fields are streamed into the ```hidden_data``` field in your events table, while ```dfe-analytics-dataform``` is responsible for ensuring that data in that field remains hidden once it has been transformed into the other tables generated by ```dfe-analytics-dataform```. If your Dataform pipeline transforms hidden data into fields in other tables which you also wish to hide, you are responsible for attaching policy tags to these fields. Google provides [documentation](https://cloud.google.com/dataform/docs/policy-tags) describing how to do this.

To use hidden field functionality in ```dfe-analytics-dataform``` effectively, you must first:
1. Set up IAM roles and policy tags in your GCP project, and attach the policy tags to the correct field in your events table, following the instructions documented [here](https://github.com/DFE-Digital/dfe-analytics/blob/main/docs/google_cloud_bigquery_setup.md).
2. Configure ```dfe-analytics``` to treat the desired fields as hidden by listing them in your application's ```config/analytics_hidden_pii.yml``` file as described [here](https://github.com/DFE-Digital/dfe-analytics/tree/main?tab=readme-ov-file#4-send-database-events).

You must then configure the following ```dfe-analytics-dataform``` parameters as appropriate:
- ```hiddenPolicyTagLocation``` - string; optional; top level or key level. The location of a policy tag that you wish to attach to hidden fields, for example projects/your-project-name/locations/europe-west2/taxonomies/your-taxonomy-id/policyTags/your-policy-tag-id. You must set this as a top level parameter when you call ```dfeAnalyticsDataform()```. You may in addition set this at key level for fields to override this value - for example, if you want to set specific data masking rules on particular fields. You may not override this parameter at key level for fields that are not your primary key, or ```created_at``` or ```updated_at```. If not set then no fields will be hidden.
- ```hidePrimaryKey``` - boolean; optional; table level. If ```true``` the primary key for this table will have the policy tag at ```hiddenPolicyTagLocation``` attached to it. The primary key is ```id``` by default, but may be configured via the table-level ```primaryKey``` parameter.
- ```hidden``` - boolean; optional; key level. Set this to ```true``` for an individual key within a table to ensure that this field has a policy tag attached to it.

dfe-analytics-dataform also contains two assertions which will fail and alert you when the data being streamed into the ```data``` and ```hidden_data``` fields in the ```events``` table does not match the configuration specified in the parameters above:
- ```hidden_pii_configuration_does_not_match_events_streamed_yesterday``` will fail when events were streamed since the beginning of yesterday which did not match your hidden field configuration. To prevent this failure, change your ```dfe-analytics``` configuration (analytics_hidden_pii.yml file) and/or your ```dfe-analytics-dataform``` configuration (parameters above) so that they match.
- ```hidden_pii_configuration_does_not_match_sample_of_historic_events_streamed``` will fail when some events in a sample representing 1% of historic events did not match your hidden field configuration. To prevent this failure, update these events to migrate the fields in ```data``` into ```hidden_data``` using the ```your-eventSourceName-here_migrate_historic_events_to_current_hidden_pii_configuration``` stored procedure which ```dfe-analytics-dataform``` will create in your Dataform output dataset.

If either of these assertions fail, most tables generated by ```dfe-analytics-dataform``` will not be updated. This is essential to ensure that data hidden fields is not copied into non-hidden fields in other tables generated by ```dfe-analytics-dataform```.

#### Hiding fields in tables *not* managed by ```dfe-analytics-dataform```
```dfe-analytics-dataform``` is only capable of hiding fields in tables that it generates. If data is present in other tables in your BigQuery project, you must ensure that the correct policy tag is attached to these fields.

Google provides [documentation](https://cloud.google.com/dataform/docs/policy-tags) describing how to do this for tables generated by Dataform (but not by ```dfe-analytics-dataform```).

To locate fields in your GCP project which have names which indicate that they may be human-readable Personally Identifiable Information (PII) or GDPR special category data, you may wish to execute [this shell script](https://github.com/DFE-Digital/teacher-services-analytics-cloud/blob/main/scripts/bq-assurance/iam-01-corporate-credentials.sh) in the GCP cloud console, from your local machine or within a Github codespace. This will identify all field names anywhere within your GCP project (except datasets beginning ```dataform``` other than the ```dataform``` dataset itself, because these are likely Dataform development or assertion datasets) which contain the patterns ```email```, ```name```, ```dob```, ```birth```, ```national```, ```insurance```, ```trn```, ```phone```, ```postcode```, ```address```, ```mobile```, ```passport```, ```driv```, ```ethni```, ```religio```, ```union```, ```sex```, ```gender```, ```orientation``` or ```disabilit``` (case insensitive), but do not have a policy tag attached.

#### Migration away from pseudonymisation of fields in streamed data to using hidden fields
```dfe-analytics``` supports pseudonymisation of fields *before* they are streamed into the source ```events``` table in BigQuery. However, it is recommended that services which use dfe-analytics migrate away from using this functionality and use hidden fields instead.

For most users the migration process should be straightforward, following the setup instructions above and the corresponding instructions for ```dfe-analytics```. However, for Dataform pipelines which make use of pseudonymised fields within the ```field_updates``` and/or ```version``` tables - including the primary key - a period of dual running is recommended:
1. Configure hidden fields in ```dfe-analytics```, and remove all fields from ```analytics_pii.yml```. This will mean that all newly streamed data in your source ```events``` table is hidden, but not pseudonymised.
2. Follow the instructions above to configure hidden fields in ```dfe-analytics-dataform```. Ensure that you follow the recommended policy tag configuration (SHA256 masking) and that at this stage you do *not* invoke the ```migrate_historic_events_to_current_hidden_pii_configuration``` stored procedure.
3. In addition, add ```coalesceWithLegacyPII: true``` to the ```dataSchema``` for all fields which your Dataform pipeline makes use of in either a ```field_updates``` and/or a ```version``` table; and likewise add ```coalescePrimaryKeyWithLegacyPII: true``` *at table level* to the ```dataSchema``` for all tables which use a pseudonymised primary key and for which your Dataform pipeline uses the ```field_updates``` and/or ```version``` tables. ```dfe-analytics-dataform``` will ensure that these hidden fields are both hidden *and* pseudonymised in all tables it generates.
4. Run a full ```dfe-analytics``` import.
5. Wait for a period of time that is long enough that you no longer need any of the pseudonymisation events data, or data from the ```field_updates``` and ```version``` tables from before the import. This may be a number of months or a year.
6. Delete all ```create_entity```, ```update_entity```, ```delete_entity``` and ```import_entity``` events from your source events table with ```occurred_at``` earlier than your import timestamp.
7. Remove all ```coalesceWithLegacyPII``` and ```coalescePrimaryKeyWithLegacyPII``` parameters from your ```dataSchema```, run a full refresh on your pipeline, and invoke the ```migrate_historic_events_to_current_hidden_pii_configuration``` stored procedure. Your hidden fields will be hidden but no longer pseudonymised at rest.

### Primary and foreign key constraints
```dfe-analytics-dataform``` automatically applies BigQuery primary key constraints to either the ```id``` field or the field set to be the primary key with the table level ```primaryKey``` parameter.

Optionally it also allows configuration of BigQuery foreign key constraints.

These constraints may accelerate ```JOIN``` performance. See [here](https://cloud.google.com/blog/products/data-analytics/join-optimizations-with-bigquery-primary-and-foreign-keys) for more information.

To configure these, add parameters to the configuration for that key in your dataSchema so that it looks like:
```
    {
      keyName: "other_thing_id",
      dataType: "string",
      description: "Description of this field to include in metadata here.",
      foreignKeyTable: "other_things", // The name of the table the other_thing_id foreign key in this table relates *to* in your application database
      foreignKeyName: "other_thing_id" // The name of the primary key in the foreignKeyTable table the other_thing_id foreign key relates to.
    }
```

If ```foreignKeyName``` is omitted, it defaults to ```id```.

## Tables, assertions, and declarations this will create
For each occurrence of ```dfeAnalyticsDataform()``` in ```definitions/dfe_analytics_dataform.js``` this package will create the following automatically in your Dataform project. You can view and manage these within the Dataform UI by opening ```definitions/dfe_analytics_dataform.js```.

The names of these will vary depending on the ```eventSourceName``` you have specified. For example if your ```eventSourceName``` was ```foo``` then the following will be created:
- An incremental table called ```events_foo```, which you can access within a Dataform declaration using something like ```SELECT * FROM ${ref("events_foo")}```. This table will include all the events ```dfe-analytics``` streamed into the raw events table, filtered by namespace (if you configured this). It will also include details of the browser and operating system of the user who caused these events to be streamed, and will attach web request data (like the request path) to all events in the table, not just the web requests.
- An assertion called ```foo_events_data_not_fresh``` which will fail if no new events have been streamed in the last day (or number of days specified in your ```eventsDataFreshnessDays``` parameter).
- An incremental table called ```foo_entity_version```, containing each version of every entity in the database over time, with a ```valid_from``` and ```valid_to``` timestamp.
- A table called ```foo_data_schema_json_latest```, which is a default dataSchema JSON you could use to get started specifying this in dfe_analytics_dataform.js
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, tables or views (depending on the ```materialisation``` set) called something like ```bar_version_foo``` and ```bar_latest_foo```. ```bar_version_foo``` is a denormalised ('flattened') version of ```foo_version```, flattened according to the schema for ```foo``` you specified in ```dataSchema```. ```bar_latest_foo``` is the same as ```bar_version_foo``` except that it only includes the latest version of each entity (i.e. with ```valid_to IS NULL```). Both tables and fields within them will have metadata set to match the descriptions set in ```dataSchema```.
- Table valued functions called something like ```bar_at_foo``` which can be used to generate a past version of a table at a specified timestamp.
- If you have configured an entity-level data freshness assertion by including an entity-level ```dataFreshnessDays``` parameter in your ```dataSchema```, an assertion called something like ```bar_data_not_fresh_foo``` which fails if no new Create, Update or Delete events have been received in the last ```dataFreshnessDays``` days.
- If you have set ```compareChecksums: true```, an assertion called something like ```foo_entity_ids_do_not_match``` which fails if a checksum or a row count in an ```entity_table_check``` event streamed yesterday does not match the corresponding checksum and/or row count in the corresponding data in BigQuery.
- An assertion called something like ```foo_entity_import_ids_do_not_match``` which fails if a checksum or a row count in an ```import_entity_table_check``` event streamed yesterday does not match the corresponding checksum and/or row count in the corresponding batch of import events.
- Tables called ```entity_table_check_import_foo``` and ```entity_table_check_scheduled_foo``` which allow you to monitor data load completeness by comparing checksums and row counts sent from ```dfe-analytics``` to checksums and row counts calculated from your events table.
- Assertions to help spot when your ```dataSchema``` has become out of date or has a problem. These will tell you if ```foo_entities_are_missing_expected_fields``` or if ```foo_unhandled_field_or_entity_is_being_streamed```. The former will halt your pipeline from executing, while the latter will just alert you to the assertion failure.
- A table called ```foo_entity_field_updates```, which contains one row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to any ```updated_at``` fields are not included, but ```NULL``` values are.
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, a table called something like ```bar_field_updates_foo```. ```bar_field_updates_foo``` is a denormalised ('flattened') version of ```foo_entity_field_updates```, filtered down to the entity ```bar```, and with the new and previous values of that entity flattened according to the schema for ```foo``` you specified in ```dataSchema```. Fields will have metadata set to match the descriptions set in ```dataSchema```.
- An incremental table called ```pageview_with_funnels_foo```, which contains pageview events from the events table, along with two ARRAYs of STRUCTs containing a number of pageviews in either direction for use in funnel analysis. This number of pageviews is determined by the ```funnelDepth``` parameter you may optionally call ```dfeAnalyticsDataform()``` with. By default ```funnelDepth``` is 10. Disabled if ```enableSessionTables``` is ```false```.
- A table called ```sessions_foo```, which contains rows representing user sessions with attribution fields (e.g. medium, referer_domain) for each session. Includes the session_started_at and next_session_started_at timestamps to allow attribution modelling of a goal conversion that occurred between those timestamps. Disabled if ```enableSessionTables``` is ```false```.
- A table called ```dfe_analytics_configuration_foo``` which contains details of the configuration of ```dfe-analytics``` for ```foo``` over time, with ```valid_from``` and ```valid_to``` fields
- A stored procedure called ```pseudonymise_request_user_ids``` in the same dataset as the events table ```dfe-analytics``` streams data into (not necessarily the same dataset that Dataform outputs to). You can invoke this to convert raw user_ids in the events table to pseudonymised user IDs following the instructions in the procedure metadata.
- A stored procedure called ```foo_migrate_historic_events_to_current_hidden_pii_configuration``` in your output Dataform dataset. You can invoke this to migrate past entity CRUD/import events to ensure that fields in the ```data``` and ```hidden_data``` arrays in your source events table and ```events_foo``` table are in the array field that matches the hidden field configuration in your ```dataSchema```.
- Assertions called ```foo_hidden_pii_configuration_does_not_match_events_streamed_yesterday``` and ```foo_hidden_pii_configuration_does_not_match_sample_of_historic_events_streamed```. See section "Hidden fields" above for more information.
