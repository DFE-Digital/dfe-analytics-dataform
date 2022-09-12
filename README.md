# dfe-analytics-dataform
Dataform package containing commonly used SQL functions and table definitions, for use with event data streamed to BigQuery using DfE-Digital/dfe-analytics: https://github.com/DFE-Digital/dfe-analytics.

## How to install
1. Set up your Dataform project.
2. Ensure that it is connected to your BigQuery project.
3. Ensure that it is synchronised with its own dedicated Github repository.
4. Add the following line within the dependencies block of the package.json file in your Dataform project:
```
"dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git#v0.10.0"
```
It should now look something like:
```
{
    "dependencies": {
        "@dataform/core": "1.22.0",
        "dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git#v0.10.0"
    }
}
```
5. Click the 'Install Packages' button on the right hand side of the package.json screen. This will also update package-lock.json automatically.
6. Create a file called ```includes/data_functions.js``` containing the following line:
```
module.exports = require("dfe-analytics-dataform/includes/data_functions");
```
7. Create a second file called definitions/dfe_analytics_dataform.js that looks like the following:
```
const dfeAnalyticsDataform = require("dfe-analytics-dataform");

// Repeat the lines below for each and every events table you want dfe-analytics-dataform to process in your Dataform project - distinguish between them by giving each one a different eventSourceName. This will cause all the tables produced automatically by dfe-analytics-dataform to have your suffix included in them to allow users to tell the difference between them.
dfeAnalyticsDataform({
  eventSourceName: "Short name for your event source here - this might be a short name for your service, for example",
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
8. Optionally, to save time if you're starting from scratch, generate a blank ```dataSchema``` JSON to paste in to this file by running the query in Dataform to create the ```data_schema_json_latest``` table. You can do this from the right hand sidebar when you open ```dfe_analytics_dataform``` in the Dataform web client, and then copying and pasting the output from the table this produces in BigQuery (don't copy and paste from Dataform as it doesn't handle newlines well). This will automatically attempt to work out table names, field names and data types for each field using data streamed the previous day and today - although you might need to add in any tables that didn't have entity events streamed yesterday/today manually, or tweak some data types.

9. Replace the parameters in this file with the parameters you need - including specifying the full schema from your ```analytics.yml``` file with data types and any aliases you need. Don't include the ```id```, ```created_at``` and ```updated_at``` fields - they are included automatically. At this stage compilation errors like ```Error: Not found: Dataset your-project-name:output-dataset-name was not found in location europe-west2 at [1:44]``` are normal because you haven't yet run the pipeline for the first time, which creates the output dataset.

10. Commit your changes and merge to ```master```.

11. Run a 'full refresh' on your entire pipeline, and resolve any errors this flags (e.g. omissions made when specifying a ```dataSchema```).

## Tables, assertions, and declarations this will create
For each occurrence of ```dfeAnalyticsDataform()``` in ```definitions/dfe_analytics_dataform.js``` this package will create the following automatically in your Dataform project. You can view and manage these within the UI by opening ```definitions/dfe_analytics_dataform.js```.

The names of these will vary depending on the ```eventSourceName``` you have specified. For example if your ```eventSourceName``` was ```foo``` then the following will be created:
- A declaration of your events table, which you can access via ```${ref("bqDatasetName","bqEventsTableName")}``` (replacing those values with your own).
- An incremental table called ```events_foo```, which you can access via ```${ref("events_foo")}```. This table will include all the events dfe-analytics streamed into the raw events table. It will also include details of the browser and operating system of the user who caused these events to be streamed, and will attach web request data (like the request path) to all events in the table, not just the web requests.
- An incremental table called ```foo_entity_version```, containing each version of every entity in the database over time, with a ```valid_from``` and ```valid_to``` timestamp.
- A table called ```foo_analytics_yml_latest```, which is a table version of the ```dataSchema``` you specified.
- A table called ```foo_data_schema_json_latest```, which is a default dataSchema JSON you could use to get started specifying this in dfe_analytics_dataform.js
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, tables called something like ```bar_version_foo``` and ```bar_latest_foo```. ```bar_version_foo``` is a denormalised ('flattened') version of ```foo_version```, flattened according to the schema for ```foo``` you specified in ```dataSchema```. ```bar_latest_foo``` is the same as ```bar_version_foo``` except that it only includes the latest version of each entity (i.e. with ```valid_to IS NULL```). Both tables and fields within them will have metadata set to match the descriptions set in ```dataSchema```.
- Table valued functions called something like ```bar_at_foo``` which can be used to generate a past version of a table at a specified timestamp.
- Assertions to help spot when your ```dataSchema``` has become out of date or has a problem. These will tell you if ```foo_entities_are_missing_expected_fields``` or if ```foo_unhandled_field_or_entity_is_being_streamed```. The former will halt your pipeline from executing, while the latter will just alert you to the assertion failure.
- A table called ```foo_entity_field_updates```, which contains one row for each time a field was updated for any entity that is streamed as events from the database, setting out the name of the field, the previous value of the field and the new value of the field. Entity deletions and updates to any ```updated_at``` fields are not included, but ```NULL``` values are.
- For each ```entityTableName``` you specified in ```dataSchema``` like ```bar```, a table called something like ```bar_field_updates_foo```. ```bar_field_updates_foo``` is a denormalised ('flattened') version of ```foo_entity_field_updates```, filtered down to the entity ```bar```, and with the new and previous values of that entity flattened according to the schema for ```foo``` you specified in ```dataSchema```. Fields will have metadata set to match the descriptions set in ```dataSchema```.
- An incremental table called ```pageview_with_funnels_foo```, which contains pageview events from the events table, along with two ARRAYs of STRUCTs containing a number of pageviews in either direction for use in funnel analysis. This number of pageviews is determined by the ```funnelDepth``` parameter you may optionally call ```dfeAnalyticsDataform()``` with. By default ```funnelDepth``` is 10.

## Using the functions in your queries
Dataform allows you to break into Javascript within a SQLX file using the syntax ```${Your Javascript goes here.}```. If you created includes/data_functions.js then this means that you can use the functions in the data_functions module provided by this package within SQL queries in the rest of your Dataform project.

The examples below assume that you have an events table created by the dfe-analytics gem which contains a field called ```DATA``` which is an ARRAY of STRUCTs named ```DATA.key``` and ```DATA.value```:
- Extract the value of a given ```key``` from within ```DATA```. If more than one value is present for ```key``` or in the unlikely event that the same ```key``` occurs multiple times, returns a comma-separated list of all values for this key. If the only values are empty strings or not present, returns ```NULL```.

> ```${data_functions.eventDataExtract("DATA","key")}```

- Extract the value of all ```key```s beginning with the string ```key_to_extract_begins``` from DATA and return them as a comma-separated list of all ```value```s for this ```key```. If the only ```value```s are empty strings or no keys begin ```key_to_extract_begins```, returns ```NULL```.

> ```${data_functions.eventDataExtractListOfStringsBeginning("DATA","key_to_extract_begins")}```

- Return ```TRUE``` if a given ```key``` is present in ```DATA```, and ```FALSE``` otherwise

> ```${data_functions.keyIsInEventData("DATA","key")}```

- Shortcut to run ```eventDataExtract``` and then parse the string extracted as a timestamp, attempting multiple formats. If timezone is not present, assumes timezone is Europe/London. If unable to parse the string as a timestamp in any of the formats, returns ```NULL``` (not an error).

> ```${data_functions.eventDataExtractTimestamp("DATA","key")}```

- Shortcut to run ```eventDataExtract``` and then parse the string extracted as a date, attempting multiple formats (including a timestamp cast to a date). If unable to parse the string as a date in any of the formats, returns ```NULL``` (not an error).

> ```${data_functions.eventDataExtractDate("DATA","key")}```

- Shortcut to extract a string like ```[3,75,2,1]``` from ```DATA``` using ```event_data_extract``` and then convert it into an array of integers.

> ```${data_functions.eventDataExtractIntegerArray("DATA","your_key_name_here")}```

- Sets or replaces the ```value``` of ```key``` to/with ```value``` and returns the entirety of a new version of ```DATA```, having done this

> ```${data_functions.eventDataCreateOrReplace("DATA","key","value")}```

If you want to use one of the functions to extract a field whose name is set dynamically from another SQL field, add an additional ```true``` parameter to the end of the function e.g. ```${data_functions.eventDataExtract("DATA", "key", true)}```.
