# dfe-analytics-dataform
Dataform package containing commonly used SQL functions and table definitions, for use with event data streamed to BigQuery using DfE-Digital/dfe-analytics: https://github.com/DFE-Digital/dfe-analytics.

## How to install
1. Set up your Dataform project.
2. Ensure that it is connected to your BigQuery project.
3. Ensure that it is synchronised with its own dedicated Github repository.
4. Add the following line within the dependencies block of the package.json file in your Dataform project:
```
"dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git"
```
It should now look something like:
```
{
    "dependencies": {
        "@dataform/core": "1.21.1",
        "dfe-analytics-dataform": "git+https://github.com/DFE-Digital/dfe-analytics-dataform.git"
    }
}
```
5. Click the 'Install Packages' button on the right hand side of the package.json screen. This will also update package-lock.json automatically.
6. Create a file called includes/data_functions.js containing the following line:
```
module.exports = require("dfe-analytics-dataform/includes/data_functions");
```

## Using the functions in your queries
Dataform allows you to break into Javascript within a SQLX file using the syntax ```${Your Javascript goes here.}```. This means that you can use the functions in the data_functions module provided by this package within SQL queries in the rest of your Dataform project.

The examples below assume that you have an events table created by the dfe-analytics gem which contains a field called ```DATA``` which is an ARRAY of STRUCTs named ```DATA.key``` and ```DATA.value```:
- Extract the value of a given ```key``` from within ```DATA```. If more than one value is present for ```key``` or in the unlikely event that the same ```key``` occurs multiple times, returns a comma-separated list of all values for this key. If the only values are empty strings or not present, returns ```NULL```.

> ```${data_functions.eventDataExtract("DATA","key")}```

- Extract the value of all ```key```s beginning with the string ```key_to_extract_begins``` from DATA and return them as a comma-separated list of all ```value```s for this ```key```. If the only ```value```s are empty strings or no keys begin ```key_to_extract_begins```, returns ```NULL```.

> ```${data_functions.eventDataExtractListOfStringsBeginning("DATA","key_to_extract_begins")}```

- Return ```TRUE``` if a given ```key``` is present in ```DATA```, and ```FALSE``` otherwise

> ```${data_functions.keyIsInEventData("DATA","key")}```

- Shortcut to run ```eventDataExtract``` and then parse the string extracted as a timestamp, attempting multiple formats. If timezone is not present, assumes timezone is Europe/London. If unable to parse the string as a timestamp in any of the formats, returns ```NULL``` (not an error).

> ```${data_functions.eventDataExtractTimestamp("DATA","key")}```

- Shortcut to run ```eventDataExtract``` and then parse the string extracted as a date, attempting multiple formats. If unable to parse the string as a date in any of the formats, returns ```NULL``` (not an error).

> ```${data_functions.eventDataExtractDate("DATA","key")}```

- Shortcut to extract a string like ```[3,75,2,1]``` from ```DATA``` using ```event_data_extract``` and then convert it into an array of integers.

> ```${data_functions.eventDataExtractIntegerArray("DATA","your_key_name_here")}```

- Sets or replaces the ```value``` of ```key``` to/with ```value``` and returns the entirety of a new version of ```DATA```, having done this

> ```${data_functions.eventDataCreateOrReplace("DATA","key","value")}```
