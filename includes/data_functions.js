/* Parses a string as a timestamp, attempting multiple formats. If timezone is not present, assumes timezone is Europe/London. If unable to parse the string as a timestamp in any of the formats, returns NULL (not an error). */

function stringToTimestamp(string) {
    return `COALESCE(
    SAFE.PARSE_TIMESTAMP(
      '%FT%H:%M:%E*S%Ez',
      TRIM(
        ${string},
        '\\"'
      )
    ),
    SAFE.PARSE_TIMESTAMP(
      '%FT%T%Ez',
      TRIM(
        ${string},
        '\\"'
      )
    ),
    SAFE.PARSE_TIMESTAMP(
      '%e %B %Y %R',
      TRIM(
        ${string},
        '\\"'
      ),
      "Europe/London"
    ),
    SAFE.PARSE_TIMESTAMP(
      '%e %B %Y %l:%M%p',
      REPLACE(
        REPLACE(
          TRIM(
            ${string},
            '\\"'
          ),
          "pm",
          "PM"
        ),
        "am",
        "AM"
      ),
      "Europe/London"
    )
  )`
};

/* Parses a string as a date, attempting multiple formats (including a timestamp cast to a date). If unable to parse the string as a date in any of the formats, returns NULL (not an error). */

function stringToDate(string) {
    return `COALESCE(
    SAFE.PARSE_DATE(
      '%F',
      ${string}
    ),
    SAFE.PARSE_DATE(
      '%e %B %Y',
      ${string}
    ),
    CAST(${stringToTimestamp(string)} AS DATE)
  )`
};

/* Shortcut to extract a string like [3,75,2,1] from a DATA struct using eventDataExtract and then convert it into an ARRAY of integers. */

function stringToIntegerArray(string) {
    return `ARRAY(
    SELECT
      step_int
    FROM
      (
        SELECT
          SAFE_CAST(step AS INT64) AS step_int
        FROM
          UNNEST(
            SPLIT(
              TRIM(
                ${string},
                "[]"
              ),
              ","
            )
          ) AS step
      )
    WHERE
      step_int IS NOT NULL
  )`
};

/* The events table uses a repeated STRUCT field containing field names key and value to store data about the event as a NoSQL-style set (document) of key-value pairs. Value is in turn a repeated field within the STRUCT. This function extracts the value of a given key from said repeated STRUCTS. If more than one value is present for key or in the unlikely event that the same key occurs multiple times, returns a comma-separated list of all values for this key. If the only values are empty strings or not present, returns NULL */

function eventDataExtract(dataField, keyToExtract, dynamic = false, dataType = 'string', isArray = false) {
    var condition = `key = "${keyToExtract}"`;
    if (dynamic) {
        condition = `key = ${keyToExtract}`;
        if (keyToExtract === 'key') {
            throw new Error("eventDataExtract cannot be used in dynamic mode to extract the value from DATA with a key in DATA that matches a field/variable named key. Try giving your key field/variable an alias first before passing it to eventDataExtract.");
        }
    };

    var thisValueWithDataTypeAppliedSql = `this_value`;
    if (dataType == "string") {
        thisValueWithDataTypeAppliedSql = 'this_value';
    } else if (dataType == "boolean") {
        thisValueWithDataTypeAppliedSql = `SAFE_CAST(LOWER(this_value) AS BOOL)`;
    } else if (dataType == "timestamp") {
        thisValueWithDataTypeAppliedSql = stringToTimestamp('this_value');
    } else if (dataType == "date") {
        thisValueWithDataTypeAppliedSql = stringToDate('this_value');
    } else if (dataType == "integer") {
        thisValueWithDataTypeAppliedSql = `SAFE_CAST(this_value AS INT64)`;
    } else if (dataType == "integer_array") {
        if (isArray) {
            throw new Error("isArray: true cannot be set for dataType: integer_array. You might just need dataType: integer instead.");
        };
        thisValueWithDataTypeAppliedSql = stringToIntegerArray('this_value');
    } else if (dataType == "float") {
        thisValueWithDataTypeAppliedSql = `SAFE_CAST(this_value AS FLOAT64)`;
    } else if (dataType == "json") {
        thisValueWithDataTypeAppliedSql = `SAFE.PARSE_JSON(this_value)`;
    } else {
        throw new Error(`Unrecognised dataType '${dataType}' for field '${keyToExtract}'. dataType should be set to boolean, timestamp, date, integer, integer_array, float, json or string or not set.`);
    };

    var arraysOfValuesToOutputDataSql = `NULLIF(STRING_AGG(${thisValueWithDataTypeAppliedSql}, ","), "")`;
    var nullSql = `NULL`;
    if (isArray) {
        arraysOfValuesToOutputDataSql = `ARRAY_AGG(${thisValueWithDataTypeAppliedSql})`;
        nullSql = `[]`;
    } else if (dataType != "string") {
        arraysOfValuesToOutputDataSql = `IF(COUNT(this_value) > 1, NULL, ANY_VALUE(${thisValueWithDataTypeAppliedSql}))`;
    };

    return `
    (
      SELECT
        IF(COUNT(this_value) = 0, ${nullSql}, ${arraysOfValuesToOutputDataSql}) AS concat_value
      FROM
        UNNEST(${dataField}), UNNEST(value) AS this_value
      WHERE
        ${condition}
    )`
};

function eventDataExtractTimestamp(dataField, keyToExtract, dynamic = false) {
    return eventDataExtract(dataField, keyToExtract, dynamic, "timestamp")
};

function eventDataExtractDate(dataField, keyToExtract, dynamic = false) {
    return eventDataExtract(dataField, keyToExtract, dynamic, "date")
};

function eventDataExtractIntegerArray(dataField, keyToExtract, dynamic = false) {
    return eventDataExtract(dataField, keyToExtract, dynamic, "integer_array")
};

/* The events table uses a repeated STRUCT field containing field names key and value to store data about the event as a NoSQL-style set (document) of key-value pairs. Value is in turn a repeated field within the STRUCT. This function extracts the value of all keys beginning key_to_extract_begins from said repeated STRUCTS and returns them as a comma-separated list of all values for this key. If the only values are empty strings or no keys begin key_to_extract_begins, returns NULL. */

function eventDataExtractListOfStringsBeginning(dataField, keyToExtractBegins, dynamic = false) {
    var condition = `STARTS_WITH(key, "${keyToExtractBegins}")`;
    if (dynamic) {
        condition = `STARTS_WITH(key, ${keyToExtractBegins})`;
        if (keyToExtractBegins === 'key') {
            throw new Error("eventDataExtractListOfStringsBeginning cannot be used in dynamic mode to extract the value from DATA with a key in DATA that matches a field/variable named key. Try giving your key field/variable an alias first before passing it to eventDataExtractListOfStringsBeginning.");
        }
    };
    return `NULLIF(
    (
      SELECT
        IF(ARRAY_LENGTH(ARRAY_CONCAT_AGG(value)) = 0, NULL, ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",")) as concat_value
      FROM
        UNNEST(${dataField})
      WHERE
        ${condition}
    ),
    ""
  )`
};

/* The events table uses a repeated STRUCT field containing field names key and value to store data about the event as a NoSQL-style set (document) of key-value pairs. Returns TRUE if a given key is present in DATA, and FALSE otherwise. */

function keyIsInEventData(dataField, keyToLookFor, dynamic = false) {
    var condition = `key = "${keyToLookFor}"`;
    if (dynamic) {
        condition = `key = ${keyToLookFor}`;
        if (keyToLookFor === 'key') {
            throw new Error("keyIsInEventData cannot be used in dynamic mode to search for the value from DATA with a key in DATA that matches a field/variable named key. Try giving your key field/variable an alias first before passing it to keyIsInEventData.");
        }
    };
    return `(
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          key
        FROM
          UNNEST(${dataField})
        WHERE
          ${condition}
      )
  ) = 1`
};

/* Sets the value of key to value within a DATA struct in a streamed event. */

function eventDataCreateOrReplace(dataField, keyToSet, valueToSet, dynamicKey = false, dynamicValue = false) {
    var keyCondition = `"""${keyToSet}"""`;
    if (dynamicKey) {
        keyCondition = keyToSet;
        if (keyToSet === 'key') {
            throw new Error("eventDataCreateOrReplace cannot be used in dynamic mode to modify a value from DATA with a key in DATA that matches a field/variable named key. Try giving your key field/variable an alias first before passing it to keyIsInEventData.");
        }
    };
    var valueCondition = `"""${valueToSet}"""`;
    if (dynamicValue) {
        valueCondition = keyToSet;
    };
    return `ARRAY_CONCAT(
    ARRAY(
      (
        SELECT
          AS STRUCT key,
          value
        FROM
          UNNEST(${dataField})
        WHERE
          key != ${keyCondition}
      )
    ),
    [
      STRUCT(
        ${keyCondition} AS key,
        [${valueCondition}] AS value
  ) ]
)`
};

/* Wait for a specified interval in the context of GoogleSQL procedural language (i.e. a SQL script, not an individual query) */
function wait(intervalString) {
    return `
    BEGIN
    DECLARE WAIT STRING;
    DECLARE DELAY_TIME DATETIME;
    SET WAIT = 'TRUE';
    SET DELAY_TIME = DATETIME_ADD(CURRENT_DATETIME, INTERVAL ${intervalString});
    WHILE WAIT = 'TRUE' DO
      IF (DELAY_TIME < CURRENT_DATETIME) THEN
         SET WAIT = 'FALSE';
      END IF;
    END WHILE;
    END;
    `;
}

/* Drops all current constraints on a table and sets the constraints specified in constraints. For use in a Dataform pre operation.
keyInForeignTable defaults to "id" if not specified. Composite keys may be specified for keyInThisTable and keyInForeignTable as a comma separated list.
e.g. 
post_operations {
    ${data_functions.setKeyConstraints(ctx, dataform, {
    primaryKey: "myidname",
    foreignKeys: [
        {keyInThisTable: "myotherkeyname", foreignTable: "myothertable", keyInForeignTable: "myotheridname"},
        {keyInThisTable: "anotherkeyname", foreignTable: "myotherothertable"},
        {keyInThisTable: "firstpartofcompositekey, secondpartofcompositekey", foreignTable: "myothertable", keyInForeignTable: "firstpartofcompositekeyinothertable, secondpartofcompositekeyinothertable"}
        ]
    })}
}
Using this function enables project level table definitions to avoid having to handle their own workarounds for the following issues:
1. Without putting the ALTER TABLE statements in a conditional IF TRUE THEN... END IF; block, although the script would execute without error, BigQuery query compilation unhelpfully returns an error on the line that attempts to add a primary key if a primary key already exists, ignoring the fact that a previous step in the script removes the primary key.
2. BigQuery has a 5 table updates per 10s limit which may be breached if too many constraint removal operations happen too quickly. The function builds in waits to avoid this.
*/
function setKeyConstraints(ctx, dataform, constraints) {
    /* Detect incorrectly formatted constraints objects */
    Object.keys(constraints).forEach(key => {
        if (!['primaryKey', 'foreignKeys'].includes(key)) {
            throw new Error(`Invalid parameter passed to setKeyConstraints() inside constraints object: ${key}. Valid top level parameters are primaryKey and foreignKeys.`);
        }
    });
    if (constraints.foreignKeys && constraints.foreignKeys.length > 0) {
        constraints.foreignKeys.forEach(foreignKey => Object.keys(foreignKey).forEach(foreignKeyParameter => {
            if (!['keyInThisTable', 'foreignTable', 'keyInForeignTable'].includes(foreignKeyParameter)) {
                throw new Error(`Invalid foreign key parameter passed to setKeyConstraints() inside constraints object: ${foreignKeyParameter}. Valid key level parameters are keyInThisTable, foreignTable and (optionally) keyInForeignTable.`);
            }
        }));
    }
    return `
    IF TRUE THEN
    /* Delete all key constraints on the table - even ones that are no longer included in dfeAnalyticsDataform() configuration */
      ALTER TABLE ${ctx.self()} DROP PRIMARY KEY IF EXISTS;
      FOR constraint_to_drop IN (
        SELECT
          SPLIT(constraint_name, ".")[1]
        FROM
          ${"`" + dataform.projectConfig.defaultDatabase + "." + dataform.projectConfig.defaultSchema + (dataform.projectConfig.schemaSuffix ? "_" + dataform.projectConfig.schemaSuffix : "" ) + ".INFORMATION_SCHEMA.TABLE_CONSTRAINTS`"}
        WHERE
          constraint_type = "FOREIGN KEY"
          AND table_name = "${ctx.name()}"
        )
      DO
        ALTER TABLE ${ctx.self()} DROP CONSTRAINT IF EXISTS constraint_to_drop;
        ${wait("2 SECOND")}
      END FOR;
    /* Set primary key */
      ${constraints.primaryKey ?
          `ALTER TABLE ${ctx.self()} ADD PRIMARY KEY(${constraints.primaryKey}) NOT ENFORCED;
          ${wait("2 SECOND")}`
          : ``}
    /* Set foreign key constraints */
      ${constraints.foreignKeys && constraints.foreignKeys.length > 0 ?
      `ALTER TABLE ${ctx.self()}
          ${constraints.foreignKeys.map(foreignKey =>
              `ADD CONSTRAINT ${foreignKey.keyInThisTable.replaceAll(",", "_").replaceAll(" ", "")}_relationship FOREIGN KEY(${foreignKey.keyInThisTable}) REFERENCES ${ctx.ref(foreignKey.foreignTable)}(${foreignKey.keyInForeignTable || "id"}) NOT ENFORCED`).join(',\n')
              };`
      : ``}
      END IF;
  `;
}


// Referer URLs in events include URI-formatted codes for some characters e.g. '%20' for ' '. This JS function parses them.
function decodeUriComponent(url) {
  return `
    (
      SELECT
        STRING_AGG(
          IF(
            REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'),
            SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))),
            y
          ),
          '' ORDER BY i
        )
      FROM UNNEST(REGEXP_EXTRACT_ALL(${url}, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y WITH OFFSET AS i
    )
  `;
}


module.exports = {
    stringToTimestamp,
    stringToDate,
    stringToIntegerArray,
    eventDataExtract,
    eventDataExtractTimestamp,
    eventDataExtractDate,
    eventDataExtractIntegerArray,
    eventDataExtractListOfStringsBeginning,
    keyIsInEventData,
    eventDataCreateOrReplace,
    wait,
    setKeyConstraints,
    decodeUriComponent
};
