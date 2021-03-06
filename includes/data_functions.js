/* The events table uses a repeated STRUCT field containing field names key and value to store data about the event as a NoSQL-style set (document) of key-value pairs. Value is in turn a repeated field within the STRUCT. This function extracts the value of a given key from said repeated STRUCTS. If more than one value is present for key or in the unlikely event that the same key occurs multiple times, returns a comma-separated list of all values for this key. If the only values are empty strings or not present, returns NULL */

function eventDataExtract(dataField, keyToExtract, dynamic = false) {
  var condition = `key = "${keyToExtract}"`;
  if (dynamic) {
    condition = `key = ${keyToExtract}`;
    if (keyToExtract === 'key') {
      throw new Error("eventDataExtract cannot be used in dynamic mode to extract the value from DATA with a key in DATA that matches a field/variable named key. Try giving your key field/variable an alias first before passing it to eventDataExtract.");
    }
  };
  return `NULLIF(
    (
      SELECT
        ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",") as concat_value
      FROM
        UNNEST(${dataField})
      WHERE
        ${condition}
    ),
    ""
  )`
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
        ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",") as concat_value
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

/* Shortcut to run eventDataExtract and then parse the string extracted as a timestamp, attempting multiple formats. If timezone is not present, assumes timezone is Europe/London. If unable to parse the string as a timestamp in any of the formats, returns NULL (not an error). */

function eventDataExtractTimestamp(dataField, keyToExtract, dynamic = false) {
  return `COALESCE(
    SAFE.PARSE_TIMESTAMP(
      '%FT%H:%M:%E*S%Ez',
      TRIM(
        ${eventDataExtract(dataField, keyToExtract, dynamic)},
        "\\""
      )
    ),
    SAFE.PARSE_TIMESTAMP(
      '%FT%T%Ez',
      TRIM(
        ${eventDataExtract(dataField, keyToExtract, dynamic)},
        "\\""
      )
    ),
    SAFE.PARSE_TIMESTAMP(
      '%e %B %Y %R',
      TRIM(
        ${eventDataExtract(dataField, keyToExtract, dynamic)},
        "\\""
      ),
      "Europe/London"
    ),
    SAFE.PARSE_TIMESTAMP(
      '%e %B %Y %l:%M%p',
      REPLACE(
        REPLACE(
          TRIM(
            ${eventDataExtract(dataField, keyToExtract, dynamic)},
            "\\""
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

/* Shortcut to run eventDataExtract and then parse the string extracted as a date, attempting multiple formats (including a timestamp cast to a date). If unable to parse the string as a date in any of the formats, returns NULL (not an error). */

function eventDataExtractDate(dataField, keyToExtract, dynamic = false) {
  return `COALESCE(
    SAFE.PARSE_DATE(
      '%F',
      ${eventDataExtract(dataField, keyToExtract, dynamic)}
    ),
    SAFE.PARSE_DATE(
      '%e %B %Y',
      ${eventDataExtract(dataField, keyToExtract, dynamic)}
    ),
    CAST(${eventDataExtractTimestamp(dataField, keyToExtract, dynamic)} AS DATE)
  )`
};

/* Shortcut to extract a string like [3,75,2,1] from a DATA struct using eventDataExtract and then convert it into an ARRAY of integers. */

function eventDataExtractIntegerArray(dataField, keyToExtract, dynamic = false) {
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
                ${eventDataExtract(dataField, keyToExtract, dynamic)},
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

/* Sets the value of key to value within a DATA struct in a streamed event. */

function eventDataCreateOrReplace(dataField, keyToSet, valueToSet) {
  return `ARRAY_CONCAT(
    ARRAY(
      (
        SELECT
          AS STRUCT key,
          value
        FROM
          UNNEST(${dataField})
        WHERE
          key != "${keyToSet}"
      )
    ),
    [
      STRUCT(
        "${keyToSet}" AS key,
        ["${valueToSet}"] AS value
  ) ]
)`
};

module.exports = {
  eventDataExtract,
  eventDataExtractListOfStringsBeginning,
  keyIsInEventData,
  eventDataExtractTimestamp,
  eventDataExtractDate,
  eventDataExtractIntegerArray,
  eventDataCreateOrReplace
};
