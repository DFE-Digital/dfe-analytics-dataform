const dataFunctions = require('../includes/data_functions');
const normalize = (sql) => sql.replace(/\s+/g, ' ').trim();
const canonicalizeSQL = (sql) =>
  sql
    .replace(/\s+/g, ' ')
    .replace(/\s*\(\s*/g, '(')
    .replace(/\s*\)\s*/g, ')')
    .trim();

describe('stringToIntegerArray', () => {
  it('should generate the correct SQL query for a valid input string', () => {
    const input = '[3,75,2,1]';
    const expectedSQL = `ARRAY(
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
                ${input},
                "[]"
              ),
              ","
            )
          ) AS step
      )
    WHERE
      step_int IS NOT NULL
  )`;

    const result = dataFunctions.stringToIntegerArray(input);
    expect(result).toBe(expectedSQL);
  });

  it('should handle an empty input string', () => {
    const input = '[]';
    const expectedSQL = `ARRAY(
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
                ${input},
                "[]"
              ),
              ","
            )
          ) AS step
      )
    WHERE
      step_int IS NOT NULL
  )`;

    const result = dataFunctions.stringToIntegerArray(input);
    expect(result).toBe(expectedSQL);
  });
});

describe('stringToDate', () => {
  it('should generate the correct SQL query for a valid input string', () => {
    const input = '"2023-10-01"';
    const expectedSQL = `COALESCE(
    SAFE.PARSE_DATE(
      '%F',
      ${input}
    ),
    SAFE.PARSE_DATE(
      '%e %B %Y',
      ${input}
    ),
    CAST(${dataFunctions.stringToTimestamp(input)} AS DATE)
  )`;

    const result = dataFunctions.stringToDate(input);
    expect(result).toBe(expectedSQL);
  });

  it('should handle an empty input string', () => {
    const input = '""';
    const expectedSQL = `COALESCE(
    SAFE.PARSE_DATE(
      '%F',
      ${input}
    ),
    SAFE.PARSE_DATE(
      '%e %B %Y',
      ${input}
    ),
    CAST(${dataFunctions.stringToTimestamp(input)} AS DATE)
  )`;

    const result = dataFunctions.stringToDate(input);
    expect(result).toBe(expectedSQL);
  });
});

describe('stringToIntegerArray', () => {
  it('should generate the correct SQL query for a valid input string', () => {
    const input = '[3,75,2,1]';
    const expectedSQL = `ARRAY(
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
                ${input},
                "[]"
              ),
              ","
            )
          ) AS step
      )
    WHERE
      step_int IS NOT NULL
  )`;

    const result = dataFunctions.stringToIntegerArray(input);
    expect(result).toBe(expectedSQL);
  });

  it('should handle an empty input string', () => {
    const input = '[]';
    const expectedSQL = `ARRAY(
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
                ${input},
                "[]"
              ),
              ","
            )
          ) AS step
      )
    WHERE
      step_int IS NOT NULL
  )`;

    const result = dataFunctions.stringToIntegerArray(input);
    expect(result).toBe(expectedSQL);
  });
});

describe('eventDataExtract', () => {
  const normalize = (sql) => sql.replace(/\s+/g, ' ').trim();

  const testCases = [
    {
      description: 'should generate the correct SQL query for extracting a string value from a static key',
      dataField: 'event_params',
      keyToExtract: 'user_id',
      dynamic: false,
      dataType: 'string',
      isArray: false,
      expectedSQL: `
        (
          SELECT
            IF(COUNT(this_value) = 0, NULL, NULLIF(STRING_AGG(this_value, ","), "")) AS concat_value
          FROM
            UNNEST(event_params), UNNEST(value) AS this_value
          WHERE
            key = "user_id"
        )`
    },
    {
      description: 'should generate the correct SQL query for extracting a dynamic key',
      dataField: 'event_params',
      keyToExtract: 'dynamic_key',
      dynamic: true,
      dataType: 'string',
      isArray: false,
      expectedSQL: `
        (
          SELECT
            IF(COUNT(this_value) = 0, NULL, NULLIF(STRING_AGG(this_value, ","), "")) AS concat_value
          FROM
            UNNEST(event_params), UNNEST(value) AS this_value
          WHERE
            key = dynamic_key
        )`
    },
    {
      description: 'should handle array data types correctly',
      dataField: 'event_params',
      keyToExtract: 'user_ids',
      dynamic: false,
      dataType: 'string',
      isArray: true,
      expectedSQL: `
        (
          SELECT
            IF(COUNT(this_value) = 0, [], ARRAY_AGG(this_value)) AS concat_value
          FROM
            UNNEST(event_params), UNNEST(value) AS this_value
          WHERE
            key = "user_ids"
        )`
    }
  ];

  testCases.forEach(({ description, dataField, keyToExtract, dynamic, dataType, isArray, expectedSQL }) => {
    it(description, () => {
      const result = dataFunctions.eventDataExtract(dataField, keyToExtract, dynamic, dataType, isArray);
      expect(normalize(result)).toEqual(normalize(expectedSQL));
    });
  });
});

describe('eventDataExtractTimestamp', () => {
  it('should generate the correct SQL query for extracting a timestamp key', () => {
    const dataField = 'event_params';
    const keyToExtract = 'event_timestamp';
    const dynamic = false;

    const expectedSQL = `
      (
        SELECT
          IF(COUNT(this_value) = 0, NULL, 
            IF(COUNT(this_value) > 1, NULL, 
              ANY_VALUE(
                COALESCE(
                  SAFE.PARSE_TIMESTAMP('%FT%H:%M:%E*S%Ez', TRIM(this_value, '\\\"')),
                  SAFE.PARSE_TIMESTAMP('%FT%T%Ez', TRIM(this_value, '\\\"')),
                  SAFE.PARSE_TIMESTAMP('%e %B %Y %R', TRIM(this_value, '\\\"'), "Europe/London"),
                  SAFE.PARSE_TIMESTAMP('%e %B %Y %l:%M%p', 
                    REPLACE(REPLACE(TRIM(this_value, '\\\"'), "pm", "PM"), "am", "AM"), 
                    "Europe/London"
                  )
                )
              )
            )
          ) AS concat_value
        FROM
          UNNEST(${dataField}), UNNEST(value) AS this_value
        WHERE
          key = "${keyToExtract}"
      )`;

    const result = dataFunctions.eventDataExtractTimestamp(dataField, keyToExtract, dynamic);

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});

describe('eventDataExtractDate', () => {
  it('should generate the correct SQL query for extracting a date key', () => {
    const dataField = 'event_params';
    const keyToExtract = 'event_date';
    const dynamic = false;

    const expectedSQL = `
      (SELECT IF(COUNT(this_value)= 0, NULL, 
        IF(COUNT(this_value)> 1, NULL, 
          ANY_VALUE(COALESCE(
            SAFE.PARSE_DATE('%F', this_value), 
            SAFE.PARSE_DATE('%e %B %Y', this_value), 
            CAST(COALESCE(
              SAFE.PARSE_TIMESTAMP('%FT%H:%M:%E*S%Ez', TRIM(this_value, '\\\"')), 
              SAFE.PARSE_TIMESTAMP('%FT%T%Ez', TRIM(this_value, '\\\"')), 
              SAFE.PARSE_TIMESTAMP('%e %B %Y %R', TRIM(this_value, '\\\"'), "Europe/London"), 
              SAFE.PARSE_TIMESTAMP('%e %B %Y %l:%M%p', REPLACE(REPLACE(TRIM(this_value, '\\\"'), "pm", "PM"), "am", "AM"), "Europe/London")
            ) AS DATE)
          ))
        )
      ) AS concat_value 
      FROM UNNEST(event_params), UNNEST(value) AS this_value 
      WHERE key = "event_date")
    `;

    const result = dataFunctions.eventDataExtract(dataField, keyToExtract, dynamic, 'date', false);
    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});

describe('eventDataExtractIntegerArray', () => {
  it('should generate the correct SQL query for extracting an integer array', () => {
    const dataField = 'event_params';
    const keyToExtract = 'event_integer_array';
    const dynamic = false;

    const expectedSQL = `
      (SELECT IF(COUNT(this_value)= 0, NULL, 
        IF(COUNT(this_value)> 1, NULL, 
          ANY_VALUE(ARRAY(
            SELECT step_int 
            FROM (
              SELECT SAFE_CAST(step AS INT64) AS step_int 
              FROM UNNEST(SPLIT(TRIM(this_value, "[]"), ",")) AS step
            ) 
            WHERE step_int IS NOT NULL
          ))
        )
      ) AS concat_value 
      FROM UNNEST(event_params), UNNEST(value) AS this_value 
      WHERE key = "event_integer_array")
    `;

    const result = dataFunctions.eventDataExtract(dataField, keyToExtract, dynamic, 'integer_array', false);
    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});