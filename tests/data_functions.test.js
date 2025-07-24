const dataFunctions = require('../includes/data_functions');
const { canonicalizeSQL } = require('./helpers/sql');

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

describe('eventDataExtractListOfStringsBeginning', () => {
  it('should return a SQL query to extract comma-separated values for keys starting with a specific prefix', () => {
    const dataField = 'event_params';
    const keyToExtractBegins = 'prefix_';
    const dynamic = false;

    const expectedSQL = `
      NULLIF(
        (SELECT IF(
          ARRAY_LENGTH(ARRAY_CONCAT_AGG(value)) = 0,
          NULL,
          ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",")
        ) AS concat_value
        FROM UNNEST(event_params)
        WHERE STARTS_WITH(key, "prefix_")),
        ""
      )
    `;

    const result = dataFunctions.eventDataExtractListOfStringsBeginning(dataField, keyToExtractBegins, dynamic);

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });

  it('should return NULL if no keys match the prefix or all values are empty strings', () => {
    const dataField = 'event_params';
    const keyToExtractBegins = 'nonexistent_prefix_';
    const dynamic = false;

    const expectedSQL = `
      NULLIF(
        (SELECT IF(
          ARRAY_LENGTH(ARRAY_CONCAT_AGG(value)) = 0,
          NULL,
          ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",")
        ) AS concat_value
        FROM UNNEST(event_params)
        WHERE STARTS_WITH(key, "nonexistent_prefix_")),
        ""
      )
    `;

    const result = dataFunctions.eventDataExtractListOfStringsBeginning(dataField, keyToExtractBegins, dynamic);

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});

describe('eventDataExtractListOfStringsBeginning', () => {
  it('should return a SQL query to extract comma-separated values for keys starting with a specific prefix', () => {
    const dataField = 'event_params';
    const keyToExtractBegins = 'prefix_';
    const dynamic = false;

    const expectedSQL = `
      NULLIF(
        (SELECT IF(
          ARRAY_LENGTH(ARRAY_CONCAT_AGG(value)) = 0,
          NULL,
          ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",")
        ) AS concat_value
        FROM UNNEST(event_params)
        WHERE STARTS_WITH(key, "prefix_")),
        ""
      )
    `;

    const result = dataFunctions.eventDataExtractListOfStringsBeginning(dataField, keyToExtractBegins, dynamic);

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });

  it('should return NULL if no keys match the prefix or all values are empty strings', () => {
    const dataField = 'event_params';
    const keyToExtractBegins = 'nonexistent_prefix_';
    const dynamic = false;

    const expectedSQL = `
      NULLIF(
        (SELECT IF(
          ARRAY_LENGTH(ARRAY_CONCAT_AGG(value)) = 0,
          NULL,
          ARRAY_TO_STRING(ARRAY_CONCAT_AGG(value), ",")
        ) AS concat_value
        FROM UNNEST(event_params)
        WHERE STARTS_WITH(key, "nonexistent_prefix_")),
        ""
      )
    `;

    const result = dataFunctions.eventDataExtractListOfStringsBeginning(dataField, keyToExtractBegins, dynamic);

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});

describe('eventDataCreateOrReplace', () => {
  it('should generate SQL to replace the value of an existing key in the event data', () => {
    const eventData = [
      { key: 'key1', value: 'value1' },
      { key: 'key2', value: 'value2' },
    ];
    const keyToReplace = 'key1';
    const newValue = 'newValue1';

    const result = dataFunctions.eventDataCreateOrReplace(eventData, keyToReplace, newValue);

    const expectedSQL = `
      ARRAY_CONCAT(
        ARRAY(
          (
            SELECT
              AS STRUCT key,
              value
            FROM
              UNNEST([object Object],[object Object])
            WHERE
              key != "key1"
          )
        ),
        [
          STRUCT(
            "key1" AS key,
            ["newValue1"] AS value
          )
        ]
      )
    `;

    expect(canonicalizeSQL(result)).toEqual(canonicalizeSQL(expectedSQL));
  });
});

describe('wait', () => {
  test('should generate correct SQL for a valid interval string', () => {
    const intervalString = '1 SECOND';
    const result = dataFunctions.wait(intervalString);
    const expectedSQL = `BEGIN
    DECLARE WAIT STRING;
    DECLARE DELAY_TIME DATETIME;
    SET WAIT = 'TRUE';
    SET DELAY_TIME = DATETIME_ADD(CURRENT_DATETIME, INTERVAL ${intervalString});
    WHILE WAIT = 'TRUE' DO
      IF (DELAY_TIME < CURRENT_DATETIME) THEN
        SET WAIT = 'FALSE';
      END IF;
    END WHILE;
  END;`;
    expect(canonicalizeSQL(result)).toBe(canonicalizeSQL(expectedSQL));
  });
});

describe('setKeyConstraints', () => {
  test('setKeyConstraints generates correct SQL for primary and foreign keys', () => {
    const ctx = {
      self: () => '`project.dataset.table`',
      name: () => 'table',
      ref: (table) => `\`project.dataset.${table}\``
    };

    const dataform = {
      projectConfig: {
        defaultDatabase: 'project',
        defaultSchema: 'dataset',
        schemaSuffix: ''
      }
    };

    const constraints = {
      primaryKey: 'myidname',
      foreignKeys: [
        { keyInThisTable: 'myotherkeyname', foreignTable: 'myothertable', keyInForeignTable: 'myotheridname' },
        { keyInThisTable: 'anotherkeyname', foreignTable: 'myotherothertable' },
        {
          keyInThisTable: 'firstpartofcompositekey, secondpartofcompositekey',
          foreignTable: 'myothertable',
          keyInForeignTable: 'firstpartofcompositekeyinothertable, secondpartofcompositekeyinothertable'
        }
      ]
    };

    const result = dataFunctions.setKeyConstraints(ctx, dataform, constraints);

    const expectedSQL = `
      IF TRUE THEN
      /* Delete all key constraints on the table - even ones that are no longer included in dfeAnalyticsDataform() configuration */
        ALTER TABLE \`project.dataset.table\` DROP PRIMARY KEY IF EXISTS;
        FOR constraint_to_drop IN (
          SELECT
            SPLIT(constraint_name, ".")[1]
          FROM
            \`project.dataset.INFORMATION_SCHEMA.TABLE_CONSTRAINTS\`
          WHERE
            constraint_type = "FOREIGN KEY"
            AND table_name = "table"
          )
        DO
          ALTER TABLE \`project.dataset.table\` DROP CONSTRAINT IF EXISTS constraint_to_drop;
          ${dataFunctions.wait("2 SECOND")}
        END FOR;
      /* Set primary key */
        ALTER TABLE \`project.dataset.table\` ADD PRIMARY KEY(myidname) NOT ENFORCED;
        ${dataFunctions.wait("2 SECOND")}
      /* Set foreign key constraints */
        ALTER TABLE \`project.dataset.table\`
            ADD CONSTRAINT myotherkeyname_relationship FOREIGN KEY(myotherkeyname) REFERENCES \`project.dataset.myothertable\`(myotheridname) NOT ENFORCED,
            ADD CONSTRAINT anotherkeyname_relationship FOREIGN KEY(anotherkeyname) REFERENCES \`project.dataset.myotherothertable\`(id) NOT ENFORCED,
            ADD CONSTRAINT firstpartofcompositekey_secondpartofcompositekey_relationship FOREIGN KEY(firstpartofcompositekey, secondpartofcompositekey) REFERENCES \`project.dataset.myothertable\`(firstpartofcompositekeyinothertable, secondpartofcompositekeyinothertable) NOT ENFORCED;
        END IF;
    `;

    expect(canonicalizeSQL(result)).toBe(canonicalizeSQL(expectedSQL));
  });
});


describe('eventDataExtractListOfStrings', () => {
  test("decodeUriComponent generates correct SQL for decoding URI components", () => {
    const url = "`events.referer_url`";

    const result = dataFunctions.decodeUriComponent(url);
    const expectedSQL = `(
        SELECT
          STRING_AGG(
            IF(
              REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'),
              SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))),
              y
            ),
            '' ORDER BY i
          )
        FROM UNNEST(REGEXP_EXTRACT_ALL(\`events.referer_url\`, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y WITH OFFSET AS i
      )
    `

    expect(canonicalizeSQL(result)).toBe(canonicalizeSQL(expectedSQL));
  });
});

describe('standardisePathQuery', () => {
  test("standardisePathQuery generates correct SQL for standardising path and query", () => {
    const path_query = "`events.page_path_and_query`";

    const result = dataFunctions.standardisePathQuery(path_query);
    const expectedSQL = `CASE WHEN \`events.page_path_and_query\` IS NOT NULL THEN
        SPLIT(\`events.page_path_and_query\`, '?')[SAFE_OFFSET(0)] || 
        IF(
          ARRAY_LENGTH(SPLIT(\`events.page_path_and_query\`, '?')) > 1,
          '?' || (
            SELECT STRING_AGG(DISTINCT key_value_pair, '&' ORDER BY key_value_pair)
            FROM UNNEST(SPLIT(SPLIT(\`events.page_path_and_query\`, '?')[SAFE_OFFSET(1)], '&')) AS key_value_pair
          ),
          ''
        )
      END
    `

    expect(canonicalizeSQL(result)).toBe(canonicalizeSQL(expectedSQL));
  });
});