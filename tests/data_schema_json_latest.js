const dataSchemaLatest = require('../includes/data_schema_json_latest');
const { canonicalizeSQL } = require('./helpers/sql');

describe('data_schema_latest', () => {
  let mockPublish;
  let capturedQueryFn;

  beforeEach(() => {
    capturedQueryFn = null;

    mockPublish = jest.fn(() => ({
      query: fn => {
        capturedQueryFn = fn;
        return 'mockedQueryStep';
      },
      tags: jest.fn().mockReturnThis()
    }));

    global.publish = mockPublish;

    global.data_functions = {
      stringToTimestamp: jest.fn(() => 'PARSE_TIMESTAMP(this_value)'),
      stringToDate: jest.fn(() => 'PARSE_DATE(this_value)')
    };
  });

  it('should call publish with correct table name and config', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'analytics',
      defaultConfig: {}
    };

    const result = dataSchemaLatest(params);

    expect(mockPublish).toHaveBeenCalledWith('MyService_data_schema_latest', expect.objectContaining({
      type: 'table',
      description: expect.stringContaining('Generates a blank version of the dataSchema JSON'),
      columns: expect.objectContaining({
        dataSchemaJSON: expect.stringContaining('dataSchema JSON')
      }),
      bigquery: {
        labels: {
          eventsource: 'myservice',
          sourcedataset: 'analytics'
        }
      },
      tags: ['myservice']
    }));

    expect(result).toBe('mockedQueryStep');
  });

  it('should generate SQL including correct fields and logic', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'analytics',
      defaultConfig: {}
    };

    dataSchemaLatest(params);

    const ctx = {
      ref: name => `mocked.${name}`
    };

    const rawSQL = capturedQueryFn(ctx);
    const sql = canonicalizeSQL(rawSQL);

    expect(sql).toContain('with keys_with_data_type as');
    expect(sql).toContain('from mocked.events_myservice');
    expect(sql).toContain('entity_table_name');
    expect(sql).toContain('data_type');
    expect(sql).toContain('dataschemajson');
    expect(sql).toContain('logical_and');
    expect(sql).toContain('string_agg');
  });

  it('should embed stringToTimestamp and stringToDate SQL logic', () => {
    global.data_functions = {
      stringToTimestamp: jest.fn(() => 'PARSE_TIMESTAMP(this_value)'),
      stringToDate: jest.fn(() => 'PARSE_DATE(this_value)')
    };

    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'analytics',
      defaultConfig: {}
    };

    dataSchemaLatest(params);

    const ctx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(ctx);

    expect(sql).toContain('PARSE_TIMESTAMP(this_value)');
    expect(sql).toContain('PARSE_DATE(this_value)');
  });

});
