const configurationModule = require('../includes/dfe_analytics_configuration');
const { canonicalizeSQL } = require('./helpers/sql');

describe('configuration_versions module', () => {
  let mockPublish;
  let capturedQueryFn;

  beforeEach(() => {
    capturedQueryFn = null;

    global.data_functions = {
      eventDataExtract: jest.fn(() => 'SAFE_EXTRACT(data, "analytics_version")')
    };

    mockPublish = jest.fn((name, config) => {
      return {
        name,
        ...config,
        query: (fn) => {
          capturedQueryFn = fn;
          return { name, ...config };
        }
      };
    });

    global.publish = mockPublish;
  });

  it('should call publish with correct table name and configuration', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'my_dataset',
      bqProjectName: 'my_project',
      dependencies: ['events_MyService'],
      defaultConfig: {}
    };

    configurationModule(params);

    expect(mockPublish).toHaveBeenCalledWith(
      'dfe_analytics_configuration_MyService',
      expect.objectContaining({
        type: 'table',
        protected: false,
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(valid_to)',
          labels: {
            eventsource: 'myservice',
            sourcedataset: 'my_dataset'
          }
        }),
        tags: ['myservice'],
        description: expect.stringContaining('Configuration versions for dfe-analytics'),
        dependencies: ['events_MyService'],
        columns: expect.objectContaining({
          valid_from: expect.any(String),
          valid_to: expect.any(String),
          version: expect.any(String)
        })
      })
    );
  });

  it('should generate correct SQL using eventDataExtract and expected refs', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'my_dataset',
      bqProjectName: 'my_project',
      dependencies: [],
      defaultConfig: {}
    };

    configurationModule(params);

    const mockCtx = {
      ref: name => `mocked.${name}`
    };

    const rawSQL = capturedQueryFn(mockCtx);
    const sql = canonicalizeSQL(rawSQL);

    expect(sql).toContain('select');
    expect(sql).toContain('occurred_at as valid_from');
    expect(sql).toContain('first_value(occurred_at)');
    expect(sql).toContain('safe_extract(data, "analytics_version")as version');
    expect(sql).toContain('from mocked.events_myservice');
    expect(sql).toContain('event_type = "initialise_analytics"');
    expect(sql).toContain('union all');
  });
});
