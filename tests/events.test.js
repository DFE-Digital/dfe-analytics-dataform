const generateEventsTransformation = require('../includes/events');
const { canonicalizeSQL } = require('./helpers/sql');

describe('events', () => {
  const queryMock = jest.fn().mockReturnThis();
  const preOpsMock = jest.fn().mockReturnThis();
  const postOpsMock = jest.fn().mockReturnThis();
  const publishMock = jest.fn(() => ({
    query: queryMock,
    preOps: preOpsMock,
    postOps: postOpsMock
  }));

  global.publish = publishMock;

  const baseParams = {
    eventSourceName: 'TestService',
    defaultConfig: { some: 'value' },
    bqProjectName: 'my-project',
    bqDatasetName: 'my_dataset',
    bqEventsTableName: 'raw_events',
    bqEventsTableNameSpace: 'some_namespace',
    expirationDays: 30,
    webRequestEventExpirationDays: 15,
    dataSchema: [
      { entityTableName: 'users', expirationDays: 20 },
      { entityTableName: 'accounts' }
    ],
    customEventSchema: [
      { eventType: 'login', expirationDays: 10 }
    ],
    hiddenPolicyTagLocation: 'projects/my-project/locations/eu/taxonomies/1234/policyTags/abcd',
    dependencies: ['dependency_1', 'dependency_2']
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should call publish with the correct table name and config', () => {
    generateEventsTransformation(baseParams);

    expect(publishMock).toHaveBeenCalledWith(
      'events_TestService',
      expect.objectContaining({
        type: 'incremental',
        protected: false,
        tags: ['testservice'],
        dependencies: ['dependency_1', 'dependency_2'],
        description: expect.stringContaining('Initial transformation of the events table'),
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(occurred_at)',
          clusterBy: ['event_type', 'request_uuid'],
          labels: {
            eventsource: 'testservice',
            sourcedataset: 'my_dataset'
          }
        }),
        columns: expect.objectContaining({
          occurred_at: expect.any(String),
          event_type: expect.any(String),
          data: expect.any(Object),
          hidden_data: expect.any(Object),
          entity_table_name: expect.any(String)
        })
      })
    );
  });

  it('should attach a query and call it with ctx', () => {
    generateEventsTransformation(baseParams);
    expect(queryMock).toHaveBeenCalled();

    const queryFn = queryMock.mock.calls[0][0];
    const ctx = {
      ref: (tableName) => `\`${tableName}\``,
      self: () => '`events_TestService`',
      incremental: () => true
    };
    const queryStr = canonicalizeSQL(queryFn(ctx));

    expect(queryStr).toContain('with earliest_web_request_event_for_request as');
  });

  it('should configure preOps with the correct checkpoint logic and retention', () => {
    generateEventsTransformation(baseParams);
    expect(preOpsMock).toHaveBeenCalled();

    const preOpsFn = preOpsMock.mock.calls[0][0];
    const ctx = {
      self: () => '`events_TestService`',
      incremental: () => true
    };
    const preOpsSql = preOpsFn(ctx);
    expect(preOpsSql).toContain('DECLARE event_timestamp_checkpoint DEFAULT');
    expect(preOpsSql).toContain('ALTER TABLE `my-project.my_dataset.raw_events`');
    expect(preOpsSql).toContain('DELETE FROM `my-project.my_dataset.raw_events`');
  });

  it('should configure postOps to set partition expiration', () => {
    generateEventsTransformation(baseParams);
    expect(postOpsMock).toHaveBeenCalled();

    const postOpsFn = postOpsMock.mock.calls[0][0];
    const ctx = { self: () => '`events_TestService`' };
    const postOpsSql = postOpsFn(ctx);

    expect(postOpsSql).toContain('ALTER TABLE `events_TestService`');
    expect(postOpsSql).toContain('SET OPTIONS (partition_expiration_days = 30)');
  });
});
