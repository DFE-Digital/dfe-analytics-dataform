const generateScheduledEntityCheck = require('../includes/entity_table_check_scheduled');
const { canonicalizeSQL } = require('./helpers/sql');

jest.mock('../includes/data_functions', () => ({
  eventDataExtract: jest.fn((source, key, isRequired, castAs) => `EXTRACT(${source}, ${key}, ${castAs || 'string'})`)
}));

describe('entity_table_check_scheduled', () => {
  const queryMock = jest.fn(() => ({ postOps: postOpsMock }));
  const postOpsMock = jest.fn();
  const tagsMock = jest.fn(() => ({ query: queryMock }));
  const publishMock = jest.fn(() => ({ tags: tagsMock }));

  beforeAll(() => {
    global.publish = publishMock;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('calls publish with correct config and generates expected SQL and postOps', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true },
      bqDatasetName: 'AnalyticsDataset',
      bqProjectName: 'MyProject',
      bqEventsTableName: 'events_table',
      expirationDays: 14
    };

    generateScheduledEntityCheck(params);

    expect(publishMock).toHaveBeenCalledWith(
      'entity_table_check_scheduled_TestService',
      expect.objectContaining({
        type: 'table',
        assertions: expect.any(Object),
        dependencies: expect.arrayContaining([
          'TestService_entities_are_missing_expected_fields'
        ]),
        bigquery: expect.objectContaining({
          partitionBy: 'checksum_calculated_on',
          labels: expect.objectContaining({ eventsource: 'testservice' })
        }),
        tags: ['testservice']
      })
    );

    const ctx = {
      ref: (name) => `\`${name}\``,
      self: () => '`MyProject.AnalyticsDataset.entity_table_check_scheduled_TestService`'
    };

    const sql = queryMock.mock.calls[0][0](ctx);
    expect(typeof sql).toBe('string');
    const canonical = canonicalizeSQL(sql);
    expect(canonical).toContain('from `testservice_entity_version`');
    expect(canonical).toContain('select');
    expect(canonical).toContain('checksum_calculated_on');

    const postSQL = postOpsMock.mock.calls[0][0](ctx);
    expect(postSQL).toContain('ALTER TABLE');
    expect(postSQL).toContain('partition_expiration_days = 14');
  });
});
