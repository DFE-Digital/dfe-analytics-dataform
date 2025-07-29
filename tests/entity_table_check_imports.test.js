const generateImportEntityCheck = require('../includes/entity_table_check_import');
const { canonicalizeSQL } = require('./helpers/sql');

jest.mock('../includes/data_functions', () => ({
  eventDataExtract: jest.fn((source, key, isRequired, castAs) => {
    return `MOCKED_EXTRACT(${source}, ${key}, ${castAs || 'string'})`;
  })
}));

const queryMock = jest.fn(() => ({ preOps: preOpsMock }));
const preOpsMock = jest.fn(() => ({ postOps: postOpsMock }));
const postOpsMock = jest.fn();
const tagsMock = jest.fn(() => ({ query: queryMock }));
const publishMock = jest.fn(() => ({ tags: tagsMock }));

beforeAll(() => {
  global.publish = publishMock;
});

describe('entity_table_check_import', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('calls publish with correct config and generates expected SQL/pre/post operations', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true },
      bqDatasetName: 'MyDataset',
      bqProjectName: 'MyProject',
      bqEventsTableName: 'events_table',
      expirationDays: 7
    };

    generateImportEntityCheck(params);

    expect(global.publish).toHaveBeenCalledWith(
      'entity_table_check_import_TestService',
      expect.objectContaining({
        type: 'incremental',
        tags: ['testservice'],
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(checksum_calculated_at)',
          labels: expect.objectContaining({ eventsource: 'testservice' })
        })
      })
    );

    const ctx = {
      ref: (name) => `\`${name}\``,
      when: (condition, ifTrue, ifFalse) => (condition ? ifTrue : ifFalse),
      incremental: () => true,
      self: () => '`MyProject.MyDataset.entity_table_check_import_TestService`'
    };

    const preSQL = preOpsMock.mock.calls[0][0](ctx);
    expect(preSQL).toContain('DECLARE event_timestamp_checkpoint');

    const sql = queryMock.mock.calls[0][0](ctx);
    expect(typeof sql).toBe('string');
    const canonical = canonicalizeSQL(sql);
    expect(canonical).toContain('from `myproject.mydataset.events_table`');
    expect(canonical).toContain('mocked_extract');

    const postSQL = postOpsMock.mock.calls[0][0](ctx);
    expect(postSQL).toContain('ALTER TABLE');
    expect(postSQL).toContain('partition_expiration_days = 7');
  });
});
