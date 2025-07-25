const generateEntityVersion = require('../includes/entity_version');

jest.mock('../includes/data_functions', () => ({
  eventDataExtract: jest.fn((source, key, isRequired, castAs) => `EXTRACTED(${source}, ${key}, ${castAs || 'string'})`),
  setKeyConstraints: jest.fn((ctx, dataform, options) => `-- setKeyConstraints with ${options.primaryKey}`)
}));

describe('entity_version', () => {
  const publishMock = jest.fn(() => ({
    query: jest.fn().mockReturnThis(),
    preOps: jest.fn().mockReturnThis(),
    postOps: jest.fn()
  }));

  global.publish = publishMock;

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('calls publish with correct table name and config', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true },
      bqDatasetName: 'MyDataset',
      bqProjectName: 'MyProject',
      bqEventsTableName: 'events_table',
      expirationDays: 180,
      dataSchema: [
        { entityTableName: 'users', primaryKey: 'id', hidePrimaryKey: false },
        { entityTableName: 'records', primaryKey: 'record_id', hidePrimaryKey: true },
      ],
      hiddenPolicyTagLocation: 'projects/my-project/locations/eu/taxonomies/1234/policyTags/abcd'
    };

    generateEntityVersion(params);

    expect(publishMock).toHaveBeenCalledWith(
      'TestService_entity_version',
      expect.objectContaining({
        type: 'incremental',
        protected: false,
        uniqueKey: ['entity_table_name', 'entity_id', 'valid_from'],
        assertions: expect.objectContaining({
          uniqueKey: expect.any(Array),
          nonNull: expect.any(Array),
          rowConditions: expect.arrayContaining([expect.stringContaining('valid_from < valid_to')])
        }),
        bigquery: expect.objectContaining({
          partitionBy: expect.stringContaining('RANGE_BUCKET'),
          updatePartitionFilter: expect.any(String),
          labels: expect.objectContaining({
            eventsource: 'testservice',
            sourcedataset: 'mydataset'
          })
        }),
        tags: ['testservice'],
        description: expect.stringContaining('Each row represents a version of an entity'),
        columns: expect.objectContaining({
          entity_id: expect.objectContaining({
            bigqueryPolicyTags: expect.any(Array)
          }),
          hidden_data: expect.objectContaining({
            columns: expect.objectContaining({
              value: expect.objectContaining({
                bigqueryPolicyTags: expect.any(Array)
              })
            })
          })
        })
      })
    );
  });
});
