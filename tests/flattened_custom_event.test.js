const flattenedCustomEvents = require('../includes/flattened_custom_event');

// Mocks
jest.mock('../includes/data_functions', () => ({
  stringToTimestamp: jest.fn(str => `CAST(${str} AS TIMESTAMP)`),
  stringToDate: jest.fn(str => `CAST(${str} AS DATE)`),
  stringToIntegerArray: jest.fn(str => `CAST(${str} AS ARRAY<INT64>)`),
}));

describe('flattened_custom_events', () => {
  const publishMock = jest.fn(() => ({
    query: jest.fn(() => ({
      preOps: jest.fn(() => ({
        postOps: jest.fn()
      }))
    }))
  }));

  global.publish = publishMock;

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should publish a custom event with expected config and query logic', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someSetting: true },
      bqDatasetName: 'test_dataset',
      bqProjectName: 'test_project',
      expirationDays: 30,
      customEventSchema: [
        {
          eventType: 'custom_event_type',
          description: 'Test event',
          expirationDays: 10,
          keys: [
            {
              keyName: 'field1',
              description: 'A test field',
              dataType: 'string'
            },
            {
              keyName: 'field2',
              description: 'A hidden field',
              dataType: 'integer',
              hidden: true,
              hiddenPolicyTagLocation: 'projects/my-project/policyTag123'
            }
          ]
        }
      ]
    };

    flattenedCustomEvents(params);

    expect(publishMock).toHaveBeenCalledWith(
      'custom_event_type_TestService',
      expect.objectContaining({
        type: 'incremental',
        description: expect.stringContaining('custom_event_type'),
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(occurred_at)',
          labels: expect.objectContaining({
            eventsource: 'testservice',
            sourcedataset: 'test_dataset',
            entitytabletype: 'custom_event'
          })
        }),
        assertions: expect.objectContaining({
          nonNull: ['occurred_at']
        }),
        tags: ['testservice'],
        columns: expect.objectContaining({
          occurred_at: expect.any(String),
          field1: expect.objectContaining({
            description: 'A test field',
            bigqueryPolicyTags: []
          }),
          field2: expect.objectContaining({
            description: 'A hidden field',
            bigqueryPolicyTags: ['projects/my-project/policyTag123']
          })
        })
      })
    );
  });
});
