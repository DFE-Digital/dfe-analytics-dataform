jest.mock('../includes/data_functions', () => ({
  stringToTimestamp: jest.fn(() => 'mocked_timestamp_expr'),
  stringToDate: jest.fn(() => 'mocked_date_expr'),
}));

const dataSchemaJsonLatest = require('../includes/data_schema_json_latest');

describe('dataSchemaJsonLatest', () => {
  it('calls publish', () => {
    const params = {
      eventSourceName: 'MyEventSource',
      bqDatasetName: 'MyBQDataset',
      defaultConfig: { someSetting: true },
    };

    const mockQuery = jest.fn();
    const mockPublish = jest.fn(() => ({ query: mockQuery }));
    global.publish = mockPublish;

    dataSchemaJsonLatest(params);

    expect(mockPublish).toHaveBeenCalledWith(
      'MyEventSource_data_schema_latest',
      expect.objectContaining({
        type: 'table',
        bigquery: expect.any(Object),
        tags: ['myeventsource'],
      })
    );
  });
});
