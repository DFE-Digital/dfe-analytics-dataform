jest.mock('../includes/data_functions', () => ({
  eventDataExtract: jest.fn(() => 'mocked_event_data_extract_expression')
}));

const dfeAnalyticsConfiguration = require('../includes/dfe_analytics_configuration');

describe('dfeAnalyticsConfiguration', () => {
  it('calls publish with correct table name and config, and generates SQL', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'AnalyticsDataset',
      bqProjectName: 'MyProject',
      defaultConfig: { someSetting: true },
      dependencies: ['table_a']
    };

    const mockQuery = jest.fn();
    const mockPublish = jest.fn(() => ({ query: mockQuery }));

    global.publish = mockPublish;

    dfeAnalyticsConfiguration(params);

    expect(mockPublish).toHaveBeenCalledWith(
      'dfe_analytics_configuration_MyService',
      expect.objectContaining({
        description: expect.stringContaining('MyService'),
        tags: ['myservice'],
        dependencies: ['table_a']
      })
    );
  });
});
