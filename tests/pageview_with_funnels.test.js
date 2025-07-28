const funnelModule = require('../includes/pageview_with_funnels');

describe('pageview_with_funnels', () => {
  let mockPublish;

  beforeEach(() => {
    mockPublish = jest.fn(() => ({
      query: jest.fn().mockImplementation(fn => {
        fn({
          ref: () => '`ref_table`',
          when: () => '',
          self: () => '`self_table`'
        });
        return { preOps: jest.fn().mockReturnThis(), postOps: jest.fn().mockReturnThis() };
      }),
      preOps: jest.fn().mockReturnThis(),
      postOps: jest.fn().mockReturnThis()
    }));

    global.publish = mockPublish;
  });

  it('should throw an error if funnelDepth is not a positive integer', () => {
    expect(() => {
      funnelModule({
        funnelDepth: -1,
        urlRegex: 'example.com',
        enableSessionTables: true,
        attributionParameters: [],
        requestPathGroupingRegex: '.*',
        eventSourceName: 'TestSource',
        bqDatasetName: 'test_dataset',
        bqProjectName: 'test_project',
        defaultConfig: {}
      });
    }).toThrow(/is not valid/);
  });

  it('should throw an error if urlRegex is missing', () => {
    expect(() => {
      funnelModule({
        funnelDepth: 3,
        enableSessionTables: true,
        attributionParameters: [],
        requestPathGroupingRegex: '.*',
        eventSourceName: 'TestSource',
        bqDatasetName: 'test_dataset',
        bqProjectName: 'test_project',
        defaultConfig: {}
      });
    }).toThrow(/urlRegex is missing or empty/);
  });

  it('should call publish with correct name and configuration', () => {
    funnelModule({
      funnelDepth: 2,
      urlRegex: 'example.com',
      enableSessionTables: true,
      attributionParameters: ['utm_medium', 'gclid'],
      requestPathGroupingRegex: '.*',
      eventSourceName: 'TestSource',
      bqDatasetName: 'test_dataset',
      bqProjectName: 'test_project',
      defaultConfig: { enabled: true }
    });

    expect(mockPublish).toHaveBeenCalledWith(
      'pageview_with_funnels_TestSource',
      expect.objectContaining({
        type: 'incremental',
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(occurred_at)',
          clusterBy: ['newly_arrived'],
          labels: expect.objectContaining({
            eventsource: 'testsource',
            sourcedataset: 'test_dataset'
          })
        }),
        tags: ['testsource']
      })
    );
  });

  it('should return true when enableSessionTables is false', () => {
    const result = funnelModule({
      funnelDepth: 2,
      urlRegex: 'example.com',
      enableSessionTables: false,
      attributionParameters: [],
      requestPathGroupingRegex: '.*',
      eventSourceName: 'TestSource',
      bqDatasetName: 'test_dataset',
      bqProjectName: 'test_project',
      defaultConfig: {}
    });

    expect(result).toBe(true);
  });
});
