const sessions = require('../includes/sessions');

describe('sessions module', () => {
  let mockPublish, mockQuery;

  beforeEach(() => {
    mockQuery = jest.fn().mockReturnValue('QUERY_PLACEHOLDER');
    mockPublish = jest.fn(() => ({
      query: mockQuery
    }));

    global.publish = mockPublish;
  });

  it('returns true when enableSessionTables is false', () => {
    const result = sessions({
      enableSessionTables: false,
      eventSourceName: 'TestSource'
    });

    expect(result).toBe(true);
    expect(mockPublish).not.toHaveBeenCalled();
  });

  it('calls publish with correct table name and configuration', () => {
    const params = {
      enableSessionTables: true,
      eventSourceName: 'TestSource',
      bqDatasetName: 'TestDataset',
      attributionParameters: ['utm_source', 'utm_medium'],
      defaultConfig: { foo: 'bar' }
    };

    const result = sessions(params);

    expect(mockPublish).toHaveBeenCalledWith('sessions_TestSource', expect.objectContaining({
      type: 'table',
      bigquery: expect.objectContaining({
        partitionBy: expect.stringContaining('next_session_started_at'),
        clusterBy: expect.arrayContaining(['user_ids', 'anonymised_user_agent_and_ip']),
        labels: expect.objectContaining({
          eventsource: 'testsource',
          sourcedataset: 'testdataset'
        })
      }),
      tags: ['testsource'],
      description: expect.stringContaining('User sessions from TestSource'),
      columns: expect.objectContaining({
        session_started_at: expect.any(String),
        next_session_started_at: expect.any(String),
        utm_source: expect.any(String),
        utm_medium: expect.any(String)
      })
    }));
  });

  it('generates query containing attribution parameters and session logic', () => {
    const params = {
      enableSessionTables: true,
      eventSourceName: 'TestSource',
      bqDatasetName: 'TestDataset',
      attributionParameters: ['utm_source', 'utm_medium'],
      defaultConfig: {}
    };

    let capturedQueryFn;

    global.publish = jest.fn(() => ({
      query: fn => {
        capturedQueryFn = fn;
        return 'mockReturn';
      }
    }));

    sessions(params);

    const ctx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(ctx);

    expect(sql).toContain('WITH');
    expect(sql).toContain('session_with_user_ids');
    expect(sql).toContain('utm_source');
    expect(sql).toContain('utm_medium');
    expect(sql).toContain('pageview_with_funnels_TestSource');
  });
});
