const generateFreshnessCheck = require('../includes/events_data_not_fresh');

describe('events_data_is_not_fresh', () => {
  const tagsMock = jest.fn().mockReturnThis();
  const queryMock = jest.fn().mockReturnThis();

  const assertMock = jest.fn(() => ({
    tags: tagsMock,
    query: queryMock,
  }));

  global.assert = assertMock;

  const ctxMock = { ref: jest.fn() };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const baseParams = {
    eventSourceName: 'TestService',
    defaultConfig: { some: 'config' },
    bqProjectName: 'my-project',
    bqDatasetName: 'my_dataset',
    bqEventsTableName: 'events_table',
    eventsDataFreshnessDays: 3
  };

  it('should call assert with correct name and query when config is valid', () => {
    const result = generateFreshnessCheck(baseParams);

    expect(assertMock).toHaveBeenCalledWith(
      'TestService_events_data_is_not_fresh',
      expect.objectContaining({
        some: 'config'
      })
    );

    expect(tagsMock).toHaveBeenCalledWith(['testservice']);

    const queryFn = queryMock.mock.calls[0][0]; // the function passed to query()
    const sql = queryFn(ctxMock);
    expect(sql).toContain('HAVING event_last_streamed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 3 DAY)');
  });

  it('should throw if freshness days is invalid', () => {
    expect(() => generateFreshnessCheck({ ...baseParams, eventsDataFreshnessDays: 'abc' }))
      .toThrow('eventsDataFreshnessDays parameter is not a positive integer');
  });

  it('should skip if disableAssertionsNow and freshnessDisableDuringRange are true', () => {
    const result = generateFreshnessCheck({
      ...baseParams,
      disableAssertionsNow: true,
      eventsDataFreshnessDisableDuringRange: true,
    });

    expect(result).toBeUndefined();
    expect(assertMock).not.toHaveBeenCalled();
  });
});
