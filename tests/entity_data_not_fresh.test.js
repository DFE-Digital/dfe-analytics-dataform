const { canonicalizeSQL } = require('./helpers/sql'); // adjust path if needed

const mockQuery = jest.fn();
const mockTags = jest.fn(() => ({ query: mockQuery }));
const mockAssert = jest.fn(() => ({ tags: mockTags }));

global.assert = mockAssert;

const generateAssertions = require('../includes/entity_data_not_fresh');

describe('entities_data_not_fresh', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('generates assertions for valid entities', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { severity: 'low' },
      dataSchema: [
        { entityTableName: 'schools', dataFreshnessDays: 5 },
        { entityTableName: 'pupils', dataFreshnessDays: 2 }
      ]
    };

    generateAssertions(params);

    expect(mockAssert).toHaveBeenCalledTimes(2);

    expect(mockAssert).toHaveBeenCalledWith(
      'schools_data_not_fresh_TestService',
      expect.objectContaining({ severity: 'low' })
    );

    expect(mockTags).toHaveBeenCalledWith(['testservice']);

    const queryFn = mockQuery.mock.calls[0][0];
    const mockCtx = { ref: (name) => `\`${name}\`` };

    const rawSQL = queryFn(mockCtx);
    const sql = canonicalizeSQL(rawSQL);

    expect(sql).toContain('from `schools_latest_testservice`');
    expect(sql).toContain('event_last_streamed_at < timestamp_sub(current_timestamp, interval 5 day)');
  });

  it('skips assertion when dataFreshnessDisableDuringRange is true and disableAssertionsNow is set', () => {
    const params = {
      eventSourceName: 'TestService',
      disableAssertionsNow: true,
      dataSchema: [
        {
          entityTableName: 'pupils',
          dataFreshnessDays: 3,
          dataFreshnessDisableDuringRange: true
        }
      ]
    };

    generateAssertions(params);

    expect(mockAssert).not.toHaveBeenCalled();
  });

  it('throws an error for non-integer or invalid dataFreshnessDays', () => {
    const params = {
      eventSourceName: 'BadService',
      dataSchema: [
        { entityTableName: 'teachers', dataFreshnessDays: 'soon' },
        { entityTableName: 'admins', dataFreshnessDays: 0 },
        { entityTableName: 'staff', dataFreshnessDays: -2 }
      ]
    };

    expect(() => generateAssertions(params)).toThrow(
      'dataFreshnessDays parameter for the teachers entityTableName is not a positive integer.'
    );
  });

  it('ignores entities without dataFreshnessDays set', () => {
    const params = {
      eventSourceName: 'TestService',
      dataSchema: [
        { entityTableName: 'missing_dates' }
      ]
    };

    generateAssertions(params);

    expect(mockAssert).not.toHaveBeenCalled();
  });
});