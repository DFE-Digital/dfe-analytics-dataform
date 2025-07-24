jest.mock('../includes/data_functions', () => ({
  keyIsInEventData: jest.fn((dataExpr, keyExpr, safe) =>
    `MOCKED_KEY_CHECK(${dataExpr}, ${keyExpr}, ${safe})`
  )
}));

const mockQuery = jest.fn();
const mockTags = jest.fn(() => ({ query: mockQuery }));
const mockAssert = jest.fn(() => ({ tags: mockTags }));
global.assert = mockAssert;

const generateAssertion = require('../includes/entities_are_missing_expected_fields');
const { canonicalizeSQL } = require('./helpers/sql');

describe('entities_are_missing_expected_fields', () => {
  it('should generate the correct assertion configuration and SQL query', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someSetting: true },
      dataSchema: [
        {
          entityTableName: 'schools',
          primaryKey: 'urn',
          keys: [
            { keyName: 'name', historic: false },
            { keyName: 'phase', historic: true },
            { keyName: 'postcode', historic: false }
          ]
        }
      ]
    };

    generateAssertion(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'TestService_entities_are_missing_expected_fields',
      expect.objectContaining({
        type: 'assertion',
        description: expect.any(String),
        someSetting: true
      })
    );

    expect(mockTags).toHaveBeenCalledWith(['testservice']);

    const queryFn = mockQuery.mock.calls[0][0];
    const mockCtx = { ref: name => `\`${name}\`` };
    const sql = queryFn(mockCtx);

    expect(canonicalizeSQL(sql)).toContain('from `events_testservice`');
    expect(sql).toContain('STRUCT("schools" AS entity_name');
    expect(sql).toContain('"name"');
    expect(sql).toContain('"postcode"');
    expect(sql).toContain('MOCKED_KEY_CHECK(ARRAY_CONCAT(data, hidden_data), expected_key, true)');
    expect(sql).toContain('HAVING\n  updates_made_yesterday_without_this_key > 0');
  });
});
