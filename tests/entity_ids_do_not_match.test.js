const { canonicalizeSQL } = require('./helpers/sql');
const generateAssertion = require('../includes/entity_ids_do_not_match');

describe('entity_ids_do_not_match', () => {
  const params = {
    eventSourceName: 'TestService',
    compareChecksums: true,
    defaultConfig: { someSetting: true }
  };

  const mockQuery = jest.fn();
  const mockTags = jest.fn(() => ({ query: mockQuery }));
  const mockAssert = jest.fn(() => ({ tags: mockTags }));

  beforeEach(() => {
    jest.clearAllMocks();
    global.assert = mockAssert;
  });

  it('calls assert with the correct table name and configuration when compareChecksums is true', () => {
    const mockCtx = { ref: name => `\`${name}\`` };

    generateAssertion(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'TestService_entity_ids_do_not_match',
      expect.objectContaining({
        someSetting: true
      })
    );

    const queryFn = mockQuery.mock.calls[0][0];
    const sql = queryFn(mockCtx);
    const canonical = canonicalizeSQL(sql)

    expect(sql).toContain('SELECT');
    expect(canonical).toContain('from `entity_table_check_scheduled_testservice`');
    expect(sql).toContain('AS issue_description')
  });

  it('does nothing if compareChecksums is false', () => {
    const paramsNoCheck = { ...params, compareChecksums: false };
    generateAssertion(paramsNoCheck);
    expect(mockAssert).not.toHaveBeenCalled();
  });
});
