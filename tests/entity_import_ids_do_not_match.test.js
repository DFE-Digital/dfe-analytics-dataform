const generateAssertion = require('../includes/entity_import_ids_do_not_match');
const { canonicalizeSQL } = require('./helpers/sql');

describe('entity_import_ids_do_not_match', () => {
  const queryMock = jest.fn();
  const tagsMock = jest.fn(() => ({ query: queryMock }));
  const mockAssert = jest.fn(() => ({ tags: tagsMock }));

  beforeAll(() => {
    global.assert = mockAssert;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('calls assert with the correct name and config when compareChecksums is true', () => {
    const params = {
      compareChecksums: true,
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true }
    };

    generateAssertion(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'TestService_entity_import_ids_do_not_match',
      expect.objectContaining({ someConfig: true })
    );

    const tagsFn = mockAssert.mock.results[0].value.tags;
    const queryFn = tagsFn().query;
    expect(typeof queryFn).toBe('function');

    const sql = queryMock.mock.calls[0][0]({ ref: (name) => `\`${name}\`` });
    expect(typeof sql).toBe('string');
    const canonical = canonicalizeSQL(sql);

    expect(canonical).toContain('from `entity_table_check_import_testservice`');
    expect(canonical).toContain('as issue_description');
    expect(canonical).toContain('date(checksum_calculated_at)>= current_date - 1');
  });

  it('does nothing if compareChecksums is false', () => {
    const params = {
      compareChecksums: false,
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true }
    };

    const result = generateAssertion(params);
    expect(result).toBeUndefined();
    expect(mockAssert).not.toHaveBeenCalled();
  });
});
