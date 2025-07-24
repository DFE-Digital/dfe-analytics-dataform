const { canonicalizeSQL } = require('./helpers/sql');

const mockQueries = jest.fn();
const mockTags = jest.fn(() => ({ queries: mockQueries }));
const mockOperate = jest.fn(() => ({ tags: mockTags }));

global.operate = mockOperate;

const generateTableFunctions = require('../includes/entity_table_function');

describe('entity_table_function', () => {
  it('generates a table function for each entity in dataSchema', () => {
    const params = {
      eventSourceName: 'MyService',
      dataSchema: [
        { entityTableName: 'schools' },
        { entityTableName: 'pupils' }
      ]
    };

    generateTableFunctions(params);

    expect(mockOperate).toHaveBeenCalledTimes(2);
    expect(mockOperate).toHaveBeenCalledWith('schools_at_MyService');
    expect(mockOperate).toHaveBeenCalledWith('pupils_at_MyService');

    expect(mockTags).toHaveBeenCalledTimes(2);
    expect(mockTags).toHaveBeenCalledWith(['myservice']);

    expect(mockQueries).toHaveBeenCalledTimes(2);

    const queryFn = mockQueries.mock.calls[0][0];
    const mockCtx = {
      ref: name => `\`${name}\``,
      database: () => 'my_db',
      schema: () => 'my_schema'
    };

    const rawSQL = queryFn(mockCtx);
    const sql = canonicalizeSQL(rawSQL);

    expect(sql).toContain('create or replace table function');
    expect(sql).toContain('my_db.my_schema.schools_at_myservice');
    expect(sql).toContain('from `schools_version_myservice`');
    expect(sql).toContain('where (valid_to is null or valid_to > timestamp_at) and valid_from <= timestamp_at');
  });
});
