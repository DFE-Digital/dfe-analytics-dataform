const { canonicalizeSQL } = require('./helpers/sql');

const mockQuery = jest.fn();
const mockTags = jest.fn(() => ({ query: mockQuery }));
const mockAssert = jest.fn(() => ({ tags: mockTags }));

global.assert = mockAssert;

const generateAssertion = require('../includes/entities_have_not_been_backfilled');

describe('entities_have_not_been_backfilled', () => {
  it('generates correct assertion config and SQL', () => {
    const params = {
      eventSourceName: 'SchoolMIS',
      defaultConfig: { type: 'assertion', severity: 'high' },
      expirationDays: 90,
      dataSchema: [
        {
          entityTableName: 'schools',
          expirationDays: 30
        },
        {
          entityTableName: 'pupils'
        }
      ]
    };

    generateAssertion(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'SchoolMIS_entities_have_not_been_backfilled',
      expect.objectContaining({
        type: 'assertion',
        description: expect.stringContaining('have no import_entity'),
        severity: 'high'
      })
    );

    expect(mockTags).toHaveBeenCalledWith(['schoolmis']);

    const queryFn = mockQuery.mock.calls[0][0];

    const mockCtx = {
      ref: name => `\`${name}\``
    };

    const rawSQL = queryFn(mockCtx);
    const sql = canonicalizeSQL(rawSQL);

    expect(sql).toContain('from `events_schoolmis`');
    expect(sql).toContain('entity_table_name');
    expect(sql).toContain('import_entity');
    expect(sql).toContain('import_entity_table_check');
    expect(sql).toContain('left join `entity_table_check_scheduled_schoolmis`');
    expect(sql).toContain('number_of_import_events = 0');

    expect(sql).toContain('struct("schools" as entity_table_name');
    expect(sql).toContain('safe_cast("30" as int64)');
    expect(sql).toContain('struct("pupils" as entity_table_name');
    expect(sql).toContain('safe_cast("90" as int64)');
  });
});