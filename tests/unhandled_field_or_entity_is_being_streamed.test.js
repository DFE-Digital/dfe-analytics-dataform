const unhandledFieldOrEntity = require('../includes/unhandled_field_or_entity_is_being_streamed');

describe('unhandledFieldOrEntityIsBeingStreamed', () => {
  let capturedQueryFn;
  let mockAssert;

  beforeEach(() => {
    capturedQueryFn = null;

    mockAssert = jest.fn(() => ({
      tags: jest.fn().mockReturnThis(),
      query: fn => {
        capturedQueryFn = fn;
        return 'mockedQueryStep';
      }
    }));

    global.assert = mockAssert;
  });

  it('should call assert with correct name and config', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { someConfig: true },
      dataSchema: [
        {
          entityTableName: 'users',
          primaryKey: 'user_id',
          keys: [
            { keyName: 'email', historic: false },
            { keyName: 'phone', historic: true },
            { keyName: 'name', historic: false }
          ]
        }
      ]
    };

    const result = unhandledFieldOrEntity(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'TestService_unhandled_field_or_entity_is_being_streamed',
      expect.objectContaining({
        type: 'assertion',
        description: expect.stringContaining('Identifies any entities or field names'),
        someConfig: true
      })
    );

    expect(result).toBe('mockedQueryStep');
  });

  it('should generate SQL including filtered keys and primary key', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: {},
      dataSchema: [
        {
          entityTableName: 'accounts',
          keys: [
            { keyName: 'email', historic: false },
            { keyName: 'status', historic: false },
            { keyName: 'legacy_code', historic: true }
          ],
          primaryKey: 'account_id'
        },
        {
          entityTableName: 'profiles',
          keys: [],
          primaryKey: 'id'
        }
      ]
    };

    unhandledFieldOrEntity(params);

    const mockCtx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(mockCtx);

    expect(sql).toContain('STRUCT("accounts" AS entity_name');
    expect(sql).toContain('"email"');
    expect(sql).toContain('"status"');
    expect(sql).not.toContain('legacy_code');
    expect(sql).toContain('"account_id"');

    expect(sql).toContain('STRUCT("profiles" AS entity_name');
    expect(sql).toContain('"id"');

    expect(sql).toContain('UNNEST(ARRAY_CONCAT(data, hidden_data))');
    expect(sql).toContain('event_type IN ("create_entity", "update_entity", "import_entity")');
    expect(sql).toContain('key NOT IN UNNEST(expected_entity_fields.keys)');
  });

  it('should still work when keys are empty', () => {
    const params = {
      eventSourceName: 'EmptyCase',
      defaultConfig: {},
      dataSchema: []
    };

    unhandledFieldOrEntity(params);

    const mockCtx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(mockCtx);

    expect(sql).toContain('UNNEST([');
    expect(sql).toContain('event_type IN ("create_entity", "update_entity", "import_entity")');
  });
});
