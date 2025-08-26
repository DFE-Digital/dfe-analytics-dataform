const sessionDetails = require('../includes/session_details');

describe('session_details module', () => {
  let mockQueryImpl;
  let mockPreOpsImpl;

  beforeEach(() => {
    jest.resetModules();

    mockQueryImpl = jest.fn();
    mockPreOpsImpl = jest.fn();

    global.publish = jest.fn(() => ({
      query: (fn) => {
        mockQueryImpl = fn;
        return {
          preOps: (preFn) => {
            mockPreOpsImpl = preFn;
            return {};
          }
        };
      }
    }));
  });

  it('calls publish with correct table name and config when enabled', () => {
    const params = {
      enableSessionDetailsTable: true,
      eventSourceName: 'TestService',
      bqDatasetName: 'test_dataset',
      defaultConfig: {},
      dependencies: []
    };

    const result = require('../includes/session_details')(params);

    expect(global.publish).toHaveBeenCalledWith(
      'session_details_TestService',
      expect.objectContaining({
        type: 'incremental',
        tags: ['testservice'],
        assertions: {
          uniqueKey: [['session_id']]
        }
      })
    );
  });

  it('evaluates query(ctx) and preOps(ctx)', () => {
    const params = {
      enableSessionDetailsTable: true,
      eventSourceName: 'TestService',
      bqDatasetName: 'test_dataset',
      defaultConfig: {},
      dependencies: []
    };

    require('../includes/session_details')(params);

    const ctx = {
      ref: (x) => `\`${x}\``,
      incremental: () => true,
      self: () => '`session_details_TestService`'
    };

    const sql = mockQueryImpl(ctx);
    const preSql = mockPreOpsImpl(ctx);

    expect(sql).toMatch(/WITH\s+events\s+AS/i);
    expect(sql).toMatch(/SELECT\s+session_id/i);
    expect(preSql).toMatch(/DECLARE event_timestamp_checkpoint TIMESTAMP DEFAULT/i);
  });
});
