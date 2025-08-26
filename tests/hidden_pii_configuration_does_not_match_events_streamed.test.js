const generateAssertions = require('../includes/hidden_pii_configuration_does_not_match_events_streamed'); // adjust path as needed

describe('assert_hidden_keys', () => {
  let mockAssert;

  beforeEach(() => {
    mockAssert = jest.fn(() => ({
      tags: jest.fn().mockReturnThis(),
      query: jest.fn().mockReturnThis()
    }));
    global.assert = mockAssert;
  });

  it('generates correct assertions for both entity and custom events', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { enabled: true },
      transformEntityEvents: true,
      dataSchema: [
        {
          entityTableName: 'user',
          description: 'user data',
          primaryKey: 'id',
          hidePrimaryKey: true,
          keys: [
            {
              keyName: 'email',
              hidden: true
            },
            {
              keyName: 'nickname',
              hidden: false
            }
          ]
        }
      ],
      customEventSchema: [
        {
          eventType: 'signup_started',
          description: 'User started signup flow',
          keys: [
            {
              keyName: 'utm_campaign',
              hidden: true
            }
          ]
        }
      ]
    };

    generateAssertions(params);

    // Expect 4 assertions total
    expect(mockAssert).toHaveBeenCalledTimes(4);

    // Check one of them
    expect(mockAssert).toHaveBeenCalledWith(
      'TestService_hidden_pii_configuration_does_not_match_entity_events_streamed_yesterday',
      expect.objectContaining({
        type: 'assertion',
        description: expect.stringContaining('Counts the number of entities updated yesterday'),
        enabled: true
      })
    );

    // Check SQL generation
    const call = mockAssert.mock.results[0].value;
    const queryCalls = call.query.mock.calls;
    expect(queryCalls).toHaveLength(1);
    const sql = queryCalls[0][0]({ ref: t => `\`${t}\`` }); // fake ctx

    expect(sql).toContain('WITH expected_fields AS');
    expect(sql).toContain('STRUCT("user" AS entity_name');
    expect(sql).toContain('updates_made_with_this_key_not_hidden');
  });

  it('skips entity assertions if transformEntityEvents is false', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: { enabled: true },
      transformEntityEvents: false,
      dataSchema: [],
      customEventSchema: [
        {
          eventType: 'event_x',
          description: 'desc',
          keys: [{ keyName: 'key1', hidden: true }]
        }
      ]
    };

    generateAssertions(params);
    expect(mockAssert).toHaveBeenCalledTimes(2);
    expect(mockAssert.mock.calls[0][0]).toMatch(/custom_events_streamed_yesterday/);
    expect(mockAssert.mock.calls[1][0]).toMatch(/historic_custom_events_streamed/);
  });

  it('skips all assertions if no custom or entity keys exist', () => {
    const params = {
      eventSourceName: 'TestService',
      defaultConfig: {},
      transformEntityEvents: false,
      dataSchema: [],
      customEventSchema: []
    };

    generateAssertions(params);
    expect(mockAssert).not.toHaveBeenCalled();
  });
});
