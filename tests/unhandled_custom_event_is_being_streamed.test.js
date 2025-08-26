const unhandledCustomEvent = require('../includes/unhandled_custom_event_is_being_streamed');

describe('unhandledCustomEventIsBeingStreamed', () => {
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

  it('should call assert with the correct name and config', () => {
    const params = {
      eventSourceName: 'TestSource',
      defaultConfig: { assertionConfig: true },
      customEventSchema: [{ eventType: 'custom_event_1' }, { eventType: 'custom_event_2' }]
    };

    const result = unhandledCustomEvent(params);

    expect(mockAssert).toHaveBeenCalledWith(
      'TestSource_unhandled_custom_event_is_being_streamed',
      expect.objectContaining({
        type: 'assertion',
        description: expect.stringContaining('Identifies any custom events'),
        assertionConfig: true
      })
    );

    expect(result).toBe('mockedQueryStep');
  });

  it('should generate query that includes expected event types and filters', () => {
    const params = {
      eventSourceName: 'TestSource',
      defaultConfig: {},
      customEventSchema: [{ eventType: 'custom_event_1' }, { eventType: 'custom_event_2' }]
    };

    unhandledCustomEvent(params);

    const mockCtx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(mockCtx);

    expect(sql).toContain('expected_custom_events AS');
    expect(sql).toContain('"custom_event_1"');
    expect(sql).toContain('"custom_event_2"');
    expect(sql).toContain('unexpected_event_type');
    expect(sql).toContain('NOT IN UNNEST');
    expect(sql).toContain('events_TestSource');
    expect(sql).toContain('create_entity');
  });

  it('should generate query with fallback when customEventSchema is empty', () => {
    const params = {
      eventSourceName: 'TestSource',
      defaultConfig: {},
      customEventSchema: []
    };

    unhandledCustomEvent(params);

    const mockCtx = {
      ref: name => `mocked.${name}`
    };

    const sql = capturedQueryFn(mockCtx);

    expect(sql).toContain('CAST(NULL AS STRING)');
    expect(sql).toContain('WHERE eventType IS NOT NULL');
  });
});
