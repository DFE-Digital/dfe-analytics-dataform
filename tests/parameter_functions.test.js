const { validateParams } = require('../includes/parameter_functions');

describe('validateParams', () => {
  const validParams = {
    eventSourceName: 'MyService',
    bqEventsTableName: 'events_table',
    urlRegex: 'example.com',
    dataSchema: [
      {
        entityTableName: 'user',
        description: 'A user table',
        keys: [
          { keyName: 'user_id', dataType: 'string' },
          { keyName: 'created_on', dataType: 'timestamp' }
        ]
      }
    ],
    customEventSchema: [
      {
        eventType: 'custom_click',
        description: 'Custom click event',
        keys: [
          { keyName: 'click_target', dataType: 'string' }
        ]
      }
    ],
    bqProjectName: 'test_project',
    bqDatasetName: 'test_dataset',
    funnelDepth: 3,
    requestPathGroupingRegex: '/\\d+',
    expirationDays: 30,
    enableSessionTables: true,
    attributionParameters: []
  };

  it('should pass validation with correct parameters', () => {
    expect(() => validateParams(validParams)).not.toThrow();
  });

  it('should throw on unknown top-level parameter', () => {
    expect(() => validateParams({ ...validParams, unknownParam: true }))
      .toThrow(/Invalid top level parameter/);
  });

  it('should throw if eventSourceName contains invalid characters', () => {
    expect(() => validateParams({ ...validParams, eventSourceName: 'invalid-name!' }))
      .toThrow(/contains characters that are not alphanumeric or an underscore/);
  });

  it('should throw if expirationDays is negative', () => {
    expect(() => validateParams({ ...validParams, expirationDays: -5 }))
      .toThrow(/expirationDays must be a positive integer/);
  });

  it('should throw if webRequestEventExpirationDays is not a positive integer', () => {
    expect(() => validateParams({ ...validParams, webRequestEventExpirationDays: -1 }))
      .toThrow(/webRequestEventExpirationDays is not a positive integer/);
  });

  it('should throw if webRequestEventExpirationDays > expirationDays', () => {
    expect(() => validateParams({ ...validParams, webRequestEventExpirationDays: 40 }))
      .toThrow(/would result in a longer data retention schedule/);
  });

  it('should throw for invalid materialisation value', () => {
    const invalidParams = JSON.parse(JSON.stringify(validParams));
    invalidParams.dataSchema[0].materialisation = 'invalid';
    expect(() => validateParams(invalidParams))
      .toThrow(/must be either 'view' or 'table'/);
  });

  it("should throw if primaryKey is set to 'id'", () => {
    const invalidParams = JSON.parse(JSON.stringify(validParams));
    invalidParams.dataSchema[0].primaryKey = 'id';
    expect(() => validateParams(invalidParams))
      .toThrow(/primaryKey .* is set to 'id'/);
  });

  it('should throw if hidden is not a boolean', () => {
    const invalidParams = JSON.parse(JSON.stringify(validParams));
    invalidParams.dataSchema[0].keys[0].hidden = 'yes';
    expect(() => validateParams(invalidParams))
      .toThrow(/hidden .* is not a boolean/);
  });

  it('should throw if invalid custom event type is used', () => {
    const invalidParams = JSON.parse(JSON.stringify(validParams));
    invalidParams.customEventSchema[0].eventType = 'web_request';
    expect(() => validateParams(invalidParams))
      .toThrow(/is an event type streamed by dfe-analytics by default/);
  });
});
