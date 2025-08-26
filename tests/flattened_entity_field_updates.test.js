const flattenedEntityFieldUpdates = require('../includes/flattened_entity_field_updates');

global.publish = jest.fn(() => ({
  query: jest.fn(() => ({
    postOps: jest.fn(() => ({
      preOps: jest.fn()
    }))
  }))
}));

describe('flattened entity field updates', () => {
  const mockParams = {
    eventSourceName: 'TestService',
    bqDatasetName: 'MyDataset',
    bqProjectName: 'my-project',
    hiddenPolicyTagLocation: 'projects/my-project/locations/eu/taxonomies/1234/policyTags/abcd',
    expirationDays: 30,
    defaultConfig: { someConfig: true },
    dataSchema: [
      {
        entityTableName: 'user',
        materialisation: 'table',
        description: 'Test user entity table',
        hidePrimaryKey: true,
        expirationDays: 15,
        keys: [
          {
            keyName: 'email',
            description: 'User email address',
            hidden: true,
            hiddenPolicyTagLocation: 'projects/my-project/locations/eu/taxonomies/1234/policyTags/email'
          },
          {
            keyName: 'name',
            alias: 'user_name',
            description: 'User name',
            hidden: false
          }
        ]
      }
    ]
  };

  beforeEach(() => {
    global.publish.mockClear();
  });

  it('calls publish with correct arguments for each entity table', () => {
    flattenedEntityFieldUpdates(mockParams);

    expect(publish).toHaveBeenCalledTimes(1);

    const [tableName, config] = publish.mock.calls[0];

    expect(tableName).toBe('user_field_updates_TestService');
    expect(config.description).toMatch(/One row for each time a field was updated/);
    expect(config.type).toBe('table');
    expect(config.bigquery.partitionBy).toBe('DATE(occurred_at)');
    expect(config.bigquery.labels.entitytabletype).toBe('field_updates');

    expect(config.columns.entity_id.bigqueryPolicyTags).toEqual([
      'projects/my-project/locations/eu/taxonomies/1234/policyTags/abcd'
    ]);

    expect(config.columns).toHaveProperty('email');
    expect(config.columns.email.description).toBe('Value immediately before this update of: User email address');
    expect(config.columns.email.bigqueryPolicyTags).toEqual([
      'projects/my-project/locations/eu/taxonomies/1234/policyTags/email'
    ]);

    expect(config.columns).toHaveProperty('user_name');
    expect(config.columns.user_name.description).toBe('Value immediately before this update of: User name');
  });
});
