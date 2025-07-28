const mockOperate = jest.fn(() => ({
  tags: jest.fn().mockReturnThis()
}));
global.operate = mockOperate;
global.dataform = {
  projectConfig: {
    defaultDatabase: 'test_db',
    defaultSchema: 'test_schema',
    schemaSuffix: 'dev'
  }
};

const migrate = require('../includes/migrate_historic_events_to_current_hidden_pii_configuration');

describe('migrate_historic_events_to_current_hidden_pii_configuration', () => {
  it('calls operate with correct name and SQL structure', () => {
    const mockParams = {
      eventSourceName: 'TestService',
      bqProjectName: 'my-project',
      bqDatasetName: 'analytics_dataset',
      bqEventsTableName: 'raw_events',
      dataSchema: [
        {
          entityTableName: 'user',
          primaryKey: 'id',
          hidePrimaryKey: true,
          keys: [
            { keyName: 'email', hidden: true },
            { keyName: 'nickname', hidden: false }
          ]
        }
      ],
      customEventSchema: [
        {
          eventType: 'page_viewed',
          keys: [
            { keyName: 'url', hidden: true },
            { keyName: 'page_title', hidden: false }
          ]
        }
      ]
    };

    migrate(mockParams);

    expect(mockOperate).toHaveBeenCalledWith(
      'TestService_migrate_historic_events_to_current_hidden_pii_configuration',
      expect.any(Function)
    );

    const ctx = {
      resolve: (name) => `\`${name}\``,
      ref: (name) => `\`ref_${name}\``
    };

    const sqlArray = mockOperate.mock.calls[0][1](ctx);
    const sql = sqlArray[0];

    expect(sql).toContain('CREATE OR REPLACE PROCEDURE');
    expect(sql).toContain('BEGIN TRANSACTION');
    expect(sql).toContain('RAISE USING MESSAGE');
    expect(sql).toContain('UPDATE');
    expect(sql).toContain('event.entity_table_name = entity.entity_name');
    expect(sql).toContain('event.event_type IN ("create_entity", "update_entity", "delete_entity", "import_entity")');
    expect(sql).toContain('custom_event.visible_keys');
    expect(sql).toContain('COMMIT TRANSACTION');
  });
});
