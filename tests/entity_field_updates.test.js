const { canonicalizeSQL } = require('./helpers/sql');

const mockPostOps = jest.fn();
const mockQuery = jest.fn(() => ({ postOps: mockPostOps }));
const mockPublish = jest.fn(() => ({
  query: mockQuery
}));

global.publish = mockPublish;

const data_functions = {
  setKeyConstraints: jest.fn(() => 'MOCKED_KEY_CONSTRAINTS'),
};
global.data_functions = data_functions;
global.dataform = {};

const generateEntityFieldUpdates = require('../includes/entity_field_updates');

describe('entity_field_updates', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('calls publish with correct name, config, query and postOps', () => {
    const params = {
      eventSourceName: 'MyService',
      bqDatasetName: 'MyDataset',
      hiddenPolicyTagLocation: 'projects/myproject/locations/eu/taxonomies/123/policyTags/abc',
      defaultConfig: { type: 'table', protected: false },
      dataSchema: [
        { entityTableName: 'schools', hidePrimaryKey: true }
      ],
    };

    generateEntityFieldUpdates(params);

    expect(mockPublish).toHaveBeenCalledWith(
      'MyService_entity_field_updates',
      expect.objectContaining({
        type: 'table',
        protected: false,
        assertions: expect.any(Object),
        bigquery: expect.any(Object),
        tags: ['myservice'],
        description: expect.any(String),
        columns: expect.any(Object)
      })
    );

    const queryFn = mockQuery.mock.calls[0][0];
    const mockCtx = {
      ref: (table) => `\`${table}\``
    };
    const sql = canonicalizeSQL(queryFn(mockCtx));
    expect(sql).toContain('from `myservice_entity_version`');
    expect(sql).toContain('lag(valid_from)over versions_of_this_instance_over_time');
    expect(sql).toContain('array_to_string(new_data_combined.value,",")as new_value');

    const postOpsFn = mockPostOps.mock.calls[0][0];
    postOpsFn(mockCtx);
    expect(data_functions.setKeyConstraints).toHaveBeenCalledWith(
      mockCtx,
      expect.anything(),
      expect.objectContaining({
        primaryKey: expect.stringContaining('entity_id'),
        foreignKeys: [
          expect.objectContaining({
            foreignTable: 'MyService_entity_version',
          }),
        ],
      })
    );
  });
});
