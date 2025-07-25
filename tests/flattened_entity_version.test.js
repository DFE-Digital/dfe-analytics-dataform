const entityVersion = require('../includes/flattened_entity_version');

describe('entity version table generation', () => {
  let mockQuery, mockPostOps;

  beforeEach(() => {
    mockQuery = jest.fn().mockReturnThis();
    mockPostOps = jest.fn().mockReturnThis();

    global.publish = jest.fn(() => ({
      query: mockQuery,
      postOps: mockPostOps,
    }));
  });

  it('calls publish with correct config and generates expected SQL', () => {
    const mockParams = {
      eventSourceName: 'TestService',
      bqDatasetName: 'analytics_dataset',
      bqProjectName: 'project-x',
      hiddenPolicyTagLocation: 'projects/project-x/locations/eu/taxonomies/1234/policyTags/abcd',
      defaultConfig: { enabled: true },
      dataSchema: [
        {
          entityTableName: 'user',
          materialisation: 'table',
          description: 'User entity description',
          hidePrimaryKey: true,
          keys: [
            {
              keyName: 'email',
              alias: 'user_email',
              description: 'Email address of the user',
              dataType: 'string',
              hidden: true,
              hiddenPolicyTagLocation: 'projects/project-x/locations/eu/taxonomies/1234/policyTags/email'
            }
          ]
        }
      ]
    };

    entityVersion(mockParams);

    expect(global.publish).toHaveBeenCalledWith(
      'user_version_TestService',
      expect.objectContaining({
        type: 'table',
        assertions: {
          uniqueKey: ['valid_from', 'id'],
          nonNull: ['id'],
          rowConditions: ['valid_from < valid_to OR valid_to IS NULL']
        },
        tags: ['testservice'],
        bigquery: expect.objectContaining({
          partitionBy: 'DATE(valid_to)',
          labels: expect.objectContaining({
            eventsource: 'testservice',
            sourcedataset: 'analytics_dataset',
            entitytabletype: 'version'
          })
        }),
        columns: expect.objectContaining({
          id: expect.objectContaining({
            description: expect.any(String),
            bigqueryPolicyTags: [mockParams.hiddenPolicyTagLocation]
          }),
          user_email: expect.objectContaining({
            description: 'Email address of the user',
            bigqueryPolicyTags: ['projects/project-x/locations/eu/taxonomies/1234/policyTags/email']
          })
        })
      })
    );

    expect(mockQuery).toHaveBeenCalled();
    const queryFn = mockQuery.mock.calls[0][0];
    const mockCtx = { ref: (name) => `\`${name}\`` };
    const sql = queryFn(mockCtx);

    expect(sql).toContain('SELECT');
    expect(sql).toContain('valid_from');
    expect(sql).toContain('event_type');
    expect(sql).toContain('user_email');
    expect(sql).toContain('FROM');
    expect(sql).toContain('user');
    expect(sql).toContain('entity_table_name = "user"');
  });
});
