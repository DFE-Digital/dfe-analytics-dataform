const flattenedLatestEntity = require('../includes/flattened_entity_latest');

describe('latest entity version', () => {
  let mockQuery, mockPostOps;

  beforeEach(() => {
    mockQuery = jest.fn().mockReturnThis();
    mockPostOps = jest.fn().mockReturnThis();

    global.publish = jest.fn(() => ({
      query: mockQuery,
      postOps: mockPostOps
    }));
  });

  it('calls publish with correct arguments for latest entity version and generates expected SQL', () => {
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
          description: 'User data table',
          hidePrimaryKey: true,
          keys: [
            {
              keyName: 'email',
              alias: 'user_email',
              description: 'User email address',
              hidden: true,
              hiddenPolicyTagLocation: 'projects/project-x/locations/eu/taxonomies/1234/policyTags/email'
            },
            {
              keyName: 'school_id',
              description: 'School foreign key',
              foreignKeyTable: 'school'
            }
          ]
        }
      ]
    };

    flattenedLatestEntity(mockParams);

    expect(global.publish).toHaveBeenCalled();

    expect(mockQuery).toHaveBeenCalled();
    const sqlFn = mockQuery.mock.calls[0][0]; // the ctx => `...` function
    const sql = sqlFn({
      ref: (name) => `\`${name}\``
    });

    expect(sql).toContain('SELECT');
    expect(sql).toContain('valid_from AS last_streamed_event_occurred_at');
    expect(sql).toContain('event_type AS last_streamed_event_type');
    expect(sql).toContain('FROM');
    expect(sql).toContain('`user_version_TestService`');
    expect(sql).toContain('valid_to IS NULL');
  });
});
