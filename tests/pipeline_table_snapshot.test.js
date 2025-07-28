const mockOperate = jest.fn((name, fn) => {
  return {
    name,
    query: fn({
      ref: (t) => `ref_${t}`
    })[0],
    tags: jest.fn().mockReturnThis(),
    dependencies: jest.fn().mockReturnThis()
  };
});
global.operate = mockOperate;
global.dataform = {
  projectConfig: {
    defaultSchema: 'test_schema',
    schemaSuffix: 'dev'
  }
};
const pipelineMonitoring = require('../includes/pipeline_table_snapshot');

describe('pipelineMonitoring', () => {
  it('should return true if monitoring is disabled', () => {
    const result = pipelineMonitoring('1.0.0', {
      enableMonitoring: false,
      eventSourceName: 'TestSource'
    });

    expect(result).toBe(true);
  });

  it('should generate pipeline_table_snapshots step with correct query and use dev schema in development', () => {
    const result = pipelineMonitoring('1.0.0', {
      bqProjectName: 'test_project',
      eventSourceName: 'TestSource',
      enableMonitoring: true,
      transformEntityEvents: true
    });

    expect(result.length).toBe(1);
    expect(result[0].name).toBe('pipeline_table_snapshots_TestSource');

    const sql = result[0].query;
    expect(typeof sql).toBe('string');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS');
    expect(sql).toContain('`test_project.test_schema_dev.pipeline_table_snapshots`');
  });

  it('should return valid SQL with minimal inputs and transformEntityEvents false', () => {
    const result = pipelineMonitoring('1.0.0', {
      bqProjectName: 'test_project',
      eventSourceName: 'TestSource',
      enableMonitoring: true,
      transformEntityEvents: false
    });

    const sql = result[0].query;
    expect(sql).toContain('CAST(NULL AS BOOL) AS matching_checksums');
    expect(sql).toContain('LOGICAL_OR(ARRAY_LENGTH(hidden_data) > 0)');
  });
});
