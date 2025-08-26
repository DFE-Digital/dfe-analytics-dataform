const pipelineSnapshot = require('../includes/pipeline_snapshot');

describe('pipelineSnapshot', () => {
  let mockOperate;
  let originalProjectConfig;

  beforeEach(() => {
    originalProjectConfig = global.dataform?.projectConfig;
    global.dataform = {
      projectConfig: {
        defaultDatabase: 'test_project',
        defaultSchema: 'test_schema',
        schemaSuffix: 'dev'
      }
    };

    mockOperate = jest.fn((name, fn) => {
      return {
        dependencies: jest.fn().mockReturnThis(),
        tags: jest.fn().mockReturnThis()
      };
    });

    global.operate = mockOperate;
  });

  afterEach(() => {
    global.dataform.projectConfig = originalProjectConfig;
    jest.clearAllMocks();
  });

  const version = '1.0.0';
  const mockParams = {
    eventSourceName: 'TestService',
    bqProjectName: 'test_project',
    enableMonitoring: true
  };

  it('should return true if monitoring is disabled', () => {
    const result = pipelineSnapshot(version, { ...mockParams, enableMonitoring: false });
    expect(result).toBe(true);
    expect(mockOperate).not.toHaveBeenCalled();
  });

  it('should call operate with correct name and include expected SQL elements in dev mode', () => {
    pipelineSnapshot(version, mockParams);

    expect(mockOperate).toHaveBeenCalledWith(
      'pipeline_snapshots_TestService',
      expect.any(Function)
    );

    const callArgs = mockOperate.mock.calls[0][1];
    const result = callArgs({
      ref: (name) => `\`ref.${name}\``,
      resolve: (name) => `\`resolve.${name}\``
    });

    const sql = result[0];
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS');
    expect(sql).toContain('INSERT INTO');
    expect(sql).toContain('pipeline_snapshots');
    expect(sql).toContain('dfe_analytics_dataform_version');
    expect(sql).toContain('TestService');
    expect(sql).toContain(version);
  });

  it('should use production table names when schemaSuffix is not set', () => {
    delete global.dataform.projectConfig.schemaSuffix;

    pipelineSnapshot(version, mockParams);

    const callArgs = mockOperate.mock.calls[0][1];
    const result = callArgs({
      ref: (name) => `\`ref.${name}\``,
      resolve: (name) => `\`resolve.${name}\``
    });

    const sql = result[0];
    expect(sql).toContain('cross-teacher-services.monitoring.pipeline_snapshots');
  });
});
