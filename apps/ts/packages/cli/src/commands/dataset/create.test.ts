import { beforeEach, describe, expect, it, mock } from 'bun:test';

const startDatasetCreateMock = mock();
const startDatasetResumeMock = mock();
const getDatasetJobStatusMock = mock();
const cancelDatasetJobMock = mock();

mock.module('chalk', () => {
  const identity = (text: string) => text;
  const bold = Object.assign((text: string) => text, {
    white: identity,
    cyan: identity,
    yellow: identity,
    green: identity,
    red: identity,
    magenta: identity,
    blue: identity,
  });
  return {
    default: {
      red: identity,
      green: identity,
      yellow: identity,
      cyan: identity,
      white: identity,
      gray: identity,
      dim: identity,
      magenta: identity,
      blue: identity,
      bold,
    },
  };
});

mock.module('ora', () => {
  return {
    default: (text?: string) => {
      return {
        text: text ?? '',
        start() {
          return this;
        },
        succeed() {
          return this;
        },
        fail() {
          return this;
        },
        warn() {
          return this;
        },
        stop() {
          return this;
        },
      };
    },
  };
});

mock.module('../../utils/api-client.js', () => {
  class MockApiClient {
    dataset = {
      startDatasetCreate: startDatasetCreateMock,
      startDatasetResume: startDatasetResumeMock,
      getDatasetJobStatus: getDatasetJobStatusMock,
      cancelDatasetJob: cancelDatasetJobMock,
    };
  }

  return {
    ApiClient: MockApiClient,
  };
});

import { createCommand } from './create.js';

type CreateCtx = Parameters<typeof createCommand.run>[0];

function createCtx(values: Record<string, unknown>): CreateCtx {
  return { values } as CreateCtx;
}

function completedJob(name: string) {
  return {
    jobId: 'dataset-job-1',
    status: 'completed',
    preset: 'quickTesting',
    name,
    startedAt: new Date().toISOString(),
    progress: {
      stage: 'complete',
      current: 7,
      total: 7,
      percentage: 100,
      message: 'done',
    },
    result: {
      success: true,
      totalStocks: 3,
      processedStocks: 3,
      warnings: [],
      errors: [],
      outputPath: `/tmp/${name}`,
    },
  };
}

describe('dataset create command timeout propagation', () => {
  beforeEach(() => {
    startDatasetCreateMock.mockReset();
    startDatasetResumeMock.mockReset();
    getDatasetJobStatusMock.mockReset();
    cancelDatasetJobMock.mockReset();
  });

  it('passes timeout to create API call', async () => {
    startDatasetCreateMock.mockResolvedValueOnce({
      jobId: 'dataset-job-1',
      status: 'pending',
      preset: 'quickTesting',
      name: 'test.db',
      message: 'started',
    });
    getDatasetJobStatusMock.mockResolvedValueOnce(completedJob('test.db'));

    await createCommand.run(
      createCtx({
        output: 'test.db',
        preset: 'quickTesting',
        overwrite: true,
        resume: false,
        timeout: 90,
        debug: false,
      })
    );

    expect(startDatasetCreateMock).toHaveBeenCalledWith('test.db', 'quickTesting', true, 90);
    expect(startDatasetResumeMock).toHaveBeenCalledTimes(0);
    expect(getDatasetJobStatusMock).toHaveBeenCalledWith('dataset-job-1');
  });

  it('passes timeout to resume API call', async () => {
    startDatasetResumeMock.mockResolvedValueOnce({
      jobId: 'dataset-job-1',
      status: 'pending',
      preset: 'quickTesting',
      name: 'resume.db',
      message: 'started',
    });
    getDatasetJobStatusMock.mockResolvedValueOnce(completedJob('resume.db'));

    await createCommand.run(
      createCtx({
        output: 'resume.db',
        preset: 'quickTesting',
        overwrite: false,
        resume: true,
        timeout: 120,
        debug: false,
      })
    );

    expect(startDatasetResumeMock).toHaveBeenCalledWith('resume.db', 'quickTesting', 120);
    expect(startDatasetCreateMock).toHaveBeenCalledTimes(0);
    expect(getDatasetJobStatusMock).toHaveBeenCalledWith('dataset-job-1');
  });
});
