import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { mkdtempSync, readFileSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { emitCommandOutput, resolveWaitFlag } from './job-command-output.js';

describe('resolveWaitFlag', () => {
  it('returns false when noWait is explicitly true', () => {
    expect(resolveWaitFlag(true, true)).toBe(false);
    expect(resolveWaitFlag(undefined, true)).toBe(false);
  });

  it('returns wait when noWait is not set', () => {
    expect(resolveWaitFlag(true, false)).toBe(true);
    expect(resolveWaitFlag(false, false)).toBe(false);
  });

  it('defaults to true when both wait and noWait are undefined', () => {
    expect(resolveWaitFlag(undefined, undefined)).toBe(true);
  });
});

describe('emitCommandOutput', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'trading25-job-output-'));
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
    mock.restore();
  });

  it('emits JSON when json option is true', async () => {
    const log = mock();
    await emitCommandOutput({
      ctx: { log },
      payload: { job_id: 'job-1', status: 'pending' },
      options: { json: true },
    });

    expect(log).toHaveBeenCalledTimes(1);
    expect(String(log.mock.calls[0]?.[0] ?? '')).toContain('"job_id": "job-1"');
  });

  it('uses table renderer when json option is false', async () => {
    const log = mock();
    const renderTable = mock();
    await emitCommandOutput({
      ctx: { log },
      payload: { job_id: 'job-2' },
      options: { json: false },
      renderTable,
    });

    expect(renderTable).toHaveBeenCalledTimes(1);
    expect(log).toHaveBeenCalledTimes(0);
  });

  it('writes payload to output file and logs path', async () => {
    const log = mock();
    const outputPath = join(tempDir, 'nested', 'payload.json');
    const infoSpy = spyOn(console, 'log').mockImplementation(() => undefined);
    try {
      await emitCommandOutput({
        ctx: { log },
        payload: { job_id: 'job-3', status: 'completed' },
        options: { output: outputPath },
      });
    } finally {
      infoSpy.mockRestore();
    }

    const content = readFileSync(outputPath, 'utf8');
    expect(content).toContain('"job_id": "job-3"');
    expect(log).toHaveBeenCalledTimes(1);
    expect(String(log.mock.calls[0]?.[0] ?? '')).toContain('Saved output:');
  });
});
