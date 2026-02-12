import { beforeEach, describe, expect, it, spyOn } from 'bun:test';
import attributionCommandRunner from './runner.js';

describe('backtest attribution command runner', () => {
  beforeEach(() => {
    spyOn(console, 'error').mockImplementation(() => {});
    spyOn(console, 'log').mockImplementation(() => {});
  });

  it('rejects non-integer random seed on run', async () => {
    await expect(
      attributionCommandRunner(['run', 'strategy.yml', '--no-wait', '--random-seed', '1.2'])
    ).rejects.toMatchObject({
      message: 'randomSeed must be an integer',
    });
  });

  it('rejects invalid run format', async () => {
    await expect(
      attributionCommandRunner(['run', 'strategy.yml', '--no-wait', '--format', 'yaml'])
    ).rejects.toMatchObject({
      message: 'format must be one of: table, json',
    });
  });

  it('rejects invalid status format', async () => {
    await expect(attributionCommandRunner(['status', 'job-1', '--format', 'yaml'])).rejects.toMatchObject({
      message: 'format must be one of: table, json',
    });
  });

  it('rejects invalid results format', async () => {
    await expect(attributionCommandRunner(['results', 'job-1', '--format', 'yaml'])).rejects.toMatchObject({
      message: 'format must be one of: table, json',
    });
  });
});
