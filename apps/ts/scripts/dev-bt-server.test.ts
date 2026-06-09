import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'bun:test';

const REPO_ROOT = resolve(import.meta.dirname, '../../..');
const SCRIPT_PATH = resolve(REPO_ROOT, 'scripts/dev-bt-server.sh');

describe('dev-bt-server wrapper', () => {
  it('uses an existing JQUANTS_API_KEY env override and builds the bt server command', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'trading25-dev-bt-server-'));
    const configPath = join(tempRoot, 'config.env');

    try {
      await writeFile(configPath, 'BT_PORT=3999\nLOG_LEVEL=debug\n');

      const proc = Bun.spawn({
        cmd: [SCRIPT_PATH],
        env: {
          ...process.env,
          TRADING25_CONFIG_FILE: configPath,
          TRADING25_DRY_RUN: '1',
          JQUANTS_API_KEY: 'test-jquants-api-key',
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });

      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);

      expect(exitCode).toBe(0);
      expect(stderr).toBe('');
      expect(stdout).toContain('J-Quants auth mode: env');
      expect(stdout).toContain('TRADING25_FORCE_COLOR=1');
      expect(stdout).toContain('uv run --project apps/bt bt server --port 3999');
      expect(stdout).not.toContain('test-jquants-api-key');
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('can load JQUANTS_API_KEY from macOS Keychain without printing it', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'trading25-dev-bt-server-'));
    const configPath = join(tempRoot, 'config.env');
    const binPath = join(tempRoot, 'bin');
    const securityPath = join(binPath, 'security');

    try {
      await writeFile(
        configPath,
        [
          'BT_PORT=3999',
          'TRADING25_JQUANTS_API_KEY_KEYCHAIN_SERVICE=trading25-jquants-api-key',
          'TRADING25_JQUANTS_API_KEY_KEYCHAIN_ACCOUNT=trading25',
          '',
        ].join('\n')
      );
      await mkdir(binPath);
      await writeFile(securityPath, '#!/usr/bin/env bash\nprintf test-jquants-api-key\n');
      await chmod(securityPath, 0o700);

      const proc = Bun.spawn({
        cmd: [SCRIPT_PATH],
        env: {
          ...process.env,
          PATH: `${binPath}:${process.env.PATH ?? ''}`,
          TRADING25_CONFIG_FILE: configPath,
          TRADING25_DRY_RUN: '1',
          JQUANTS_API_KEY: '',
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });

      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);

      expect(exitCode).toBe(0);
      expect(stderr).toBe('');
      expect(stdout).toContain('J-Quants auth mode: keychain');
      expect(stdout).toContain('uv run --project apps/bt bt server --port 3999');
      expect(stdout).not.toContain('test-jquants-api-key');
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('rejects JQUANTS_API_KEY from the persistent config file', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'trading25-dev-bt-server-'));
    const configPath = join(tempRoot, 'config.env');

    try {
      await writeFile(configPath, 'JQUANTS_API_KEY=persistent-secret\n');

      const proc = Bun.spawn({
        cmd: [SCRIPT_PATH],
        env: {
          ...process.env,
          TRADING25_CONFIG_FILE: configPath,
          TRADING25_DRY_RUN: '1',
          JQUANTS_API_KEY: '',
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });

      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);

      expect(exitCode).toBe(1);
      expect(stdout).toBe('');
      expect(stderr).toContain('Do not set JQUANTS_API_KEY');
      expect(stderr).not.toContain('persistent-secret');
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });
});
