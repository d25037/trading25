import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'bun:test';

const REPO_ROOT = resolve(import.meta.dirname, '../../..');
const SCRIPT_PATH = resolve(REPO_ROOT, 'scripts/dev-bt-server.sh');

describe('dev-bt-server wrapper', () => {
  it('sources plain config and builds op run command for secrets', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'trading25-dev-bt-server-'));
    const configPath = join(tempRoot, 'config.env');
    const secretsPath = join(tempRoot, 'secrets.env');

    try {
      await writeFile(configPath, 'BT_PORT=3999\nLOG_LEVEL=debug\n');
      await writeFile(secretsPath, 'JQUANTS_API_KEY=op://Personal/Jpx-jquants/API-KEY\n');

      const proc = Bun.spawn({
        cmd: [SCRIPT_PATH],
        env: {
          ...process.env,
          TRADING25_CONFIG_FILE: configPath,
          TRADING25_SECRETS_FILE: secretsPath,
          TRADING25_DRY_RUN: '1',
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
      expect(stdout).toContain('1Password auth mode: desktop-app-or-existing-op-session');
      expect(stdout).toContain('TRADING25_FORCE_COLOR=1');
      expect(stdout).toContain(`op run --env-file ${secretsPath}`);
      expect(stdout).toContain('uv run --project apps/bt bt server --port 3999');
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('can load the 1Password service account token from macOS Keychain without printing it', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'trading25-dev-bt-server-'));
    const configPath = join(tempRoot, 'config.env');
    const secretsPath = join(tempRoot, 'secrets.env');
    const binPath = join(tempRoot, 'bin');
    const securityPath = join(binPath, 'security');

    try {
      await writeFile(
        configPath,
        [
          'BT_PORT=3999',
          'TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_SERVICE=trading25-op-service-account-token',
          'TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_ACCOUNT=trading25',
          '',
        ].join('\n')
      );
      await writeFile(secretsPath, 'JQUANTS_API_KEY=op://Personal/Jpx-jquants/API-KEY\n');
      await mkdir(binPath);
      await writeFile(securityPath, '#!/usr/bin/env bash\nprintf test-service-account-token\n');
      await chmod(securityPath, 0o700);

      const proc = Bun.spawn({
        cmd: [SCRIPT_PATH],
        env: {
          ...process.env,
          PATH: `${binPath}:${process.env.PATH ?? ''}`,
          TRADING25_CONFIG_FILE: configPath,
          TRADING25_SECRETS_FILE: secretsPath,
          TRADING25_DRY_RUN: '1',
          OP_SERVICE_ACCOUNT_TOKEN: '',
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
      expect(stdout).toContain('1Password auth mode: service-account-keychain');
      expect(stdout).toContain(`op run --env-file ${secretsPath}`);
      expect(stdout).not.toContain('test-service-account-token');
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });
});
