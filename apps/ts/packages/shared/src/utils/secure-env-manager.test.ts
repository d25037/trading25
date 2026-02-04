import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { SecureEnvManager } from './secure-env-manager';

let tempDir = '';
let envPath = '';
let keyPath = '';

function readEnvValue(filePath: string, key: string): string | null {
  if (!fs.existsSync(filePath)) return null;
  const content = fs.readFileSync(filePath, 'utf-8');
  const line = content
    .split('\n')
    .map((value) => value.trim())
    .find((value) => value.startsWith(`${key}=`));
  if (!line) return null;
  return line.slice(key.length + 1);
}

describe('SecureEnvManager', () => {
  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-secure-env-'));
    envPath = path.join(tempDir, '.env');
    keyPath = path.join(tempDir, '.trading25.key');
    fs.writeFileSync(envPath, 'JQUANTS_API_KEY=plain-token\n', 'utf-8');
  });

  afterEach(() => {
    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('stores encrypted tokens and returns decrypted values', async () => {
    const manager = new SecureEnvManager(envPath, keyPath);

    await manager.storeTokensSecurely({ JQUANTS_API_KEY: 'secret-token' });

    const storedValue = readEnvValue(envPath, 'JQUANTS_API_KEY');
    expect(storedValue).not.toBeNull();
    expect(storedValue).not.toBe('secret-token');

    const decrypted = await manager.getTokensSecurely();
    expect(decrypted.JQUANTS_API_KEY).toBe('secret-token');
  });

  it('returns plain token when stored value is not encrypted', async () => {
    const manager = new SecureEnvManager(envPath, keyPath);
    const tokens = await manager.getTokensSecurely();
    expect(tokens.JQUANTS_API_KEY).toBe('plain-token');
  });

  it('migrates plain token to encrypted format', async () => {
    const manager = new SecureEnvManager(envPath, keyPath);

    const result = await manager.migrateToEncryption();
    expect(result.migrated).toBe(1);
    expect(result.skipped).toBe(0);

    const storedValue = readEnvValue(envPath, 'JQUANTS_API_KEY');
    expect(storedValue).not.toBeNull();
    expect(storedValue).not.toBe('plain-token');

    const decrypted = await manager.getTokensSecurely();
    expect(decrypted.JQUANTS_API_KEY).toBe('plain-token');
  });

  it('reports encryption status with token flags', async () => {
    const manager = new SecureEnvManager(envPath, keyPath);
    await manager.storeTokensSecurely({ JQUANTS_API_KEY: 'status-token' });

    const status = await manager.getEncryptionStatus();
    expect(status.keyExists).toBe(true);
    expect(status.tokensEncrypted.JQUANTS_API_KEY).toBe(true);
  });
});
