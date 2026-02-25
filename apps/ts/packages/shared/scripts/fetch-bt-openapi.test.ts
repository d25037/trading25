import { describe, expect, test } from 'bun:test';
import { rm } from 'node:fs/promises';

import {
  main,
  normalizeOpenApiText,
  resolveConfig,
  summarizeStderr,
  syncOpenApiSnapshot,
  tryFetchFromServer,
  tryGenerateFromBtSource,
  type FetchBtOpenApiConfig,
  type SyncDeps,
} from './fetch-bt-openapi';

function streamFrom(text: string): ReadableStream<Uint8Array> {
  return new Response(text).body as ReadableStream<Uint8Array>;
}

function spawnResult(stdout: string, stderr: string, exitCode: number) {
  return {
    stdout: streamFrom(stdout),
    stderr: streamFrom(stderr),
    exited: Promise.resolve(exitCode),
  };
}

function baseConfig(): FetchBtOpenApiConfig {
  return {
    btProjectPath: '/tmp/project',
    btApiUrl: 'http://localhost:3002',
    uvCacheDir: '/tmp/uv-cache',
    outputPath: '/tmp/openapi.json',
    openapiUrl: 'http://localhost:3002/openapi.json',
    btVenvPython: '/tmp/project/.venv/bin/python',
  };
}

function silentLogger() {
  return {
    log: (_message: string) => {},
    warn: (_message: string) => {},
    error: (_message: string) => {},
  };
}

describe('fetch-bt-openapi helpers', () => {
  test('resolveConfig uses defaults and optional output override', () => {
    const config = resolveConfig({
      BT_PROJECT_PATH: '/repo/apps/bt',
      BT_API_URL: 'http://127.0.0.1:3002',
      UV_CACHE_DIR: '/tmp/custom-uv',
      BT_OPENAPI_OUTPUT_PATH: '/tmp/custom-openapi.json',
    });

    expect(config.btProjectPath).toBe('/repo/apps/bt');
    expect(config.btApiUrl).toBe('http://127.0.0.1:3002');
    expect(config.uvCacheDir).toBe('/tmp/custom-uv');
    expect(config.outputPath).toBe('/tmp/custom-openapi.json');
    expect(config.openapiUrl).toBe('http://127.0.0.1:3002/openapi.json');
    expect(config.btVenvPython).toBe('/repo/apps/bt/.venv/bin/python');
  });

  test('normalizeOpenApiText canonicalizes JSON', () => {
    const normalized = normalizeOpenApiText('{"b":2,"a":1}');
    expect(normalized).toBe('{\n  "a": 1,\n  "b": 2\n}\n');
  });

  test('normalizeOpenApiText sorts nested object keys recursively', () => {
    const normalized = normalizeOpenApiText(
      '{"paths":{"z":{"post":{}},"a":{"get":{}}},"components":{"schemas":{"B":{"type":"object"},"A":{"type":"object"}}}}',
    );

    expect(normalized).toBe(
      '{\n'
        + '  "components": {\n'
        + '    "schemas": {\n'
        + '      "A": {\n'
        + '        "type": "object"\n'
        + '      },\n'
        + '      "B": {\n'
        + '        "type": "object"\n'
        + '      }\n'
        + '    }\n'
        + '  },\n'
        + '  "paths": {\n'
        + '    "a": {\n'
        + '      "get": {}\n'
        + '    },\n'
        + '    "z": {\n'
        + '      "post": {}\n'
        + '    }\n'
        + '  }\n'
        + '}\n',
    );
  });

  test('summarizeStderr returns last non-empty line or exit code', () => {
    expect(summarizeStderr('\nfoo\nbar\n', 1)).toBe('bar');
    expect(summarizeStderr('', 7)).toBe('exit code 7');
  });
});

describe('tryGenerateFromBtSource', () => {
  test('returns generated schema via .venv python when available', async () => {
    const config = baseConfig();
    const commands: string[][] = [];

    const generated = await tryGenerateFromBtSource(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.btVenvPython,
      spawn: (command) => {
        commands.push(command);
        return spawnResult('{"openapi":"3.1.0"}', '', 0);
      },
    });

    expect(generated).toBe('{\n  "openapi": "3.1.0"\n}\n');
    expect(commands).toHaveLength(1);
    expect(commands[0]).toEqual([config.btVenvPython, 'scripts/export_openapi.py']);
  });

  test('falls back to uv run after .venv failure', async () => {
    const config = baseConfig();
    const commands: string[][] = [];
    let callCount = 0;

    const generated = await tryGenerateFromBtSource(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.btVenvPython,
      spawn: (command) => {
        commands.push(command);
        callCount += 1;
        if (callCount === 1) {
          return spawnResult('', 'ModuleNotFoundError', 1);
        }
        return spawnResult('{"openapi":"3.1.0"}', '', 0);
      },
    });

    expect(generated).toBe('{\n  "openapi": "3.1.0"\n}\n');
    expect(commands).toHaveLength(2);
    expect(commands[1]).toEqual(['uv', 'run', 'python', 'scripts/export_openapi.py']);
  });

  test('returns null when all generation methods fail', async () => {
    const config = baseConfig();

    const generated = await tryGenerateFromBtSource(config, {
      logger: silentLogger(),
      exists: async () => false,
      spawn: () => spawnResult('', 'failed', 1),
    });

    expect(generated).toBeNull();
  });

  test('returns null when spawn throws', async () => {
    const config = baseConfig();

    const generated = await tryGenerateFromBtSource(config, {
      logger: silentLogger(),
      exists: async () => false,
      spawn: () => {
        throw new Error('spawn crashed');
      },
    });

    expect(generated).toBeNull();
  });
});

describe('tryFetchFromServer', () => {
  test('returns normalized schema on successful fetch', async () => {
    const config = baseConfig();
    const fetched = await tryFetchFromServer(config, {
      logger: silentLogger(),
      fetch: async () => new Response('{"openapi":"3.1.0"}', { status: 200 }),
    });

    expect(fetched).toBe('{\n  "openapi": "3.1.0"\n}\n');
  });

  test('returns null when fetch fails', async () => {
    const config = baseConfig();
    const fetched = await tryFetchFromServer(config, {
      logger: silentLogger(),
      fetch: async () => {
        throw new Error('network down');
      },
    });

    expect(fetched).toBeNull();
  });

  test('returns null when server responds with non-OK status', async () => {
    const config = baseConfig();
    const fetched = await tryFetchFromServer(config, {
      logger: silentLogger(),
      fetch: async () => new Response('oops', { status: 503, statusText: 'Service Unavailable' }),
    });

    expect(fetched).toBeNull();
  });
});

describe('syncOpenApiSnapshot', () => {
  test('returns 1 when refresh fails and snapshot is missing', async () => {
    const config = baseConfig();

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async () => false,
      spawn: () => spawnResult('', 'failed', 1),
      fetch: async () => {
        throw new Error('network down');
      },
    });

    expect(exitCode).toBe(1);
  });

  test('returns 1 when existing snapshot is invalid JSON', async () => {
    const config = baseConfig();

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.outputPath,
      readText: async () => '{invalid json',
      spawn: () => spawnResult('', 'failed', 1),
      fetch: async () => {
        throw new Error('network down');
      },
    });

    expect(exitCode).toBe(1);
  });

  test('normalizes existing snapshot when refresh fails', async () => {
    const config = baseConfig();
    let written: string | null = null;

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.outputPath,
      readText: async () => '{"openapi":"3.1.0"}',
      writeText: async (_path, text) => {
        written = text;
      },
      spawn: () => spawnResult('', 'failed', 1),
      fetch: async () => {
        throw new Error('network down');
      },
    });

    expect(exitCode).toBe(0);
    expect(written).toBe('{\n  "openapi": "3.1.0"\n}\n');
  });

  test('keeps existing snapshot when already normalized and refresh fails', async () => {
    const config = baseConfig();
    let writeCalled = false;

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.outputPath,
      readText: async () => '{\n  "openapi": "3.1.0"\n}\n',
      writeText: async () => {
        writeCalled = true;
      },
      spawn: () => spawnResult('', 'failed', 1),
      fetch: async () => {
        throw new Error('network down');
      },
    });

    expect(exitCode).toBe(0);
    expect(writeCalled).toBe(false);
  });

  test('writes updated snapshot when fetched content changed', async () => {
    const config = baseConfig();
    let written: string | null = null;

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.outputPath,
      readText: async () => '{"openapi":"3.0.0"}\n',
      writeText: async (_path, text) => {
        written = text;
      },
      spawn: () => spawnResult('{"openapi":"3.1.0"}', '', 0),
    });

    expect(exitCode).toBe(0);
    expect(written).toBe('{\n  "openapi": "3.1.0"\n}\n');
  });

  test('does not write when snapshot is already up to date', async () => {
    const config = baseConfig();
    let writeCalled = false;

    const normalized = '{\n  "openapi": "3.1.0"\n}\n';

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async (path) => path === config.outputPath,
      readText: async () => normalized,
      writeText: async () => {
        writeCalled = true;
      },
      spawn: () => spawnResult('{"openapi":"3.1.0"}', '', 0),
    });

    expect(exitCode).toBe(0);
    expect(writeCalled).toBe(false);
  });

  test('creates initial snapshot when none exists', async () => {
    const config = baseConfig();
    let written: string | null = null;

    const exitCode = await syncOpenApiSnapshot(config, {
      logger: silentLogger(),
      exists: async () => false,
      writeText: async (_path, text) => {
        written = text;
      },
      spawn: () => spawnResult('{"openapi":"3.1.0"}', '', 0),
    });

    expect(exitCode).toBe(0);
    expect(written).toBe('{\n  "openapi": "3.1.0"\n}\n');
  });

  test('uses default deps safely with local temp output path', async () => {
    const outputPath = `${process.cwd()}/packages/shared/.tmp-openapi.json`;
    await Bun.write(outputPath, '{"openapi":"3.1.0"}');

    const config: FetchBtOpenApiConfig = {
      btProjectPath: '/path/that/does/not/exist',
      btApiUrl: 'http://127.0.0.1:3002',
      uvCacheDir: '/tmp/uv-cache',
      outputPath,
      openapiUrl: 'http://127.0.0.1:9/openapi.json',
      btVenvPython: '/path/that/does/not/exist/.venv/bin/python',
    };

    const exitCode = await syncOpenApiSnapshot(config, { logger: silentLogger() });
    expect(exitCode).toBe(0);

    const text = await Bun.file(outputPath).text();
    expect(text).toBe('{\n  "openapi": "3.1.0"\n}\n');
    await rm(outputPath, { force: true });
  });
});

describe('main', () => {
  test('exits with sync result code', async () => {
    const outputPath = `${process.cwd()}/packages/shared/.tmp-openapi-main.json`;
    await Bun.write(outputPath, '{\n  "openapi": "3.1.0"\n}\n');

    const previous = {
      BT_PROJECT_PATH: process.env.BT_PROJECT_PATH,
      BT_API_URL: process.env.BT_API_URL,
      BT_OPENAPI_OUTPUT_PATH: process.env.BT_OPENAPI_OUTPUT_PATH,
      UV_CACHE_DIR: process.env.UV_CACHE_DIR,
    };
    process.env.BT_PROJECT_PATH = '/path/that/does/not/exist';
    process.env.BT_API_URL = 'http://127.0.0.1:9';
    process.env.BT_OPENAPI_OUTPUT_PATH = outputPath;
    process.env.UV_CACHE_DIR = '/tmp/uv-cache';

    const originalExit = process.exit;
    let capturedCode: number | undefined;
    process.exit = ((code?: number) => {
      capturedCode = code;
      throw new Error('process.exit called');
    }) as typeof process.exit;

    try {
      await main();
      throw new Error('main should have called process.exit');
    } catch (err) {
      expect(err).toBeInstanceOf(Error);
      expect(capturedCode).toBe(0);
    } finally {
      process.exit = originalExit;
      process.env.BT_PROJECT_PATH = previous.BT_PROJECT_PATH;
      process.env.BT_API_URL = previous.BT_API_URL;
      process.env.BT_OPENAPI_OUTPUT_PATH = previous.BT_OPENAPI_OUTPUT_PATH;
      process.env.UV_CACHE_DIR = previous.UV_CACHE_DIR;
      await rm(outputPath, { force: true });
    }
  });
});
