/**
 * Resolve bt OpenAPI schema and save as snapshot for offline type generation.
 *
 * Resolution order:
 * 1) Generate directly from apps/bt source (prefer .venv python, then uv)
 * 2) Fetch from running FastAPI server (/openapi.json) as fallback
 * 3) If both fail and snapshot exists, keep existing snapshot
 *
 * Usage: bun scripts/fetch-bt-openapi.ts
 *
 * Environment:
 *   BT_PROJECT_PATH - bt project path (default: ../../../../bt from this script)
 *   BT_API_URL - bt server URL for fallback fetch (default: http://localhost:3002)
 *   BT_OPENAPI_OUTPUT_PATH - output snapshot path override
 *   UV_CACHE_DIR - uv cache directory (default: /tmp/uv-cache)
 */

import { resolve } from 'node:path';

export interface FetchBtOpenApiConfig {
  btProjectPath: string;
  btApiUrl: string;
  uvCacheDir: string;
  outputPath: string;
  openapiUrl: string;
  btVenvPython: string;
}

interface SpawnOptions {
  cwd: string;
  stdout: 'pipe';
  stderr: 'pipe';
  env?: Record<string, string | undefined>;
}

interface SpawnResult {
  stdout: ReadableStream<Uint8Array>;
  stderr: ReadableStream<Uint8Array>;
  exited: Promise<number>;
}

type SpawnFn = (command: string[], options: SpawnOptions) => SpawnResult;
type ExistsFn = (path: string) => Promise<boolean>;
type ReadTextFn = (path: string) => Promise<string>;
type WriteTextFn = (path: string, text: string) => Promise<void>;
type FetchFn = typeof fetch;

interface LoggerLike {
  log(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export interface SyncDeps {
  spawn: SpawnFn;
  exists: ExistsFn;
  readText: ReadTextFn;
  writeText: WriteTextFn;
  fetch: FetchFn;
  logger: LoggerLike;
}

export function resolveConfig(env: NodeJS.ProcessEnv = process.env): FetchBtOpenApiConfig {
  const btProjectPath = env.BT_PROJECT_PATH ?? resolve(import.meta.dir, '../../../../bt');
  const btApiUrl = env.BT_API_URL ?? 'http://localhost:3002';
  const uvCacheDir = env.UV_CACHE_DIR ?? '/tmp/uv-cache';
  const outputPath = env.BT_OPENAPI_OUTPUT_PATH ?? resolve(import.meta.dir, '../openapi/bt-openapi.json');
  return {
    btProjectPath,
    btApiUrl,
    uvCacheDir,
    outputPath,
    openapiUrl: `${btApiUrl}/openapi.json`,
    btVenvPython: resolve(btProjectPath, '.venv/bin/python'),
  };
}

export function toMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function sortJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sortJsonValue(item));
  }

  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    const sorted: Record<string, unknown> = {};
    for (const [key, child] of entries) {
      sorted[key] = sortJsonValue(child);
    }
    return sorted;
  }

  return value;
}

export function normalizeOpenApiText(text: string): string {
  const parsed = JSON.parse(text);
  const sorted = sortJsonValue(parsed);
  return `${JSON.stringify(sorted, null, 2)}\n`;
}

export function summarizeStderr(stderr: string, exitCode: number): string {
  const lines = stderr
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) {
    return `exit code ${exitCode}`;
  }
  return lines[lines.length - 1];
}

function defaultDeps(): SyncDeps {
  return {
    spawn: (command: string[], options: SpawnOptions): SpawnResult => Bun.spawn(command, options),
    exists: async (path: string) => Bun.file(path).exists(),
    readText: async (path: string) => Bun.file(path).text(),
    writeText: async (path: string, text: string) => {
      await Bun.write(path, text);
    },
    fetch,
    logger: console,
  };
}

async function readStream(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new Response(stream).text();
}

export async function tryGenerateFromBtSource(
  config: FetchBtOpenApiConfig,
  depsOverrides: Partial<SyncDeps> = {},
): Promise<string | null> {
  const deps = { ...defaultDeps(), ...depsOverrides };
  deps.logger.log(`Generating OpenAPI schema from bt source (${config.btProjectPath}) ...`);

  const attempts: Array<{ command: string[]; label: string; env?: Record<string, string | undefined> }> = [];
  if (await deps.exists(config.btVenvPython)) {
    attempts.push({
      command: [config.btVenvPython, 'scripts/export_openapi.py'],
      label: '.venv python',
    });
  }
  attempts.push({
    command: ['uv', 'run', 'python', 'scripts/export_openapi.py'],
    label: 'uv run',
    env: { ...process.env, UV_CACHE_DIR: config.uvCacheDir },
  });

  for (const attempt of attempts) {
    try {
      const proc = deps.spawn(
        attempt.command,
        {
          cwd: config.btProjectPath,
          stdout: 'pipe',
          stderr: 'pipe',
          env: attempt.env,
        },
      );
      const [stdout, stderr, exitCode] = await Promise.all([
        readStream(proc.stdout),
        readStream(proc.stderr),
        proc.exited,
      ]);

      if (exitCode !== 0) {
        const reason = summarizeStderr(stderr, exitCode);
        deps.logger.warn(`⚠ Local generation via ${attempt.label} failed (${reason}).`);
        continue;
      }

      const generated = normalizeOpenApiText(stdout);
      deps.logger.log(`✓ Generated OpenAPI schema from bt source via ${attempt.label}.`);
      return generated;
    } catch (err) {
      deps.logger.warn(`⚠ Local generation via ${attempt.label} failed (${toMessage(err)}).`);
      continue;
    }
  }

  deps.logger.warn('⚠ Local generation failed for all methods. Falling back to HTTP fetch.');
  return null;
}

export async function tryFetchFromServer(
  config: FetchBtOpenApiConfig,
  depsOverrides: Partial<SyncDeps> = {},
): Promise<string | null> {
  const deps = { ...defaultDeps(), ...depsOverrides };
  deps.logger.log(`Fetching OpenAPI schema from ${config.openapiUrl} ...`);
  try {
    const res = await deps.fetch(config.openapiUrl, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }

    const body = await res.text();
    const fetched = normalizeOpenApiText(body);
    deps.logger.log('✓ Fetched OpenAPI schema from running server.');
    return fetched;
  } catch (err) {
    deps.logger.warn(`⚠ HTTP fetch failed (${toMessage(err)}).`);
    return null;
  }
}

export async function syncOpenApiSnapshot(
  config: FetchBtOpenApiConfig = resolveConfig(),
  depsOverrides: Partial<SyncDeps> = {},
): Promise<number> {
  const deps = { ...defaultDeps(), ...depsOverrides };
  const generated = await tryGenerateFromBtSource(config, deps);
  const fetched = generated ?? (await tryFetchFromServer(config, deps));

  if (fetched === null) {
    if (await deps.exists(config.outputPath)) {
      const existing = await deps.readText(config.outputPath);
      try {
        const normalizedExisting = normalizeOpenApiText(existing);
        if (normalizedExisting !== existing) {
          await deps.writeText(config.outputPath, normalizedExisting);
          deps.logger.warn('⚠ Could not refresh schema. Normalized existing snapshot and continued.');
        } else {
          deps.logger.warn('⚠ Could not refresh schema. Using existing snapshot.');
        }
        return 0;
      } catch {
        deps.logger.error('✗ Could not refresh schema. Existing snapshot is not valid JSON.');
        return 1;
      }
    }

    deps.logger.error('✗ Could not generate/fetch bt OpenAPI schema and no snapshot exists.');
    return 1;
  }

  if (await deps.exists(config.outputPath)) {
    const existing = await deps.readText(config.outputPath);
    if (existing === fetched) {
      deps.logger.log('✓ Snapshot is up to date (no changes).');
      return 0;
    }
    deps.logger.log('⚠ Snapshot has changed — updating.');
  } else {
    deps.logger.log('Creating initial snapshot.');
  }

  await deps.writeText(config.outputPath, fetched);
  deps.logger.log(`✓ Saved to ${config.outputPath}`);
  return 0;
}

export async function main(): Promise<void> {
  const exitCode = await syncOpenApiSnapshot(resolveConfig());
  process.exit(exitCode);
}

if (import.meta.main) {
  void main();
}
