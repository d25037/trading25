/**
 * Fetch OpenAPI schema from trading25-bt FastAPI server
 * and save as snapshot for offline type generation.
 *
 * Usage: bun scripts/fetch-bt-openapi.ts
 *
 * Environment:
 *   BT_API_URL - bt server URL (default: http://localhost:3002)
 */

import { resolve } from 'node:path';

const BT_API_URL = process.env.BT_API_URL ?? 'http://localhost:3002';
const OPENAPI_URL = `${BT_API_URL}/openapi.json`;
const OUTPUT_PATH = resolve(import.meta.dir, '../openapi/bt-openapi.json');

const outputFile = Bun.file(OUTPUT_PATH);

async function main(): Promise<void> {
  console.log(`Fetching OpenAPI schema from ${OPENAPI_URL} ...`);

  let fetched: string;
  try {
    const res = await fetch(OPENAPI_URL, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }
    const json = await res.json();
    fetched = `${JSON.stringify(json, null, 2)}\n`;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (await outputFile.exists()) {
      console.warn(`⚠ bt server unreachable (${msg}). Using existing snapshot.`);
      process.exit(0);
    }
    console.error(`✗ bt server unreachable (${msg}) and no existing snapshot found.`);
    process.exit(1);
  }

  if (await outputFile.exists()) {
    const existing = await outputFile.text();
    if (existing === fetched) {
      console.log('✓ Snapshot is up to date (no changes).');
      return;
    }
    console.log('⚠ Snapshot has changed — updating.');
  } else {
    console.log('Creating initial snapshot.');
  }

  await Bun.write(OUTPUT_PATH, fetched);
  console.log(`✓ Saved to ${OUTPUT_PATH}`);
}

main();
