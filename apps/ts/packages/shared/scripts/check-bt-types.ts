/**
 * CI sanity check: Verify that bt-api-types.ts can be generated
 * from the committed OpenAPI snapshot without errors.
 *
 * In the CI pipeline, bt:generate-types runs before tsc --noEmit,
 * so the type-compatibility-check.ts will catch structural drift.
 * This script is an additional guard that the snapshot is valid.
 *
 * Usage: bun scripts/check-bt-types.ts
 *
 * Exit codes:
 *   0 — generation succeeded
 *   1 — snapshot missing or generation failed
 */

import { resolve } from 'node:path';

const SCHEMA_PATH = resolve(import.meta.dir, '../openapi/bt-openapi.json');
const OUTPUT_PATH = resolve(import.meta.dir, '../src/clients/backtest/generated/bt-api-types.ts');

async function main(): Promise<void> {
  if (!(await Bun.file(SCHEMA_PATH).exists())) {
    console.error('✗ OpenAPI snapshot not found:', SCHEMA_PATH);
    console.error('  Run `bun run bt:fetch-schema` with bt server running to create it.');
    process.exit(1);
  }

  console.log('Generating bt-api-types.ts from snapshot...');
  const proc = Bun.spawn(
    ['bunx', 'openapi-typescript', SCHEMA_PATH, '-o', OUTPUT_PATH],
    { cwd: resolve(import.meta.dir, '..'), stdout: 'inherit', stderr: 'inherit' },
  );
  const exitCode = await proc.exited;

  if (exitCode !== 0) {
    console.error('✗ openapi-typescript failed (exit code', exitCode, ')');
    process.exit(1);
  }

  console.log('✓ bt-api-types.ts generated successfully.');
}

main();
