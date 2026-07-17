/**
 * Run the repository's non-destructive OpenAPI and generated-type drift gate.
 *
 * Usage: bun scripts/check-bt-types.ts
 *
 * Exit codes:
 *   0 — committed snapshot and generated types are current
 *   non-zero — source export or drift check failed
 */

import { resolve } from 'node:path';

const REPO_ROOT = resolve(import.meta.dir, '../../../../..');
const CHECK_SCRIPT = resolve(REPO_ROOT, 'scripts/check-contract-sync.sh');

async function main(): Promise<void> {
  const proc = Bun.spawn([CHECK_SCRIPT], {
    cwd: REPO_ROOT,
    stdout: 'inherit',
    stderr: 'inherit',
  });
  const exitCode = await proc.exited;

  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}

void main();
