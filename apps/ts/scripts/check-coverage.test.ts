import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'bun:test';

const coverageScript = fileURLToPath(new URL('./check-coverage.ts', import.meta.url));
const coverageFiles = [
  'packages/contracts/coverage/lcov.info',
  'packages/utils/coverage/lcov.info',
  'packages/api-clients/coverage/lcov.info',
  'extensions/shikiho/coverage/lcov.info',
  'packages/web/coverage/lcov.info',
];
const validLcov = `TN:
SF:src/example.ts
FNF:10
FNH:9
LF:10
LH:9
end_of_record
`;

const tempDirs: string[] = [];

async function runCoverageGate(override: string): Promise<{
  exitCode: number;
  stderr: string;
  stdout: string;
}> {
  const root = await mkdtemp(join(tmpdir(), 'coverage-gate-'));
  tempDirs.push(root);

  for (const relativePath of coverageFiles) {
    const path = join(root, relativePath);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, relativePath === coverageFiles[0] ? override : validLcov);
  }

  const child = Bun.spawn([process.execPath, coverageScript], {
    cwd: root,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { exitCode, stdout, stderr };
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe('check-coverage LCOV validation', () => {
  it('accepts complete finite LCOV records that meet thresholds', async () => {
    const result = await runCoverageGate(validLcov);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('Coverage gate passed for all packages.');
    expect(result.stderr).toBe('');
  });

  const invalidCases: Array<[string, string]> = [
    ['whitespace-only input', '  \n\t'],
    ['input without a source record', 'TN:empty\n'],
    ['malformed numeric totals', `${validLcov.replace('LF:10', 'LF:not-a-number')}`],
    ['NaN totals', `${validLcov.replace('LF:10', 'LF:NaN')}`],
    ['non-finite totals', `${validLcov.replace('LF:10', 'LF:Infinity')}`],
    ['negative totals', `${validLcov.replace('LF:10', 'LF:-10').replace('LH:9', 'LH:-9')}`],
    ['line hits greater than lines found', `${validLcov.replace('LH:9', 'LH:11')}`],
    ['function hits greater than functions found', `${validLcov.replace('FNH:9', 'FNH:11')}`],
    ['missing required summary totals', `${validLcov.replace('FNH:9\n', '')}`],
  ];

  for (const [name, content] of invalidCases) {
    it(`rejects ${name}`, async () => {
      const result = await runCoverageGate(content);

      expect(result.exitCode).toBe(1);
      expect(result.stderr).toContain('invalid LCOV data');
      expect(result.stdout).not.toContain('Coverage gate passed for all packages.');
    });
  }
});
