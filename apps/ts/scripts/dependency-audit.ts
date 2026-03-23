import { resolve } from 'node:path';
import {
  auditManifest,
  collectUsedPackages,
  formatDependencyAuditIssues,
  readGlobbedFileTexts,
  readWorkspaceManifest,
  type ManifestAuditConfig,
} from './dependency-audit-lib';

const ROOT = resolve(import.meta.dir, '..');
const ROOT_TOOLING_ALLOW_MISSING = {
  '@biomejs/biome': 'shared root tooling',
  '@vitest/coverage-v8': 'shared root coverage provider',
  bun: 'bun runtime import',
  typescript: 'shared root tooling',
} as const;

const AUDIT_CONFIGS: ManifestAuditConfig[] = [
  {
    manifestPath: 'package.json',
    scanGlobs: ['scripts/check-coverage.ts', 'scripts/dependency-audit-lib.ts', 'scripts/dependency-audit.ts', 'scripts/tasks.ts'],
    allowUnused: {
      '@biomejs/biome': 'task runner shell command',
      'bun-types': 'TypeScript ambient runtime types',
      'typescript': 'task runner shell command',
    },
  },
  {
    manifestPath: 'packages/api-clients/package.json',
    scanGlobs: ['packages/api-clients/src/**/*.ts', 'packages/api-clients/src/**/*.test.ts'],
    allowMissing: ROOT_TOOLING_ALLOW_MISSING,
  },
  {
    manifestPath: 'packages/contracts/package.json',
    scanGlobs: [
      'packages/contracts/src/**/*.ts',
      'packages/contracts/scripts/**/*.ts',
      'packages/contracts/src/**/*.test.ts',
      'packages/contracts/scripts/**/*.test.ts',
    ],
    allowMissing: ROOT_TOOLING_ALLOW_MISSING,
  },
  {
    manifestPath: 'packages/utils/package.json',
    scanGlobs: ['packages/utils/src/**/*.ts', 'packages/utils/src/**/*.test.ts'],
    allowMissing: ROOT_TOOLING_ALLOW_MISSING,
  },
  {
    manifestPath: 'packages/web/package.json',
    scanGlobs: [
      'packages/web/src/**/*.ts',
      'packages/web/src/**/*.tsx',
      'packages/web/e2e/**/*.ts',
      'packages/web/playwright.config.ts',
      'packages/web/vitest.config.ts',
      'packages/web/vite.config.ts',
    ],
    allowUnused: {
      'happy-dom': 'Vitest environment string configuration',
      'monaco-editor': '@monaco-editor/react peer/runtime dependency',
      'tailwindcss': 'Tailwind v4 runtime package consumed by Vite plugin',
    },
    allowMissing: ROOT_TOOLING_ALLOW_MISSING,
  },
];

async function main(): Promise<void> {
  const rootManifest = await readWorkspaceManifest(ROOT, 'package.json');
  const rootOverrides = rootManifest.overrides ?? {};
  const issues = [];

  for (const config of AUDIT_CONFIGS) {
    const manifest = await readWorkspaceManifest(ROOT, config.manifestPath);
    const fileTexts = await readGlobbedFileTexts(ROOT, config.scanGlobs);
    const usedPackages = collectUsedPackages({
      fileTexts,
      scripts: manifest.scripts,
    });

    issues.push(
      ...auditManifest({
        manifestPath: config.manifestPath,
        manifest,
        usedPackages,
        rootOverrides,
        allowUnused: config.allowUnused,
        allowMissing: config.allowMissing,
      })
    );
  }

  if (issues.length > 0) {
    console.error('Dependency audit failed.');
    console.error(formatDependencyAuditIssues(issues));
    process.exit(1);
  }

  console.log(`Dependency audit passed (${AUDIT_CONFIGS.length} manifests checked).`);
}

await main();
