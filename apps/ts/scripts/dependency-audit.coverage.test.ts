import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  auditManifest,
  collectImportedPackagesFromText,
  collectScriptPackages,
  formatDependencyAuditIssues,
  normalizePackageSpecifier,
  readGlobbedFileTexts,
  readWorkspaceManifest,
} from './dependency-audit-lib';

const tempDirs: string[] = [];

async function createTempWorkspace(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dependency-audit-'));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe('dependency-audit-lib coverage', () => {
  it('normalizes package specifiers and ignores local or runtime-specific imports', () => {
    expect(normalizePackageSpecifier('./local-file')).toBeNull();
    expect(normalizePackageSpecifier('/absolute/path')).toBeNull();
    expect(normalizePackageSpecifier('@/components/Button')).toBeNull();
    expect(normalizePackageSpecifier('~/lib/runtime')).toBeNull();
    expect(normalizePackageSpecifier('#internal')).toBeNull();
    expect(normalizePackageSpecifier('node:fs')).toBeNull();
    expect(normalizePackageSpecifier('bun:test')).toBeNull();
    expect(normalizePackageSpecifier('virtual:generated')).toBeNull();
    expect(normalizePackageSpecifier('@scope/pkg/subpath')).toBe('@scope/pkg');
    expect(normalizePackageSpecifier('react/jsx-runtime')).toBe('react');
  });

  it('collects imported packages and script-only tooling packages', () => {
    const sourceText = `
      import React from 'react';
      export { QueryClient } from '@tanstack/react-query';
      const store = require('zustand/traditional');
      const editor = import('@monaco-editor/react');
      import '@/local-alias';
      import '#internal';
    `;

    expect(collectImportedPackagesFromText(sourceText)).toEqual(
      new Set(['react', '@tanstack/react-query', 'zustand', '@monaco-editor/react'])
    );

    expect(
      collectScriptPackages({
        lint: 'biome check .',
        sync: 'openapi-typescript schema.json -o types.ts',
        typecheck: 'tsc --noEmit',
        dev: 'vite --host',
        test: 'vitest run --coverage',
        'test:ui': 'vitest --ui',
        e2e: 'playwright test',
        runner: 'tsx scripts/run.ts',
      })
    ).toEqual(
      new Set([
        '@biomejs/biome',
        'openapi-typescript',
        'typescript',
        'vite',
        'vitest',
        '@vitest/coverage-v8',
        '@vitest/ui',
        '@playwright/test',
        'tsx',
      ])
    );
  });

  it('reports missing, unused, and override-drift issues while honoring allowlists and type packages', () => {
    const issues = auditManifest({
      manifestPath: 'packages/web/package.json',
      manifest: {
        dependencies: {
          react: '^19.2.4',
          '@scope/kept': '^1.0.0',
          'monaco-editor': '0.55.1',
        },
        devDependencies: {
          vitest: '^4.1.0',
          '@types/node': '^24.0.0',
        },
      },
      usedPackages: new Set(['react', 'vitest', 'missing-package']),
      rootOverrides: {
        'monaco-editor': '0.53.0',
      },
      allowUnused: {
        '@scope/kept': 'kept for peer resolution',
      },
      allowMissing: {
        'missing-package': 'provided by parent workspace',
      },
    });

    expect(issues).toEqual([
      expect.objectContaining({
        kind: 'override-version-drift',
        packageName: 'monaco-editor',
        section: 'dependencies',
      }),
      expect.objectContaining({
        kind: 'unused-dependency',
        packageName: 'monaco-editor',
        section: 'dependencies',
      }),
    ]);
  });

  it('detects undeclared usage when no allowMissing rule is present', () => {
    const issues = auditManifest({
      manifestPath: 'packages/utils/package.json',
      manifest: {
        dependencies: {
          react: '^19.2.4',
        },
      },
      usedPackages: new Set(['react', 'zod']),
    });

    expect(issues).toEqual([
      expect.objectContaining({
        kind: 'missing-dependency',
        packageName: 'zod',
      }),
    ]);
  });

  it('reads workspace manifests and globbed source files', async () => {
    const rootDir = await createTempWorkspace();
    const originalBun = (globalThis as { Bun?: unknown }).Bun;

    await mkdir(join(rootDir, 'src', 'nested'), { recursive: true });
    await writeFile(
      join(rootDir, 'package.json'),
      JSON.stringify({
        name: '@trading25/example',
        scripts: {
          test: 'vitest run',
        },
      })
    );
    await writeFile(join(rootDir, 'src', 'first.ts'), "import React from 'react';\n");
    await writeFile(join(rootDir, 'src', 'nested', 'second.ts'), "export * from 'zod';\n");

    await expect(readWorkspaceManifest(rootDir, 'package.json')).resolves.toEqual({
      name: '@trading25/example',
      scripts: {
        test: 'vitest run',
      },
    });

    Reflect.set(globalThis as object, 'Bun', {
      Glob: class MockGlob {
        constructor(private pattern: string) {}

        async *scan({ cwd }: { cwd: string }): AsyncIterable<string> {
          if (cwd !== rootDir || this.pattern !== 'src/**/*.ts') {
            return;
          }
          yield 'src/first.ts';
          yield 'src/nested/second.ts';
        }
      },
    });

    try {
      await expect(readGlobbedFileTexts(rootDir, ['src/**/*.ts'])).resolves.toEqual(
        expect.arrayContaining(["import React from 'react';\n", "export * from 'zod';\n"])
      );
    } finally {
      Reflect.set(globalThis as object, 'Bun', originalBun);
    }
  });

  it('formats issues into a stable terminal-friendly summary', () => {
    expect(
      formatDependencyAuditIssues([
        {
          kind: 'unused-dependency',
          manifestPath: 'packages/web/package.json',
          packageName: 'zustand',
          section: 'dependencies',
          detail: 'dependencies declares zustand but no matching import or script usage was found',
        },
        {
          kind: 'missing-dependency',
          manifestPath: 'packages/utils/package.json',
          packageName: 'zod',
          detail: 'imports or scripts use zod but it is not declared in dependencies/devDependencies',
        },
      ])
    ).toBe(
      '- packages/web/package.json: zustand (dependencies) -> dependencies declares zustand but no matching import or script usage was found\n' +
        '- packages/utils/package.json: zod -> imports or scripts use zod but it is not declared in dependencies/devDependencies'
    );
  });
});
