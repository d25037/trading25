import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { describe, expect, it } from 'bun:test';
import {
  auditManifest,
  collectImportedPackagesFromText,
  collectScriptPackages,
  collectUsedPackages,
  formatDependencyAuditIssues,
  normalizePackageSpecifier,
  readGlobbedFileTexts,
  readWorkspaceManifest,
} from './dependency-audit-lib';

describe('dependency-audit-lib', () => {
  it('normalizes package specifiers and ignores local aliases', () => {
    expect(normalizePackageSpecifier('@/components/Button')).toBeNull();
    expect(normalizePackageSpecifier('node:fs')).toBeNull();
    expect(normalizePackageSpecifier('@scope/pkg/subpath')).toBe('@scope/pkg');
    expect(normalizePackageSpecifier('react/jsx-runtime')).toBe('react');
  });

  it('collects imported and script-only packages', () => {
    const fileTexts = [
      `
        import { describe } from 'vitest';
        import { Link } from '@tanstack/react-router';
        const lazy = import('@monaco-editor/react');
      `,
    ];
    const sourceText = fileTexts[0]!;

    const usedPackages = collectUsedPackages({
      fileTexts,
      scripts: {
        test: 'vitest run --coverage',
        e2e: 'playwright test',
      },
    });

    expect(collectImportedPackagesFromText(sourceText)).toEqual(
      new Set(['vitest', '@tanstack/react-router', '@monaco-editor/react'])
    );
    expect(collectScriptPackages({ test: 'vitest run --coverage', e2e: 'playwright test' })).toEqual(
      new Set(['vitest', '@vitest/coverage-v8', '@playwright/test'])
    );
    expect(usedPackages).toEqual(
      new Set([
        'vitest',
        '@tanstack/react-router',
        '@monaco-editor/react',
        '@vitest/coverage-v8',
        '@playwright/test',
      ])
    );
  });

  it('handles empty scripts and vitest ui script detection', () => {
    expect(collectScriptPackages(undefined)).toEqual(new Set());
    expect(collectScriptPackages({ ui: 'vitest --ui' })).toEqual(new Set(['vitest', '@vitest/ui']));
  });

  it('reports unused dependencies and override version drift while honoring allowlists', () => {
    const issues = auditManifest({
      manifestPath: 'packages/web/package.json',
      manifest: {
        dependencies: {
          react: '^19.2.4',
          zustand: '^5.0.12',
          'monaco-editor': '0.55.1',
        },
        devDependencies: {
          vitest: '^4.1.0',
          '@vitest/ui': '^4.1.0',
        },
      },
      usedPackages: new Set(['react', 'vitest']),
      rootOverrides: { 'monaco-editor': '0.53.0' },
      allowUnused: { '@vitest/ui': 'script-only UI runner' },
    });

    expect(issues).toEqual([
      expect.objectContaining({
        kind: 'override-version-drift',
        packageName: 'monaco-editor',
      }),
      expect.objectContaining({
        kind: 'unused-dependency',
        packageName: 'monaco-editor',
      }),
      expect.objectContaining({
        kind: 'unused-dependency',
        packageName: 'zustand',
      }),
    ]);
  });

  it('reports missing dependencies while ignoring @types packages and formats issues', () => {
    const issues = auditManifest({
      manifestPath: 'packages/web/package.json',
      manifest: {
        devDependencies: {
          '@types/js-yaml': '^4.0.9',
        },
      },
      usedPackages: new Set(['react']),
    });

    expect(issues).toEqual([
      expect.objectContaining({
        kind: 'missing-dependency',
        packageName: 'react',
      }),
    ]);
    expect(formatDependencyAuditIssues(issues)).toContain(
      'packages/web/package.json: react -> imports or scripts use react but it is not declared'
    );
  });

  it('reads manifests and globbed source texts from disk', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'dependency-audit-'));

    try {
      await mkdir(resolve(tempRoot, 'src/nested'), { recursive: true });
      await writeFile(
        resolve(tempRoot, 'package.json'),
        JSON.stringify({
          name: 'fixture',
          dependencies: { react: '^19.2.4' },
        })
      );
      await writeFile(resolve(tempRoot, 'src/index.ts'), "import React from 'react';\n");
      await writeFile(resolve(tempRoot, 'src/nested/view.tsx'), "const lazy = import('@monaco-editor/react');\n");

      expect(await readWorkspaceManifest(tempRoot, 'package.json')).toEqual({
        name: 'fixture',
        dependencies: { react: '^19.2.4' },
      });

      const texts = await readGlobbedFileTexts(tempRoot, ['src/**/*.ts', 'src/**/*.tsx']);
      expect(texts).toHaveLength(2);
      expect(texts.some((text) => text.includes("import React from 'react'"))).toBe(true);
      expect(texts.some((text) => text.includes("@monaco-editor/react"))).toBe(true);
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  });
});
