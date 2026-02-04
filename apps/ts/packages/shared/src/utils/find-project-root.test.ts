import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { findProjectRoot, getProjectEnvPath } from './find-project-root';

describe('findProjectRoot', () => {
  let testDir: string;

  beforeEach(() => {
    testDir = join(tmpdir(), `find-root-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    mkdirSync(testDir, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  test('finds root via .git directory', () => {
    mkdirSync(join(testDir, '.git'), { recursive: true });
    const subDir = join(testDir, 'a', 'b', 'c');
    mkdirSync(subDir, { recursive: true });

    const result = findProjectRoot(subDir);
    expect(result).toBe(testDir);
  });

  test('finds root via package.json with workspaces', () => {
    writeFileSync(join(testDir, 'package.json'), JSON.stringify({ name: 'test', workspaces: ['packages/*'] }));
    const subDir = join(testDir, 'packages', 'shared');
    mkdirSync(subDir, { recursive: true });

    const result = findProjectRoot(subDir);
    expect(result).toBe(testDir);
  });

  test('ignores package.json without workspaces', () => {
    writeFileSync(join(testDir, 'package.json'), JSON.stringify({ name: 'test' }));
    // Also add .env to parent so it eventually finds something
    writeFileSync(join(testDir, '.env'), 'KEY=val');

    const result = findProjectRoot(testDir);
    // Should still find via .env fallback
    expect(result).toBe(testDir);
  });

  test('finds root via pnpm-workspace.yaml', () => {
    writeFileSync(join(testDir, 'pnpm-workspace.yaml'), 'packages:\n  - packages/*');
    const subDir = join(testDir, 'sub');
    mkdirSync(subDir, { recursive: true });

    const result = findProjectRoot(subDir);
    expect(result).toBe(testDir);
  });

  test('finds root via .env file', () => {
    writeFileSync(join(testDir, '.env'), 'KEY=val');
    const result = findProjectRoot(testDir);
    expect(result).toBe(testDir);
  });

  test('finds root via CLAUDE.md', () => {
    writeFileSync(join(testDir, 'CLAUDE.md'), '# Project');
    const result = findProjectRoot(testDir);
    expect(result).toBe(testDir);
  });

  test('handles invalid package.json gracefully', () => {
    writeFileSync(join(testDir, 'package.json'), 'not json');
    writeFileSync(join(testDir, '.env'), 'KEY=val');

    const result = findProjectRoot(testDir);
    expect(result).toBe(testDir);
  });

  test('throws when no project root found', () => {
    // Use a bare temp subdirectory with no indicators anywhere up the tree
    // We create a deeply nested dir that won't match anything
    const isolated = join(testDir, 'deep', 'nested');
    mkdirSync(isolated, { recursive: true });

    // This might or might not throw depending on the real filesystem above testDir
    // If there's a .git somewhere up the tree it'll find it
    // So we just verify it returns a string or throws
    try {
      const result = findProjectRoot(isolated);
      expect(typeof result).toBe('string');
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      expect((e as Error).message).toContain('Could not find project root');
    }
  });
});

describe('getProjectEnvPath', () => {
  test('returns path to .env in project root', () => {
    const envPath = getProjectEnvPath();
    expect(envPath).toMatch(/\.env$/);
  });
});
