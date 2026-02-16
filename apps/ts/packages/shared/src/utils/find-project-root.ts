import * as fs from 'node:fs';
import * as path from 'node:path';

const reportedInvalidPackageJsonPaths = new Set<string>();

function reportProjectIndicatorParseError(packageJsonPath: string, error: unknown): void {
  if (process.env.NODE_ENV === 'test' || reportedInvalidPackageJsonPaths.has(packageJsonPath)) {
    return;
  }

  reportedInvalidPackageJsonPaths.add(packageJsonPath);
  console.warn(
    `[findProjectRoot] Ignoring invalid package.json at ${packageJsonPath}: ${error instanceof Error ? error.message : String(error)}`
  );
}

/**
 * Check if directory contains project indicators
 */
function checkForProjectIndicators(dir: string): boolean {
  // Primary indicator: .git directory (most reliable)
  if (fs.existsSync(path.join(dir, '.git'))) {
    return true;
  }

  // Secondary indicator: package.json with workspaces (monorepo root)
  const packageJsonPath = path.join(dir, 'package.json');
  if (fs.existsSync(packageJsonPath)) {
    try {
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));
      if (packageJson.workspaces) {
        return true;
      }
    } catch (error) {
      reportProjectIndicatorParseError(packageJsonPath, error);
      // Continue searching if package.json is invalid
    }
  }

  // Tertiary indicator: pnpm-workspace.yaml (pnpm monorepo)
  if (fs.existsSync(path.join(dir, 'pnpm-workspace.yaml'))) {
    return true;
  }

  // Fallback indicators (less reliable in CI/Docker environments)
  const envPath = path.join(dir, '.env');
  const claudeMdPath = path.join(dir, 'CLAUDE.md');
  return fs.existsSync(envPath) || fs.existsSync(claudeMdPath);
}

/**
 * Find the project root directory by traversing up the directory tree
 * Looks for multiple indicators in order of reliability
 */
export function findProjectRoot(startDir: string = process.cwd()): string {
  let currentDir = path.resolve(startDir);

  while (currentDir !== path.dirname(currentDir)) {
    if (checkForProjectIndicators(currentDir)) {
      return currentDir;
    }

    // Move up one directory
    currentDir = path.dirname(currentDir);
  }

  throw new Error(
    `Could not find project root starting from ${startDir}. Looked for .git, package.json with workspaces, pnpm-workspace.yaml, .env, or CLAUDE.md`
  );
}

/**
 * Get the path to the .env file in the project root
 */
export function getProjectEnvPath(startDir?: string): string {
  const projectRoot = findProjectRoot(startDir);
  return path.join(projectRoot, '.env');
}
