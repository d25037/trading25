import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

export type DependencySection = 'dependencies' | 'devDependencies';

export interface PackageManifest {
  name?: string;
  private?: boolean;
  scripts?: Record<string, string>;
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
  overrides?: Record<string, string>;
}

export interface DependencyAuditIssue {
  kind: 'missing-dependency' | 'override-version-drift' | 'unused-dependency';
  manifestPath: string;
  packageName: string;
  detail: string;
  section?: DependencySection;
}

export interface ManifestAuditConfig {
  manifestPath: string;
  scanGlobs: string[];
  allowUnused?: Record<string, string>;
  allowMissing?: Record<string, string>;
}

const IMPORT_PATTERNS = [
  /(?:import|export)\s+(?:type\s+)?(?:[^'"]*?\s+from\s+)?["']([^"']+)["']/g,
  /import\(\s*["']([^"']+)["']\s*\)/g,
  /require\(\s*["']([^"']+)["']\s*\)/g,
] as const;

const SCRIPT_PACKAGE_PATTERNS = [
  { pattern: /\bbiome\b/, packageName: '@biomejs/biome' },
  { pattern: /\bopenapi-typescript\b/, packageName: 'openapi-typescript' },
  { pattern: /\bplaywright\b/, packageName: '@playwright/test' },
  { pattern: /\btsc\b/, packageName: 'typescript' },
  { pattern: /\btsx\b/, packageName: 'tsx' },
  { pattern: /\bvite\b/, packageName: 'vite' },
  { pattern: /\bvitest\b/, packageName: 'vitest' },
] as const;

function isIgnorableSpecifier(specifier: string): boolean {
  return (
    specifier.startsWith('.') ||
    specifier.startsWith('/') ||
    specifier.startsWith('@/') ||
    specifier.startsWith('~/') ||
    specifier.startsWith('#') ||
    specifier.startsWith('node:') ||
    specifier.startsWith('bun:') ||
    specifier.startsWith('virtual:')
  );
}

function isTypePackage(packageName: string): boolean {
  return packageName.startsWith('@types/');
}

function getDeclaredDependencies(manifest: PackageManifest): Map<string, DependencySection> {
  const declared = new Map<string, DependencySection>();

  for (const packageName of Object.keys(manifest.dependencies ?? {})) {
    declared.set(packageName, 'dependencies');
  }
  for (const packageName of Object.keys(manifest.devDependencies ?? {})) {
    declared.set(packageName, 'devDependencies');
  }

  return declared;
}

export function normalizePackageSpecifier(specifier: string): string | null {
  if (isIgnorableSpecifier(specifier)) {
    return null;
  }

  if (specifier.startsWith('@')) {
    const [scope, name] = specifier.split('/');
    return scope && name ? `${scope}/${name}` : null;
  }

  const [name] = specifier.split('/');
  return name || null;
}

export function collectImportedPackagesFromText(text: string): Set<string> {
  const packages = new Set<string>();

  for (const pattern of IMPORT_PATTERNS) {
    for (const match of text.matchAll(pattern)) {
      const specifier = match[1];
      if (!specifier) {
        continue;
      }
      const normalized = normalizePackageSpecifier(specifier);
      if (normalized) {
        packages.add(normalized);
      }
    }
  }

  return packages;
}

export function collectScriptPackages(scripts: Record<string, string> | undefined): Set<string> {
  const packages = new Set<string>();

  if (!scripts) {
    return packages;
  }

  for (const command of Object.values(scripts)) {
    for (const entry of SCRIPT_PACKAGE_PATTERNS) {
      if (entry.pattern.test(command)) {
        packages.add(entry.packageName);
      }
    }
    if (command.includes('--coverage')) {
      packages.add('@vitest/coverage-v8');
    }
    if (command.includes('--ui')) {
      packages.add('@vitest/ui');
    }
  }

  return packages;
}

export function collectUsedPackages(params: {
  fileTexts: string[];
  scripts?: Record<string, string>;
}): Set<string> {
  const packages = collectScriptPackages(params.scripts);

  for (const text of params.fileTexts) {
    for (const packageName of collectImportedPackagesFromText(text)) {
      packages.add(packageName);
    }
  }

  return packages;
}

export function auditManifest(params: {
  manifestPath: string;
  manifest: PackageManifest;
  usedPackages: Set<string>;
  rootOverrides?: Record<string, string>;
  allowUnused?: Record<string, string>;
  allowMissing?: Record<string, string>;
}): DependencyAuditIssue[] {
  const issues: DependencyAuditIssue[] = [];
  const declared = getDeclaredDependencies(params.manifest);

  for (const [packageName, section] of declared.entries()) {
    const declaredVersion =
      section === 'dependencies'
        ? params.manifest.dependencies?.[packageName]
        : params.manifest.devDependencies?.[packageName];
    const overrideVersion = params.rootOverrides?.[packageName];

    if (declaredVersion && overrideVersion && declaredVersion !== overrideVersion) {
      issues.push({
        kind: 'override-version-drift',
        manifestPath: params.manifestPath,
        packageName,
        section,
        detail: `declares ${declaredVersion} but root override pins ${overrideVersion}`,
      });
    }

    if (
      !params.usedPackages.has(packageName) &&
      !params.allowUnused?.[packageName] &&
      !isTypePackage(packageName)
    ) {
      issues.push({
        kind: 'unused-dependency',
        manifestPath: params.manifestPath,
        packageName,
        section,
        detail: `${section} declares ${packageName} but no matching import or script usage was found`,
      });
    }
  }

  for (const packageName of params.usedPackages) {
    if (!declared.has(packageName) && !params.allowMissing?.[packageName]) {
      issues.push({
        kind: 'missing-dependency',
        manifestPath: params.manifestPath,
        packageName,
        detail: `imports or scripts use ${packageName} but it is not declared in dependencies/devDependencies`,
      });
    }
  }

  return issues.sort((left, right) => {
    const manifestCompare = left.manifestPath.localeCompare(right.manifestPath);
    if (manifestCompare !== 0) {
      return manifestCompare;
    }
    return left.packageName.localeCompare(right.packageName);
  });
}

export async function readWorkspaceManifest(rootDir: string, manifestPath: string): Promise<PackageManifest> {
  const manifestText = await readFile(resolve(rootDir, manifestPath), 'utf8');
  return JSON.parse(manifestText) as PackageManifest;
}

export async function readGlobbedFileTexts(rootDir: string, scanGlobs: string[]): Promise<string[]> {
  const texts: string[] = [];

  for (const pattern of scanGlobs) {
    for await (const path of new Bun.Glob(pattern).scan({ cwd: rootDir })) {
      texts.push(await readFile(resolve(rootDir, path), 'utf8'));
    }
  }

  return texts;
}

export function formatDependencyAuditIssues(issues: DependencyAuditIssue[]): string {
  return issues
    .map((issue) => {
      const sectionLabel = issue.section ? ` (${issue.section})` : '';
      return `- ${issue.manifestPath}: ${issue.packageName}${sectionLabel} -> ${issue.detail}`;
    })
    .join('\n');
}
