import { rm } from 'node:fs/promises';
import { resolve } from 'node:path';

type TaskStep =
  | {
      task: string;
      optional?: boolean;
      warning?: string;
    }
  | {
      command: string[];
      withEnvFile?: boolean;
    }
  | {
      shell: string;
    };

type TaskDefinition = {
  description: string;
  steps: TaskStep[];
};

const ROOT = resolve(import.meta.dir, '..');
const ENV_FILE = '../../.env';

const TASKS: Record<string, TaskDefinition> = {
  'api:hint': {
    description: 'Show backend startup hint',
    steps: [{ shell: "echo 'Use: uv run bt server --port 3002'" }],
  },
  'apps:test': {
    description: 'Run app tests (cli + web)',
    steps: [
      { command: ['run', '--filter', '@trading25/cli', 'test'] },
      { command: ['run', '--filter', '@trading25/web', 'test'] },
    ],
  },
  'cli:build': {
    description: 'Build CLI package',
    steps: [{ command: ['run', '--filter', '@trading25/cli', 'build'] }],
  },
  'cli:dev': {
    description: 'Run CLI in watch mode',
    steps: [{ command: ['run', '--filter', '@trading25/cli', 'dev'], withEnvFile: true }],
  },
  'cli:run': {
    description: 'Run CLI entrypoint',
    steps: [{ command: ['packages/cli/src/index.ts'], withEnvFile: true }],
  },
  'clients:build': {
    description: 'Build clients-ts package',
    steps: [{ command: ['run', '--filter', '@trading25/clients-ts', 'build'] }],
  },
  'core:test': {
    description: 'Run package tests + CLI tests',
    steps: [
      { task: 'packages:test' },
      { command: ['run', '--filter', '@trading25/cli', 'test'] },
    ],
  },
  'coverage:check': {
    description: 'Check coverage thresholds',
    steps: [{ command: ['scripts/check-coverage.ts'] }],
  },
  'packages:test': {
    description: 'Run package tests (shared + clients-ts)',
    steps: [
      { command: ['run', '--filter', '@trading25/shared', 'test'] },
      { command: ['run', '--filter', '@trading25/clients-ts', 'test'] },
    ],
  },
  'quality:check:fix': {
    description: 'Run biome check with auto-fix',
    steps: [{ command: ['x', 'biome', 'check', '.', '--write'] }],
  },
  'quality:format': {
    description: 'Run biome format',
    steps: [{ command: ['x', 'biome', 'format'] }],
  },
  'quality:lint': {
    description: 'Run biome lint',
    steps: [{ command: ['x', 'biome', 'lint'] }],
  },
  'quality:lint:fix': {
    description: 'Run biome lint with auto-fix',
    steps: [{ command: ['x', 'biome', 'lint', '.', '--write'] }],
  },
  'quality:typecheck': {
    description: 'Run full typecheck (root + clients-ts + web)',
    steps: [
      { task: 'quality:typecheck:root' },
      { command: ['run', '--filter', '@trading25/clients-ts', 'typecheck'] },
      { task: 'quality:typecheck:web' },
    ],
  },
  'quality:typecheck:root': {
    description: 'Run root TypeScript typecheck',
    steps: [{ command: ['x', 'tsc', '--noEmit'] }],
  },
  'quality:typecheck:web': {
    description: 'Run web TypeScript typecheck',
    steps: [{ command: ['run', '--filter', '@trading25/web', 'typecheck'] }],
  },
  'shared:build': {
    description: 'Build shared package',
    steps: [{ command: ['run', '--filter', '@trading25/shared', 'build'] }],
  },
  'shared:sync:bt': {
    description: 'Sync bt OpenAPI schema and generated types',
    steps: [{ command: ['run', '--filter', '@trading25/shared', 'bt:sync'] }],
  },
  'web:build': {
    description: 'Build web package',
    steps: [{ command: ['run', '--filter', '@trading25/web', 'build'] }],
  },
  'web:dev': {
    description: 'Run web dev server',
    steps: [{ command: ['run', '--filter', '@trading25/web', 'dev'], withEnvFile: true }],
  },
  'web:test': {
    description: 'Run web tests',
    steps: [{ command: ['run', '--filter', '@trading25/web', 'test'] }],
  },
  'workspace:build': {
    description: 'Build all workspace packages',
    steps: [{ task: 'clients:build' }, { task: 'shared:build' }, { task: 'web:build' }, { task: 'cli:build' }],
  },
  'workspace:clean': {
    description: 'Clean dist/node_modules and lock files',
    steps: [{ shell: 'workspace-clean' }],
  },
  'workspace:dev': {
    description: 'Run default development target (web)',
    steps: [{ task: 'web:dev' }],
  },
  'workspace:dev:sync': {
    description: 'Run bt sync then web dev (continue on sync failure)',
    steps: [
      {
        task: 'shared:sync:bt',
        optional: true,
        warning: '[WARNING] bt:sync failed - continuing with existing types',
      },
      { task: 'web:dev' },
    ],
  },
  'workspace:test': {
    description: 'Run all workspace tests',
    steps: [{ task: 'packages:test' }, { task: 'apps:test' }],
  },
  'workspace:test:coverage': {
    description: 'Run all workspace coverage tests',
    steps: [
      { command: ['run', '--filter', '@trading25/shared', 'test:coverage'] },
      { command: ['run', '--filter', '@trading25/clients-ts', 'test:coverage'] },
      { command: ['run', '--filter', '@trading25/cli', 'test:coverage'] },
      { command: ['run', '--filter', '@trading25/web', 'test:coverage'] },
    ],
  },
};

async function runBun(args: string[], options: { withEnvFile?: boolean } = {}): Promise<number> {
  const bunArgs = options.withEnvFile ? ['--env-file=' + ENV_FILE, ...args] : args;
  const proc = Bun.spawn({
    cmd: ['bun', ...bunArgs],
    cwd: ROOT,
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });
  return proc.exited;
}

async function runShell(command: string): Promise<number> {
  if (command === 'workspace-clean') {
    await Promise.all([
      rm(resolve(ROOT, 'packages/cli/dist'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/clients-ts/dist'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/shared/dist'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/web/dist'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/cli/node_modules'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/clients-ts/node_modules'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/shared/node_modules'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'packages/web/node_modules'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'node_modules'), { recursive: true, force: true }),
      rm(resolve(ROOT, 'pnpm-lock.yaml'), { force: true }),
      rm(resolve(ROOT, 'bun.lockb'), { force: true }),
    ]);
    return 0;
  }

  const proc = Bun.spawn({
    cmd: ['/bin/zsh', '-lc', command],
    cwd: ROOT,
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });
  return proc.exited;
}

async function runTask(taskName: string): Promise<number> {
  const definition = TASKS[taskName];
  if (!definition) {
    return 2;
  }

  for (const step of definition.steps) {
    if ('task' in step) {
      const code = await runTask(step.task);
      if (code !== 0) {
        if (step.optional) {
          if (step.warning) {
            console.warn(step.warning);
          }
          continue;
        }
        return code;
      }
      continue;
    }

    if ('command' in step) {
      const code = await runBun(step.command, { withEnvFile: step.withEnvFile });
      if (code !== 0) {
        return code;
      }
      continue;
    }

    const code = await runShell(step.shell);
    if (code !== 0) {
      return code;
    }
  }

  return 0;
}

function printUsage(): void {
  console.error('Usage: bun scripts/tasks.ts <task>');
  console.error('\nAvailable tasks:');
  const lines = Object.entries(TASKS)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, def]) => `  - ${name}: ${def.description}`);
  for (const line of lines) {
    console.error(line);
  }
}

async function main(): Promise<void> {
  const task = process.argv[2];
  if (!task) {
    printUsage();
    process.exit(2);
  }

  const exitCode = await runTask(task);
  if (exitCode === 2 && !TASKS[task]) {
    console.error(`Unknown task: ${task}`);
    printUsage();
  }
  process.exit(exitCode);
}

if (import.meta.main) {
  await main();
}
