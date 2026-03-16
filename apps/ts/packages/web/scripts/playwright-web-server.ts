import { spawn, type ChildProcess } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

const DEFAULT_WEB_PORT = 4173;
const DEFAULT_WEB_HOST = '127.0.0.1';
const DEFAULT_TIMEOUT_MS = process.env.CI ? 180_000 : 120_000;
const DEFAULT_POLL_INTERVAL_MS = 1_000;
const STARTUP_REQUEST_TIMEOUT_MS = 2_000;
const STARTUP_LOG_INTERVAL_MS = 10_000;

type ChildExitState = {
  code: number | null;
  signal: NodeJS.Signals | null;
};

function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function formatChildExitState(state: ChildExitState): string {
  if (state.code !== null) {
    return `exit code ${state.code}`;
  }
  if (state.signal !== null) {
    return `signal ${state.signal}`;
  }
  return 'unknown termination';
}

async function isServerReady(url: URL): Promise<boolean> {
  try {
    const response = await fetch(url, {
      headers: {
        accept: 'text/html',
      },
      signal: AbortSignal.timeout(STARTUP_REQUEST_TIMEOUT_MS),
    });
    return response.status < 500;
  } catch {
    return false;
  }
}

async function waitForStartup(
  url: URL,
  timeoutMs: number,
  pollIntervalMs: number,
  getChildExitState: () => ChildExitState | null
): Promise<void> {
  const startedAt = Date.now();
  let nextLogAt = 0;

  while (Date.now() - startedAt < timeoutMs) {
    const childExitState = getChildExitState();
    if (childExitState !== null) {
      throw new Error(`Vite exited before becoming ready with ${formatChildExitState(childExitState)}.`);
    }

    if (await isServerReady(url)) {
      console.log(`[playwright-web-server] ready at ${url.toString()} after ${Date.now() - startedAt}ms`);
      return;
    }

    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= nextLogAt) {
      const secondsLeft = Math.max(0, Math.ceil((timeoutMs - elapsedMs) / 1_000));
      console.log(`[playwright-web-server] waiting for ${url.toString()} (${secondsLeft}s left)`);
      nextLogAt += STARTUP_LOG_INTERVAL_MS;
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${url.toString()}.`);
}

async function stopChild(processHandle: ChildProcess, signal: NodeJS.Signals): Promise<void> {
  if (processHandle.exitCode !== null || processHandle.signalCode !== null) {
    return;
  }

  processHandle.kill(signal);
  await sleep(250);
}

const webPort = parsePositiveInteger(process.env.PLAYWRIGHT_WEB_PORT, DEFAULT_WEB_PORT);
const webHost = process.env.PLAYWRIGHT_WEB_HOST ?? DEFAULT_WEB_HOST;
const startupTimeoutMs = parsePositiveInteger(
  process.env.PLAYWRIGHT_WEB_SERVER_TIMEOUT_MS,
  DEFAULT_TIMEOUT_MS
);
const pollIntervalMs = parsePositiveInteger(
  process.env.PLAYWRIGHT_WEB_SERVER_POLL_INTERVAL_MS,
  DEFAULT_POLL_INTERVAL_MS
);
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? `http://${webHost}:${webPort}`;
const readyUrl = new URL(baseURL);

const viteCommand = ['bunx', '--bun', 'vite', '--configLoader', 'native', '--port', String(webPort), '--host', webHost];

console.log(`[playwright-web-server] starting ${viteCommand.join(' ')} in ${process.cwd()}`);

const viteProcess = spawn(viteCommand[0], viteCommand.slice(1), {
  cwd: process.cwd(),
  env: process.env,
  stdio: 'inherit',
});

let childExitState: ChildExitState | null = null;
const childExitPromise = new Promise<ChildExitState>((resolve) => {
  viteProcess.once('exit', (code, signal) => {
    childExitState = { code, signal };
    resolve(childExitState);
  });
});

const shutdownSignals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM'];
for (const signal of shutdownSignals) {
  process.on(signal, () => {
    void stopChild(viteProcess, signal).finally(() => {
      process.exit(0);
    });
  });
}

try {
  await waitForStartup(readyUrl, startupTimeoutMs, pollIntervalMs, () => childExitState);
  const finalState = await childExitPromise;
  process.exit(finalState.code ?? 0);
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[playwright-web-server] ${message}`);
  await stopChild(viteProcess, 'SIGTERM');
  await Promise.race([childExitPromise, sleep(1_000)]);
  process.exit(1);
}
