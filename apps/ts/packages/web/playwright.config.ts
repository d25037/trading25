import { defineConfig, devices } from '@playwright/test';
import { fileURLToPath } from 'node:url';

const isCI = !!process.env.CI;
const webCwd = fileURLToPath(new URL('.', import.meta.url));
const webPort = Number(process.env.PLAYWRIGHT_WEB_PORT ?? '4173');
const webHost = process.env.PLAYWRIGHT_WEB_HOST ?? '127.0.0.1';
const webServerTimeout = Number(process.env.PLAYWRIGHT_WEB_SERVER_TIMEOUT_MS ?? (isCI ? '180000' : '120000'));
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? `http://${webHost}:${webPort}`;
const skipWebServer = process.env.PLAYWRIGHT_SKIP_WEBSERVER === '1';

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  forbidOnly: isCI,
  retries: isCI ? 1 : 0,
  workers: isCI ? 1 : undefined,
  reporter: isCI ? [['github'], ['html', { open: 'never' }]] : [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  ...(skipWebServer
    ? {}
    : {
        webServer: {
          command: 'bun run playwright:web-server',
          url: baseURL,
          reuseExistingServer: !isCI,
          cwd: webCwd,
          timeout: webServerTimeout + 10_000,
        },
      }),
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
