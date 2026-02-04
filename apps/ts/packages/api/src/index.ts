import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { $ } from 'bun';

// Note: Bun automatically loads .env files from project root

import { OpenAPIHono } from '@hono/zod-openapi';
import { apiReference } from '@scalar/hono-api-reference';
import { logger } from '@trading25/shared/utils/logger';
import { serveStatic } from 'hono/bun';
import { cors } from 'hono/cors';
import { mountAllRoutes } from './app-routes';
import { errorHandler, requestLogger } from './middleware/http-logger';
import { openapiConfig, scalarConfig } from './openapi/config';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const isDevelopment = process.env.NODE_ENV !== 'production';

logger.debug('Environment variables loaded', {
  MAIL_ADDRESS: process.env.MAIL_ADDRESS ? 'Set' : 'Not set',
  PASSWORD: process.env.PASSWORD ? 'Set' : 'Not set',
  REFRESH_TOKEN: process.env.REFRESH_TOKEN ? 'Set' : 'Not set',
  ID_TOKEN: process.env.ID_TOKEN ? 'Set' : 'Not set',
});

const app = new OpenAPIHono();

// Error handling middleware (must be first)
app.use('*', errorHandler());

// Logger middleware with correlation ID
app.use('*', ...requestLogger());

// CORS settings for Vite dev server (development only)
if (isDevelopment) {
  app.use(
    '/*',
    cors({
      origin: ['http://localhost:5173', 'http://localhost:4173'],
      allowHeaders: ['Content-Type', 'Authorization', 'x-correlation-id'],
      allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    })
  );
}

// Mount all route modules (shared with generate-openapi.ts)
mountAllRoutes(app);

// OpenAPI documentation endpoint
app.doc('/openapi.json', openapiConfig);

// Scalar documentation UI
app.get('/doc', apiReference(scalarConfig));

// Serve static files in production
if (!isDevelopment) {
  const distPath = path.join(__dirname, '../dist/client');

  // Serve static assets
  app.use('/assets/*', serveStatic({ root: distPath }));

  // Serve index.html for all other routes (SPA fallback)
  app.get('*', serveStatic({ path: path.join(distPath, 'index.html') }));

  logger.info('Production mode: serving static files', { distPath });
}

const port = Number(process.env.PORT || 3001);

// Function to kill any process using the target port
async function killPortProcess(port: number): Promise<boolean> {
  try {
    // Find processes using the port
    const result = await $`lsof -ti:${port}`.text();
    const trimmed = result.trim();
    if (trimmed) {
      const pids = trimmed.split('\n').filter((pid) => pid.trim());
      if (pids.length > 0) {
        logger.info('Found processes using port, killing them', { port, pids });
        await $`kill -9 ${pids.join(' ')}`;
        logger.info('Successfully killed existing processes', { port, count: pids.length });
        // Wait a moment for ports to be released
        await Bun.sleep(2000);
        return true;
      }
    }
    return false;
  } catch (_error) {
    // No processes found using the port, or error occurred
    return false;
  }
}

// Note: Shutdown handlers are registered in service-lifecycle.ts
// which properly cleans up all services before exiting

// Kill any existing processes on the target port before starting
async function startServer() {
  await killPortProcess(port);

  logger.info(`Backend server starting at http://localhost:${port}`);

  // Start server using Bun.serve
  try {
    const server = Bun.serve({
      fetch: app.fetch,
      port,
      error(error) {
        logger.error('Server error', { error: error.message });
        return new Response('Internal Server Error', { status: 500 });
      },
    });

    logger.info(`Backend server running at http://localhost:${server.port}`);
  } catch (error) {
    if (error instanceof Error && error.message.includes('EADDRINUSE')) {
      logger.error('Port is still in use after cleanup attempt', {
        port,
        suggestion: 'Run: lsof -ti:3001 | xargs kill -9',
        error: error.message,
      });
    } else {
      logger.error('Failed to start server', {
        error: error instanceof Error ? error.message : String(error),
      });
    }
    process.exit(1);
  }
}

startServer().catch((error) => {
  logger.error('Failed to start server', { error: error.message });
  process.exit(1);
});
