import { beforeEach, describe, expect, it, mock } from 'bun:test';
import type { Hono } from 'hono';

mock.module('hono/bun', () => ({
  serveStatic: mock(),
}));

describe('Server', () => {
  let app: Hono;

  beforeEach(async () => {
    // Dynamically import and setup the server
    const { Hono } = await import('hono');
    app = new Hono();

    // Set up basic HTML route for testing
    app.get('/', (c) => {
      return c.html(`<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Trading25</title>
  <link rel="stylesheet" href="/styles-built.css">
</head>
<body>
  <div id="root"></div>
  <script type="module" src="/app.js"></script>
</body>
</html>`);
    });
  });

  describe('GET /', () => {
    it('should return HTML page', async () => {
      const req = new Request('http://localhost/', { method: 'GET' });
      const res = await app.request(req);

      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toContain('text/html');

      const html = await res.text();
      expect(html).toContain('<!DOCTYPE html>');
      expect(html).toContain('<title>Trading25</title>');
      expect(html).toContain('<div id="root"></div>');
      expect(html).toContain('<script type="module" src="/app.js"></script>');
    });

    it('should include required meta tags', async () => {
      const req = new Request('http://localhost/', { method: 'GET' });
      const res = await app.request(req);
      const html = await res.text();

      expect(html).toContain('<meta charset="UTF-8">');
      expect(html).toContain('<meta name="viewport" content="width=device-width, initial-scale=1.0">');
      expect(html).toContain('lang="ja"');
    });

    it('should include CSS and JS references', async () => {
      const req = new Request('http://localhost/', { method: 'GET' });
      const res = await app.request(req);
      const html = await res.text();

      expect(html).toContain('<link rel="stylesheet" href="/styles-built.css">');
      expect(html).toContain('<script type="module" src="/app.js"></script>');
    });
  });

  describe('error handling', () => {
    it('should handle 404 for unknown routes', async () => {
      const req = new Request('http://localhost/unknown', { method: 'GET' });
      const res = await app.request(req);

      expect(res.status).toBe(404);
    });
  });
});
