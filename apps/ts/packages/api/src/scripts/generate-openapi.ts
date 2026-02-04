import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { OpenAPIHono } from '@hono/zod-openapi';
import { stringify } from 'yaml';
import { mountAllRoutes } from '../app-routes';
import { openapiConfig } from '../openapi/config';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(__dirname, '../..');

/**
 * Generate OpenAPI specification files (JSON and YAML)
 */
async function generateSpec(): Promise<void> {
  console.log('Generating OpenAPI specification...');

  const app = new OpenAPIHono();
  mountAllRoutes(app);
  const spec = app.getOpenAPIDocument(openapiConfig);

  const jsonPath = path.join(projectRoot, 'openapi.json');
  const yamlPath = path.join(projectRoot, 'openapi.yaml');

  await Promise.all([
    Bun.write(jsonPath, JSON.stringify(spec, null, 2)),
    Bun.write(yamlPath, stringify(spec, { indent: 2 })),
  ]);

  console.log(`Generated: ${jsonPath}`);
  console.log(`Generated: ${yamlPath}`);
}

// Run generation
generateSpec().catch((error) => {
  console.error('Failed to generate OpenAPI specification:', error);
  process.exit(1);
});
