/**
 * OpenAPI configuration for Trading25 API
 */

export const API_VERSION = '1.0.0';

/**
 * Scalar configuration for API documentation UI
 */
export const scalarConfig = {
  spec: {
    url: '/openapi.json',
  },
  theme: 'default' as const,
  layout: 'modern' as const,
  defaultHttpClient: {
    targetKey: 'js' as const,
    clientKey: 'fetch' as const,
  },
};

/**
 * OpenAPI document configuration
 */
export const openapiConfig = {
  openapi: '3.1.0' as const,
  info: {
    title: 'Trading25 API',
    version: API_VERSION,
    description: `# Trading25 API

Financial data analysis API with JQuants integration.

## Two-Layer Architecture

### 🔧 Layer 1: JQuants Proxy (Development)
Raw JQuants API data for debugging, verification, and development. Tagged as **jquants**.

**Use for**: Debugging, data verification, custom processing development

### 🚀 Layer 2: Chart & Analytics (Production)
Optimized, chart-ready data for production applications. Tagged as **chart** and **analytics**.

**Use for**: Web UI, CLI tools, production applications

### 🗂️ Portfolio Management
CRUD operations for portfolio tracking. Tagged as **portfolio**.`,
    contact: {
      name: 'Trading25 Team',
    },
    license: {
      name: 'MIT',
      url: 'https://opensource.org/licenses/MIT',
    },
  },
  servers: [
    {
      url: 'http://localhost:3001',
      description: 'Development server',
    },
    {
      url: 'https://api.trading25.example.com',
      description: 'Production server (placeholder)',
    },
  ],
  tags: [
    {
      name: 'Health',
      description: '🏥 Health check endpoints for service monitoring',
    },
    {
      name: 'JQuants Proxy',
      description:
        '🔧 **Layer 1: JQuants Proxy API** - Raw JQuants data for debugging and development. Use for debugging, data verification, and custom processing. Not optimized for production.',
    },
    {
      name: 'Chart',
      description:
        '🚀 **Layer 2: Chart Data API** - Optimized, chart-ready data for production applications. Use for web UI, CLI tools, and production data consumption. Caching enabled.',
    },
    {
      name: 'Analytics',
      description:
        '🚀 **Layer 2: Analytics API** - Computed metrics and analytics for production applications. Market rankings, screening results, ROE calculations, and technical indicators.',
    },
    {
      name: 'Market',
      description:
        '🔄 **Market Management** - Sync, validate, and refresh market data. Database maintenance operations.',
    },
    {
      name: 'Portfolio',
      description:
        '🗂️ **Portfolio Management** - CRUD operations for portfolio tracking. Manage portfolios and stock holdings.',
    },
  ],
};
