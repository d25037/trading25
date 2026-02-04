/**
 * Dataset Data Routes
 * Provides data access endpoints for dataset/*.db files
 * Used by Python API clients (trading25-bt)
 */
import { createRoute, z } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  BatchMarginQuerySchema,
  BatchMarginResponseSchema,
  BatchOHLCVQuerySchema,
  BatchOHLCVResponseSchema,
  BatchStatementsQuerySchema,
  BatchStatementsResponseSchema,
  DatasetNameSchema,
  DateRangeQuerySchema,
  IndexCodeSchema,
  IndexListQuerySchema,
  IndexListResponseSchema,
  MarginListQuerySchema,
  MarginListResponseSchema,
  MarginResponseSchema,
  OHLCResponseSchema,
  OHLCVQuerySchema,
  OHLCVResponseSchema,
  SectorMappingResponseSchema,
  SectorNameSchema,
  SectorStocksResponseSchema,
  SectorWithCountResponseSchema,
  StatementsQuerySchema,
  StatementsResponseSchema,
  StockCodeSchema,
  StockListQuerySchema,
  StockListResponseSchema,
  StockSectorMappingResponseSchema,
} from '../../schemas/dataset-data';
import { datasetDataService } from '../../services/dataset/dataset-data-service';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const datasetDataApp = createOpenAPIApp();

// ===== Stock OHLCV =====

const getStockOHLCVRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/stocks/{code}/ohlcv',
  tags: ['Dataset Data'],
  summary: 'Get stock OHLCV data',
  description:
    'Get OHLCV (Open, High, Low, Close, Volume) data for a specific stock. Supports daily, weekly, and monthly timeframes.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
      code: StockCodeSchema,
    }),
    query: OHLCVQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: OHLCVResponseSchema } },
      description: 'Stock OHLCV data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset or stock not found',
    },
  },
});

datasetDataApp.openapi(getStockOHLCVRoute, async (c) => {
  const { name, code } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStockOHLCV(name, code, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" or stock "${code}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Stock OHLCV Batch =====

const getStockOHLCVBatchRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/stocks/ohlcv/batch',
  tags: ['Dataset Data'],
  summary: 'Get batch stock OHLCV data',
  description:
    'Get OHLCV (Open, High, Low, Close, Volume) data for multiple stocks in a single request. Returns a record mapping stock codes to their OHLCV data. Maximum 100 stock codes per request.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: BatchOHLCVQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: BatchOHLCVResponseSchema } },
      description: 'Batch stock OHLCV data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getStockOHLCVBatchRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStockOHLCVBatch(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Stock List =====

const getStockListRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/stocks',
  tags: ['Dataset Data'],
  summary: 'Get stock list',
  description: 'Get list of available stocks in the dataset.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: StockListQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: StockListResponseSchema } },
      description: 'Stock list',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getStockListRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStockList(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== TOPIX =====

const getTopixRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/topix',
  tags: ['Dataset Data'],
  summary: 'Get TOPIX data',
  description: 'Get TOPIX index data from the dataset.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: DateRangeQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: OHLCResponseSchema } },
      description: 'TOPIX data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getTopixRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getTopix(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Index Data =====

const getIndexRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/indices/{code}',
  tags: ['Dataset Data'],
  summary: 'Get index data',
  description: 'Get OHLC data for a specific index.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
      code: IndexCodeSchema,
    }),
    query: DateRangeQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: OHLCResponseSchema } },
      description: 'Index data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset or index not found',
    },
  },
});

datasetDataApp.openapi(getIndexRoute, async (c) => {
  const { name, code } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getIndex(name, code, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" or index "${code}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Index List =====

const getIndexListRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/indices',
  tags: ['Dataset Data'],
  summary: 'Get index list',
  description: 'Get list of available indices in the dataset.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: IndexListQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: IndexListResponseSchema } },
      description: 'Index list',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getIndexListRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getIndexList(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Margin Batch =====
// NOTE: Batch route MUST be registered before {code} route to avoid
// "batch" being captured by {code} param and failing StockCodeSchema validation.

const getMarginBatchRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/margin/batch',
  tags: ['Dataset Data'],
  summary: 'Get batch margin data',
  description:
    'Get margin trading data for multiple stocks in a single request. Returns a record mapping stock codes to their margin data. Maximum 100 stock codes per request.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: BatchMarginQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: BatchMarginResponseSchema } },
      description: 'Batch margin data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getMarginBatchRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getMarginBatch(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Margin Data =====

const getMarginRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/margin/{code}',
  tags: ['Dataset Data'],
  summary: 'Get margin data',
  description: 'Get margin trading data for a specific stock.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
      code: StockCodeSchema,
    }),
    query: DateRangeQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: MarginResponseSchema } },
      description: 'Margin data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset or stock not found',
    },
  },
});

datasetDataApp.openapi(getMarginRoute, async (c) => {
  const { name, code } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getMargin(name, code, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" or stock "${code}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Margin List =====

const getMarginListRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/margin',
  tags: ['Dataset Data'],
  summary: 'Get margin list',
  description: 'Get list of stocks with margin data.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: MarginListQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: MarginListResponseSchema } },
      description: 'Margin list',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getMarginListRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getMarginList(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Statements Batch =====
// NOTE: Batch route MUST be registered before {code} route to avoid
// "batch" being captured by {code} param and failing StockCodeSchema validation.

const getStatementsBatchRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/statements/batch',
  tags: ['Dataset Data'],
  summary: 'Get batch financial statements',
  description:
    'Get financial statements data for multiple stocks in a single request. Returns a record mapping stock codes to their statements data. Maximum 100 stock codes per request. Supports period_type and actual_only filters.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
    query: BatchStatementsQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: BatchStatementsResponseSchema } },
      description: 'Batch financial statements data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getStatementsBatchRoute, async (c) => {
  const { name } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStatementsBatch(name, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Statements =====

const getStatementsRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/statements/{code}',
  tags: ['Dataset Data'],
  summary: 'Get financial statements',
  description:
    'Get financial statements data for a specific stock. Includes EPS, profit, equity, sales, operating profit, and other financial metrics. Use period_type to filter by FY/Q1/Q2/Q3 and actual_only to exclude forecast data.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
      code: StockCodeSchema,
    }),
    query: StatementsQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: StatementsResponseSchema } },
      description: 'Financial statements data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset or stock not found',
    },
  },
});

datasetDataApp.openapi(getStatementsRoute, async (c) => {
  const { name, code } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStatements(name, code, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" or stock "${code}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Sector Mapping =====

const getSectorMappingRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/sectors/mapping',
  tags: ['Dataset Data'],
  summary: 'Get sector mapping',
  description: 'Get sector to index mapping.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
  },
  responses: {
    200: {
      content: { 'application/json': { schema: SectorMappingResponseSchema } },
      description: 'Sector mapping',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getSectorMappingRoute, async (c) => {
  const { name } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getSectorMapping(name);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Stock-Sector Mapping =====

const getStockSectorMappingRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/sectors/stock-mapping',
  tags: ['Dataset Data'],
  summary: 'Get stock to sector mapping',
  description: 'Get mapping of all stock codes to their sector names (33 categories).',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
  },
  responses: {
    200: {
      content: { 'application/json': { schema: StockSectorMappingResponseSchema } },
      description: 'Stock to sector mapping',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getStockSectorMappingRoute, async (c) => {
  const { name } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getStockSectorMapping(name);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Sector Stocks =====

const getSectorStocksRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/sectors/{sectorName}/stocks',
  tags: ['Dataset Data'],
  summary: 'Get stocks in a sector',
  description: 'Get list of stock codes belonging to a specific sector.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
      sectorName: SectorNameSchema,
    }),
  },
  responses: {
    200: {
      content: { 'application/json': { schema: SectorStocksResponseSchema } },
      description: 'Stock codes in the sector',
    },
    400: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Invalid sector name encoding',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getSectorStocksRoute, async (c) => {
  const { name, sectorName } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  let decodedSectorName: string;
  try {
    decodedSectorName = decodeURIComponent(sectorName);
  } catch {
    return c.json(
      createErrorResponse({
        error: 'Bad Request',
        message: 'Invalid URI encoding in sector name',
        correlationId,
      }),
      400
    );
  }

  const data = await datasetDataService.getSectorStocks(name, decodedSectorName);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Sectors with Count =====

const getSectorsWithCountRoute = createRoute({
  method: 'get',
  path: '/api/dataset/{name}/sectors',
  tags: ['Dataset Data'],
  summary: 'Get all sectors with stock count',
  description: 'Get list of all sectors with their stock counts.',
  request: {
    params: z.object({
      name: DatasetNameSchema,
    }),
  },
  responses: {
    200: {
      content: { 'application/json': { schema: SectorWithCountResponseSchema } },
      description: 'Sectors with stock count',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Dataset not found',
    },
  },
});

datasetDataApp.openapi(getSectorsWithCountRoute, async (c) => {
  const { name } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const data = await datasetDataService.getSectorsWithCount(name);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Dataset "${name}" not found`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

export default datasetDataApp;
