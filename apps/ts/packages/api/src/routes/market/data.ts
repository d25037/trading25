/**
 * Market Data Routes
 * Provides data access endpoints for market.db
 * Used by Python API clients (trading25-bt)
 */
import { createRoute, z } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  MarketDateRangeQuerySchema,
  MarketOHLCResponseSchema,
  MarketOHLCVResponseSchema,
  MarketStocksQuerySchema,
  MarketStocksResponseSchema,
  StockInfoSchema,
} from '../../schemas/market-data';
import { marketDataService } from '../../services/market/market-data-service';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const marketDataApp = createOpenAPIApp();

// ===== Single Stock Info =====

const getStockInfoRoute = createRoute({
  method: 'get',
  path: '/api/market/stocks/{code}',
  tags: ['Market Data'],
  summary: 'Get single stock info from market.db',
  description: `Get stock information (company name, market, sector) for a specific stock from market.db.

**Used by**: apps/bt/ fundamentals service for company name lookup.`,
  request: {
    params: z.object({
      code: z.string().min(1).openapi({ description: 'Stock code (4 or 5 digits)', example: '7203' }),
    }),
  },
  responses: {
    200: {
      content: { 'application/json': { schema: StockInfoSchema } },
      description: 'Stock info',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Stock not found',
    },
  },
});

marketDataApp.openapi(getStockInfoRoute, (c) => {
  const { code } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  const data = marketDataService.getStockInfo(code);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Stock "${code}" not found in market.db`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== Stock OHLCV =====

const getStockOHLCVRoute = createRoute({
  method: 'get',
  path: '/api/market/stocks/{code}/ohlcv',
  tags: ['Market Data'],
  summary: 'Get stock OHLCV data from market.db',
  description: 'Get OHLCV (Open, High, Low, Close, Volume) data for a specific stock from market.db.',
  request: {
    params: z.object({
      code: z.string().min(1).openapi({ description: 'Stock code', example: '7203' }),
    }),
    query: MarketDateRangeQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: MarketOHLCVResponseSchema } },
      description: 'Stock OHLCV data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Stock not found',
    },
  },
});

marketDataApp.openapi(getStockOHLCVRoute, (c) => {
  const { code } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = marketDataService.getStockOHLCV(code, query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: `Stock "${code}" not found in market.db`,
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

// ===== All Stocks (Screening) =====

const getAllStocksRoute = createRoute({
  method: 'get',
  path: '/api/market/stocks',
  tags: ['Market Data'],
  summary: 'Get all stocks data for screening',
  description: 'Get all stocks with OHLCV history for a specified period. Used for screening.',
  request: {
    query: MarketStocksQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: MarketStocksResponseSchema } },
      description: 'All stocks data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Market database not found',
    },
  },
});

marketDataApp.openapi(getAllStocksRoute, (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = marketDataService.getAllStocks(query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: 'Market database not found',
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
  path: '/api/market/topix',
  tags: ['Market Data'],
  summary: 'Get TOPIX data from market.db',
  description: 'Get TOPIX index data from market.db.',
  request: {
    query: MarketDateRangeQuerySchema,
  },
  responses: {
    200: {
      content: { 'application/json': { schema: MarketOHLCResponseSchema } },
      description: 'TOPIX data',
    },
    404: {
      content: { 'application/json': { schema: ErrorResponseSchema } },
      description: 'Market database not found',
    },
  },
});

marketDataApp.openapi(getTopixRoute, (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  const data = marketDataService.getTopix(query);

  if (data === null) {
    return c.json(
      createErrorResponse({
        error: 'Not Found',
        message: 'Market database not found or TOPIX data not available',
        correlationId,
      }),
      404
    );
  }

  return c.json(data, 200);
});

export default marketDataApp;
