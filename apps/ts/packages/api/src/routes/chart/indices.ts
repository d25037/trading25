import { Database } from 'bun:sqlite';
import { createRoute, z } from '@hono/zod-openapi';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

interface IndexMasterRow {
  code: string;
  name: string;
  name_english: string | null;
  category: string;
  data_start_date: string | null;
}

interface IndicesDataRow {
  date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
}

const indicesApp = createOpenAPIApp();

// Schemas
const IndexItemSchema = z.object({
  code: z.string().openapi({ example: '0000', description: 'Index code' }),
  name: z.string().openapi({ example: 'TOPIX', description: 'Index name' }),
  nameEnglish: z.string().nullable().openapi({ example: 'TOPIX', description: 'English name' }),
  category: z.string().openapi({ example: 'topix', description: 'Index category' }),
  dataStartDate: z.string().nullable().openapi({ example: '2008-05-07', description: 'Data start date' }),
});

const IndicesListResponseSchema = z.object({
  indices: z.array(IndexItemSchema).openapi({ description: 'List of available indices' }),
  lastUpdated: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Last update time' }),
});

const IndexDataPointSchema = z.object({
  date: z.string().openapi({ example: '2024-01-01', description: 'Trading date' }),
  open: z.number().openapi({ example: 2500.5, description: 'Opening price' }),
  high: z.number().openapi({ example: 2520.0, description: 'High price' }),
  low: z.number().openapi({ example: 2480.0, description: 'Low price' }),
  close: z.number().openapi({ example: 2510.0, description: 'Closing price' }),
});

const IndexDataResponseSchema = z.object({
  code: z.string().openapi({ example: '0000', description: 'Index code' }),
  name: z.string().openapi({ example: 'TOPIX', description: 'Index name' }),
  data: z.array(IndexDataPointSchema).openapi({ description: 'OHLC data points' }),
  lastUpdated: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Last update time' }),
});

const IndexCodeParamSchema = z.object({
  code: z
    .string()
    .min(1)
    .openapi({
      example: '0000',
      description: 'Index code',
      param: { name: 'code', in: 'path' },
    }),
});

/**
 * Get indices list route
 */
const getIndicesListRoute = createRoute({
  method: 'get',
  path: '/api/chart/indices',
  tags: ['Chart'],
  summary: 'Get list of available indices',
  description: 'Fetch list of all available indices from index master table',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: IndicesListResponseSchema,
        },
      },
      description: 'Indices list retrieved successfully',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

/**
 * Get indices list handler
 */
indicesApp.openapi(getIndicesListRoute, async (c) => {
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  let db: InstanceType<typeof Database> | null = null;
  try {
    const dbPath = getMarketDbPath();
    db = new Database(dbPath, { readonly: true });

    const indices = db
      .query<IndexMasterRow, []>(
        'SELECT code, name, name_english, category, data_start_date FROM index_master ORDER BY code'
      )
      .all();

    return c.json(
      {
        indices: indices.map((idx) => ({
          code: idx.code,
          name: idx.name,
          nameEnglish: idx.name_english,
          category: idx.category,
          dataStartDate: idx.data_start_date,
        })),
        lastUpdated: new Date().toISOString(),
      },
      200
    );
  } catch (error) {
    logger.error('Failed to fetch indices list', {
      correlationId,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch indices list',
        correlationId,
      }),
      500
    );
  } finally {
    db?.close();
  }
});

/**
 * Get index data route
 */
const getIndexDataRoute = createRoute({
  method: 'get',
  path: '/api/chart/indices/{code}',
  tags: ['Chart'],
  summary: 'Get index OHLC data',
  description: 'Fetch historical OHLC data for a specific index',
  request: {
    params: IndexCodeParamSchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: IndexDataResponseSchema,
        },
      },
      description: 'Index data retrieved successfully',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Index not found',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

/**
 * Get index data handler
 */
indicesApp.openapi(getIndexDataRoute, async (c) => {
  const { code } = c.req.valid('param');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  let db: InstanceType<typeof Database> | null = null;
  try {
    const dbPath = getMarketDbPath();
    db = new Database(dbPath, { readonly: true });

    // Get index master info
    const indexInfo = db
      .query<IndexMasterRow, [string]>(
        'SELECT code, name, name_english, category, data_start_date FROM index_master WHERE code = ? LIMIT 1'
      )
      .get(code);

    if (!indexInfo) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Index with code "${code}" not found`,
          correlationId,
        }),
        404
      );
    }

    // Get OHLC data
    const data = db
      .query<IndicesDataRow, [string]>(
        'SELECT date, open, high, low, close FROM indices_data WHERE code = ? ORDER BY date DESC'
      )
      .all(code);

    // Filter out rows with null values and format response
    const validData = data
      .filter((row) => row.open !== null && row.high !== null && row.low !== null && row.close !== null)
      .map((row) => ({
        date: row.date,
        open: row.open as number,
        high: row.high as number,
        low: row.low as number,
        close: row.close as number,
      }))
      .reverse(); // Chronological order

    return c.json(
      {
        code: indexInfo.code,
        name: indexInfo.name,
        data: validData,
        lastUpdated: new Date().toISOString(),
      },
      200
    );
  } catch (error) {
    logger.error('Failed to fetch index data', {
      correlationId,
      code,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch index data',
        correlationId,
      }),
      500
    );
  } finally {
    db?.close();
  }
});

export default indicesApp;
