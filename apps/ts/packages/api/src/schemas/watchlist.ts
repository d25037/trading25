import { z } from '@hono/zod-openapi';

/**
 * Watchlist ID parameter schema
 */
export const WatchlistIdParamSchema = z
  .object({
    id: z
      .string()
      .regex(/^\d+$/, 'Watchlist ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Watchlist ID',
        param: {
          name: 'id',
          in: 'path',
        },
      }),
  })
  .openapi('WatchlistIdParam');

/**
 * Watchlist Item ID parameter schema
 */
export const WatchlistItemIdParamSchema = z
  .object({
    id: z
      .string()
      .regex(/^\d+$/, 'Watchlist ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Watchlist ID',
        param: {
          name: 'id',
          in: 'path',
        },
      }),
    itemId: z
      .string()
      .regex(/^\d+$/, 'Item ID must be a positive integer')
      .openapi({
        example: '1',
        description: 'Watchlist Item ID',
        param: {
          name: 'itemId',
          in: 'path',
        },
      }),
  })
  .openapi('WatchlistItemIdParam');

/**
 * Create Watchlist request schema
 */
export const CreateWatchlistRequestSchema = z
  .object({
    name: z.string().min(1, 'Watchlist name is required').openapi({
      example: 'Tech Stocks',
      description: 'Watchlist name (must be unique)',
    }),
    description: z.string().optional().openapi({
      example: 'Technology sector stocks to monitor',
      description: 'Watchlist description',
    }),
  })
  .openapi('CreateWatchlistRequest');

/**
 * Update Watchlist request schema
 */
export const UpdateWatchlistRequestSchema = z
  .object({
    name: z.string().min(1).optional().openapi({
      example: 'Updated Watchlist',
      description: 'Updated watchlist name',
    }),
    description: z.string().optional().openapi({
      example: 'Updated description',
      description: 'Updated watchlist description',
    }),
  })
  .openapi('UpdateWatchlistRequest');

/**
 * Create Watchlist Item request schema
 */
export const CreateWatchlistItemRequestSchema = z
  .object({
    code: z
      .string()
      .regex(/^\d[0-9A-Z]\d[0-9A-Z]$/, 'Stock code must be 4 characters (e.g., 7203 or 285A)')
      .openapi({
        example: '7203',
        description: 'Stock code (4 characters)',
      }),
    companyName: z.string().min(1).optional().openapi({
      example: 'Toyota Motor',
      description: 'Company name (if not provided, will be fetched from market database)',
    }),
    memo: z.string().optional().openapi({
      example: 'Watching for breakout',
      description: 'Optional memo',
    }),
  })
  .openapi('CreateWatchlistItemRequest');

/**
 * Watchlist response schema
 */
export const WatchlistResponseSchema = z
  .object({
    id: z.number().openapi({ example: 1, description: 'Watchlist ID' }),
    name: z.string().openapi({ example: 'Tech Stocks', description: 'Watchlist name' }),
    description: z.string().optional().openapi({ example: 'Technology sector stocks', description: 'Description' }),
    createdAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Creation timestamp' }),
    updatedAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Last update timestamp' }),
  })
  .openapi('WatchlistResponse');

/**
 * Watchlist Item response schema
 */
export const WatchlistItemResponseSchema = z
  .object({
    id: z.number().openapi({ example: 1, description: 'Item ID' }),
    watchlistId: z.number().openapi({ example: 1, description: 'Watchlist ID' }),
    code: z.string().openapi({ example: '7203', description: 'Stock code' }),
    companyName: z.string().openapi({ example: 'Toyota Motor', description: 'Company name' }),
    memo: z.string().optional().openapi({ example: 'Watching for breakout', description: 'Memo' }),
    createdAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Creation timestamp' }),
  })
  .openapi('WatchlistItemResponse');

/**
 * Watchlist with Items response schema
 */
export const WatchlistWithItemsResponseSchema = z
  .object({
    id: z.number().openapi({ example: 1, description: 'Watchlist ID' }),
    name: z.string().openapi({ example: 'Tech Stocks', description: 'Watchlist name' }),
    description: z.string().optional().openapi({ example: 'Technology sector stocks', description: 'Description' }),
    createdAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Creation timestamp' }),
    updatedAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Last update timestamp' }),
    items: z.array(WatchlistItemResponseSchema).openapi({ description: 'Watchlist items' }),
  })
  .openapi('WatchlistWithItemsResponse');

/**
 * Watchlist Summary response schema
 */
export const WatchlistSummaryResponseSchema = z
  .object({
    id: z.number().openapi({ example: 1, description: 'Watchlist ID' }),
    name: z.string().openapi({ example: 'Tech Stocks', description: 'Watchlist name' }),
    description: z.string().optional().openapi({ example: 'Technology sector stocks', description: 'Description' }),
    stockCount: z.number().openapi({ example: 5, description: 'Number of stocks in watchlist' }),
    createdAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Creation timestamp' }),
    updatedAt: z.string().openapi({ example: '2024-01-01T00:00:00.000Z', description: 'Last update timestamp' }),
  })
  .openapi('WatchlistSummaryResponse');

/**
 * List Watchlists response schema
 */
export const ListWatchlistsResponseSchema = z
  .object({
    watchlists: z.array(WatchlistSummaryResponseSchema).openapi({
      description: 'List of watchlists with summary statistics',
    }),
  })
  .openapi('ListWatchlistsResponse');

/**
 * Delete success response schema
 */
export const WatchlistDeleteSuccessResponseSchema = z
  .object({
    success: z.boolean().openapi({ example: true, description: 'Operation success status' }),
    message: z.string().openapi({ example: 'Watchlist deleted successfully', description: 'Success message' }),
  })
  .openapi('WatchlistDeleteSuccessResponse');

/**
 * Stock price info schema
 */
export const WatchlistStockPriceSchema = z
  .object({
    code: z.string().openapi({ example: '7203', description: 'Stock code' }),
    close: z.number().openapi({ example: 2500, description: 'Latest close price' }),
    prevClose: z.number().nullable().openapi({ example: 2450, description: 'Previous close price' }),
    changePercent: z.number().nullable().openapi({ example: 2.04, description: 'Change percentage' }),
    volume: z.number().openapi({ example: 1500000, description: 'Trading volume' }),
    date: z.string().openapi({ example: '2024-01-15', description: 'Data date' }),
  })
  .openapi('WatchlistStockPrice');

/**
 * Watchlist prices response schema
 */
export const WatchlistPricesResponseSchema = z
  .object({
    prices: z.array(WatchlistStockPriceSchema).openapi({ description: 'Stock prices for watchlist items' }),
  })
  .openapi('WatchlistPricesResponse');
