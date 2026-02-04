import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CreateWatchlistItemRequestSchema,
  WatchlistDeleteSuccessResponseSchema,
  WatchlistIdParamSchema,
  WatchlistItemIdParamSchema,
  WatchlistItemResponseSchema,
} from '../../schemas/watchlist';
import type { WatchlistService } from '../../services/watchlist-service';
import { createOpenAPIApp, safeParseInt } from '../../utils';
import { handleWatchlistError, serializeWatchlistItem } from './watchlist-helpers';

export function createWatchlistItemRoutes(getWatchlistService: () => WatchlistService) {
  const app = createOpenAPIApp();

  const addItemRoute = createRoute({
    method: 'post',
    path: '/api/watchlist/{id}/items',
    tags: ['Watchlist'],
    summary: 'Add stock to watchlist',
    description: 'Add a stock to a watchlist (company name auto-fetched if not provided)',
    request: {
      params: WatchlistIdParamSchema,
      body: { content: { 'application/json': { schema: CreateWatchlistItemRequestSchema } } },
    },
    responses: {
      201: {
        content: { 'application/json': { schema: WatchlistItemResponseSchema } },
        description: 'Stock added successfully',
      },
      400: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Invalid request parameters',
      },
      404: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Watchlist not found',
      },
      409: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Stock already in watchlist',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(addItemRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const watchlistId = safeParseInt(id, 'watchlistId');
    const body = c.req.valid('json');

    try {
      const item = await getWatchlistService().addItem(watchlistId, body);
      return c.json(serializeWatchlistItem(item), 201);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'add item to watchlist', { watchlistId }, [
        400, 404, 409, 500,
      ] as const);
    }
  });

  const deleteItemRoute = createRoute({
    method: 'delete',
    path: '/api/watchlist/{id}/items/{itemId}',
    tags: ['Watchlist'],
    summary: 'Remove stock from watchlist',
    description: 'Remove a stock from a watchlist by item ID',
    request: {
      params: WatchlistItemIdParamSchema,
    },
    responses: {
      200: {
        content: { 'application/json': { schema: WatchlistDeleteSuccessResponseSchema } },
        description: 'Stock removed successfully',
      },
      404: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Item not found',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(deleteItemRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { itemId } = c.req.valid('param');
    const parsedItemId = safeParseInt(itemId, 'itemId');

    try {
      await getWatchlistService().deleteItem(parsedItemId);
      return c.json({ success: true, message: 'Stock removed from watchlist' }, 200);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'delete watchlist item', { itemId: parsedItemId }, [
        404, 500,
      ] as const);
    }
  });

  return app;
}
