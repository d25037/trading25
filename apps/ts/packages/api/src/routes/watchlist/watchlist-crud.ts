import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CreateWatchlistRequestSchema,
  ListWatchlistsResponseSchema,
  UpdateWatchlistRequestSchema,
  WatchlistDeleteSuccessResponseSchema,
  WatchlistIdParamSchema,
  WatchlistResponseSchema,
  WatchlistWithItemsResponseSchema,
} from '../../schemas/watchlist';
import type { WatchlistService } from '../../services/watchlist-service';
import { createOpenAPIApp, safeParseInt } from '../../utils';
import {
  handleWatchlistError,
  serializeWatchlist,
  serializeWatchlistItem,
  serializeWatchlistSummary,
} from './watchlist-helpers';

export function createWatchlistCrudRoutes(getWatchlistService: () => WatchlistService) {
  const app = createOpenAPIApp();

  const listWatchlistsRoute = createRoute({
    method: 'get',
    path: '/api/watchlist',
    tags: ['Watchlist'],
    summary: 'List all watchlists',
    description: 'Retrieve all watchlists with summary statistics',
    responses: {
      200: {
        content: { 'application/json': { schema: ListWatchlistsResponseSchema } },
        description: 'Watchlists retrieved successfully',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(listWatchlistsRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    try {
      const watchlists = await getWatchlistService().listWatchlists();
      return c.json(
        {
          watchlists: watchlists.map((w) => serializeWatchlistSummary(w)),
        },
        200
      );
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'list watchlists', undefined, [500] as const);
    }
  });

  const createWatchlistRoute = createRoute({
    method: 'post',
    path: '/api/watchlist',
    tags: ['Watchlist'],
    summary: 'Create a new watchlist',
    description: 'Create a new watchlist with a unique name',
    request: {
      body: { content: { 'application/json': { schema: CreateWatchlistRequestSchema } } },
    },
    responses: {
      201: {
        content: { 'application/json': { schema: WatchlistResponseSchema } },
        description: 'Watchlist created successfully',
      },
      400: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Invalid request parameters',
      },
      409: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Watchlist name already exists',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(createWatchlistRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const body = c.req.valid('json');
    try {
      const watchlist = await getWatchlistService().createWatchlist(body);
      return c.json(serializeWatchlist(watchlist), 201);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'create watchlist', undefined, [400, 409, 500] as const);
    }
  });

  const getWatchlistRoute = createRoute({
    method: 'get',
    path: '/api/watchlist/{id}',
    tags: ['Watchlist'],
    summary: 'Get watchlist details',
    description: 'Retrieve a watchlist with all its items',
    request: { params: WatchlistIdParamSchema },
    responses: {
      200: {
        content: { 'application/json': { schema: WatchlistWithItemsResponseSchema } },
        description: 'Watchlist retrieved successfully',
      },
      400: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Invalid watchlist ID',
      },
      404: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Watchlist not found',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(getWatchlistRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const watchlistId = safeParseInt(id, 'watchlistId');
    try {
      const watchlist = await getWatchlistService().getWatchlistWithItems(watchlistId);
      return c.json(
        {
          ...serializeWatchlist(watchlist),
          items: watchlist.items.map((item) => serializeWatchlistItem(item)),
        },
        200
      );
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'get watchlist', { watchlistId }, [400, 404, 500] as const);
    }
  });

  const updateWatchlistRoute = createRoute({
    method: 'put',
    path: '/api/watchlist/{id}',
    tags: ['Watchlist'],
    summary: 'Update watchlist',
    description: 'Update watchlist name or description',
    request: {
      params: WatchlistIdParamSchema,
      body: { content: { 'application/json': { schema: UpdateWatchlistRequestSchema } } },
    },
    responses: {
      200: {
        content: { 'application/json': { schema: WatchlistResponseSchema } },
        description: 'Watchlist updated successfully',
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
        description: 'Watchlist name already exists',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(updateWatchlistRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const watchlistId = safeParseInt(id, 'watchlistId');
    const body = c.req.valid('json');
    try {
      const watchlist = await getWatchlistService().updateWatchlist(watchlistId, body);
      return c.json(serializeWatchlist(watchlist), 200);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'update watchlist', { watchlistId }, [
        400, 404, 409, 500,
      ] as const);
    }
  });

  const deleteWatchlistRoute = createRoute({
    method: 'delete',
    path: '/api/watchlist/{id}',
    tags: ['Watchlist'],
    summary: 'Delete watchlist',
    description: 'Delete a watchlist and all its items',
    request: { params: WatchlistIdParamSchema },
    responses: {
      200: {
        content: { 'application/json': { schema: WatchlistDeleteSuccessResponseSchema } },
        description: 'Watchlist deleted successfully',
      },
      400: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Invalid watchlist ID',
      },
      404: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Watchlist not found',
      },
      500: {
        content: { 'application/json': { schema: ErrorResponseSchema } },
        description: 'Internal server error',
      },
    },
  });

  app.openapi(deleteWatchlistRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const watchlistId = safeParseInt(id, 'watchlistId');
    try {
      await getWatchlistService().deleteWatchlist(watchlistId);
      return c.json({ success: true, message: 'Watchlist deleted successfully' }, 200);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'delete watchlist', { watchlistId }, [
        400, 404, 500,
      ] as const);
    }
  });

  return app;
}
