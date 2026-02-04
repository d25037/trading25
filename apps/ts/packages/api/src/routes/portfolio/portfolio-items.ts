import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CreatePortfolioItemRequestSchema,
  DeleteSuccessResponseSchema,
  PortfolioIdParamSchema,
  PortfolioItemIdParamSchema,
  PortfolioItemResponseSchema,
  UpdatePortfolioItemRequestSchema,
} from '../../schemas/portfolio';
import type { PortfolioService } from '../../services/portfolio-service';
import { createOpenAPIApp, safeParseInt } from '../../utils';
import { handlePortfolioError, serializeItem } from './portfolio-helpers';

/**
 * Create portfolio item routes (ID-based operations)
 */
export function createPortfolioItemRoutes(getPortfolioService: () => PortfolioService) {
  const app = createOpenAPIApp();

  // Add item to portfolio
  const addItemRoute = createRoute({
    method: 'post',
    path: '/api/portfolio/{id}/items',
    tags: ['Portfolio'],
    summary: 'Add stock to portfolio',
    description: 'Add a new stock holding to a portfolio',
    request: {
      params: PortfolioIdParamSchema,
      body: {
        content: {
          'application/json': {
            schema: CreatePortfolioItemRequestSchema,
          },
        },
      },
    },
    responses: {
      201: {
        content: {
          'application/json': {
            schema: PortfolioItemResponseSchema,
          },
        },
        description: 'Item added successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid request parameters',
      },
      404: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Portfolio not found',
      },
      409: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Stock already exists in portfolio',
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

  app.openapi(addItemRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const portfolioId = safeParseInt(id, 'portfolioId');
    const body = c.req.valid('json');

    try {
      const item = await getPortfolioService().addItem(portfolioId, {
        ...body,
        purchaseDate: new Date(body.purchaseDate),
      });
      return c.json(serializeItem(item), 201);
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'add item', { portfolioId }, [400, 404, 409, 500] as const);
    }
  });

  // Update portfolio item by ID
  const updateItemRoute = createRoute({
    method: 'put',
    path: '/api/portfolio/{id}/items/{itemId}',
    tags: ['Portfolio'],
    summary: 'Update portfolio item by ID',
    description: 'Update stock holding details using item ID (for programmatic access)',
    request: {
      params: PortfolioItemIdParamSchema,
      body: {
        content: {
          'application/json': {
            schema: UpdatePortfolioItemRequestSchema,
          },
        },
      },
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: PortfolioItemResponseSchema,
          },
        },
        description: 'Item updated successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid request parameters',
      },
      404: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Item not found',
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

  app.openapi(updateItemRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { itemId } = c.req.valid('param');
    const itemIdNum = safeParseInt(itemId, 'itemId');
    const body = c.req.valid('json');

    try {
      const updateInput = {
        ...body,
        purchaseDate: body.purchaseDate ? new Date(body.purchaseDate) : undefined,
      };
      const item = await getPortfolioService().updateItem(itemIdNum, updateInput);
      return c.json(serializeItem(item), 200);
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'update item', { itemId: itemIdNum }, [
        400, 404, 500,
      ] as const);
    }
  });

  // Delete portfolio item by ID
  const deleteItemRoute = createRoute({
    method: 'delete',
    path: '/api/portfolio/{id}/items/{itemId}',
    tags: ['Portfolio'],
    summary: 'Delete portfolio item by ID',
    description: 'Remove a stock holding using item ID (for programmatic access)',
    request: {
      params: PortfolioItemIdParamSchema,
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: DeleteSuccessResponseSchema,
          },
        },
        description: 'Item deleted successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid item ID',
      },
      404: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Item not found',
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

  app.openapi(deleteItemRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { itemId } = c.req.valid('param');
    const itemIdNum = safeParseInt(itemId, 'itemId');

    try {
      await getPortfolioService().deleteItem(itemIdNum);
      return c.json(
        {
          success: true,
          message: 'Item deleted successfully',
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'delete item', { itemId: itemIdNum }, [
        400, 404, 500,
      ] as const);
    }
  });

  return app;
}
