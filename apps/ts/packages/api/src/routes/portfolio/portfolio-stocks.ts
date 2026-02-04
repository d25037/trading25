import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  PortfolioCodesResponseSchema,
  PortfolioItemDeletedResponseSchema,
  PortfolioItemResponseSchema,
  PortfolioNameParamSchema,
  PortfolioNameStockCodeParamSchema,
  UpdatePortfolioItemRequestSchema,
} from '../../schemas/portfolio';
import type { PortfolioService } from '../../services/portfolio-service';
import { createOpenAPIApp, safeDecodeURIComponent } from '../../utils';
import { handlePortfolioError, serializeItem } from './portfolio-helpers';

/**
 * Create portfolio stock routes (name+code based operations for CLI/human access)
 */
export function createPortfolioStockRoutes(getPortfolioService: () => PortfolioService) {
  const app = createOpenAPIApp();

  // Update stock in portfolio
  const updateStockRoute = createRoute({
    method: 'put',
    path: '/api/portfolio/{portfolioName}/stocks/{code}',
    tags: ['Portfolio'],
    summary: 'Update stock in portfolio',
    description: 'Update stock holding details in a portfolio',
    request: {
      params: PortfolioNameStockCodeParamSchema,
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
        description: 'Stock updated successfully',
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
        description: 'Portfolio or stock not found',
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

  app.openapi(updateStockRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { portfolioName, code } = c.req.valid('param');
    const body = c.req.valid('json');

    try {
      const updateInput = {
        ...body,
        purchaseDate: body.purchaseDate ? new Date(body.purchaseDate) : undefined,
      };
      const item = await getPortfolioService().updateItemByPortfolioNameAndCode(
        safeDecodeURIComponent(portfolioName, 'portfolioName'),
        code,
        updateInput
      );
      return c.json(serializeItem(item), 200);
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'update stock', { portfolioName, code }, [
        400, 404, 500,
      ] as const);
    }
  });

  // Delete stock from portfolio
  const deleteStockRoute = createRoute({
    method: 'delete',
    path: '/api/portfolio/{portfolioName}/stocks/{code}',
    tags: ['Portfolio'],
    summary: 'Remove stock from portfolio',
    description: 'Remove a stock holding from a portfolio',
    request: {
      params: PortfolioNameStockCodeParamSchema,
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: PortfolioItemDeletedResponseSchema,
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
        description: 'Invalid parameters',
      },
      404: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Portfolio or stock not found',
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

  app.openapi(deleteStockRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { portfolioName, code } = c.req.valid('param');

    try {
      const deletedItem = await getPortfolioService().deleteItemByPortfolioNameAndCode(
        safeDecodeURIComponent(portfolioName, 'portfolioName'),
        code
      );
      return c.json(
        {
          success: true,
          message: 'Stock removed successfully',
          deletedItem: serializeItem(deletedItem),
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'delete stock', { portfolioName, code }, [
        400, 404, 500,
      ] as const);
    }
  });

  // Get stock codes in portfolio by name
  const getPortfolioCodesRoute = createRoute({
    method: 'get',
    path: '/api/portfolio/{name}/codes',
    tags: ['Portfolio'],
    summary: 'Get stock codes in portfolio',
    description: 'Get list of stock codes in a portfolio by portfolio name. Used by Python API clients.',
    request: {
      params: PortfolioNameParamSchema,
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: PortfolioCodesResponseSchema,
          },
        },
        description: 'Stock codes retrieved successfully',
      },
      404: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Portfolio not found',
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

  app.openapi(getPortfolioCodesRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { name } = c.req.valid('param');
    const decodedName = safeDecodeURIComponent(name, 'portfolioName');

    try {
      const portfolio = await getPortfolioService().getPortfolioByName(decodedName);
      const portfolioWithItems = await getPortfolioService().getPortfolioWithItems(portfolio.id);
      const codes = portfolioWithItems.items.map((item) => item.code);

      return c.json(
        {
          name: portfolioWithItems.name,
          codes,
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'get portfolio codes', { name: decodedName }, [
        404, 500,
      ] as const);
    }
  });

  return app;
}
