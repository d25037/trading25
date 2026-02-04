import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  CreatePortfolioRequestSchema,
  DeleteSuccessResponseSchema,
  ListPortfoliosResponseSchema,
  PortfolioIdParamSchema,
  PortfolioResponseSchema,
  PortfolioWithItemsResponseSchema,
  UpdatePortfolioRequestSchema,
} from '../../schemas/portfolio';
import type { PortfolioService } from '../../services/portfolio-service';
import { createOpenAPIApp, safeParseInt } from '../../utils';
import {
  handlePortfolioError,
  serializeItem,
  serializePortfolio,
  serializePortfolioSummary,
} from './portfolio-helpers';

/**
 * Create portfolio CRUD routes
 */
export function createPortfolioCrudRoutes(getPortfolioService: () => PortfolioService) {
  const app = createOpenAPIApp();

  // List all portfolios
  const listPortfoliosRoute = createRoute({
    method: 'get',
    path: '/api/portfolio',
    tags: ['Portfolio'],
    summary: 'List all portfolios',
    description: 'Retrieve all portfolios with summary statistics',
    responses: {
      200: {
        content: {
          'application/json': {
            schema: ListPortfoliosResponseSchema,
          },
        },
        description: 'Portfolios retrieved successfully',
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

  app.openapi(listPortfoliosRoute, async (c) => {
    const correlationId = getCorrelationId(c);

    try {
      const portfolios = await getPortfolioService().listPortfolios();
      return c.json(
        {
          portfolios: portfolios.map((p) => serializePortfolioSummary(p)),
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'list portfolios', undefined, [500] as const);
    }
  });

  // Create a new portfolio
  const createPortfolioRoute = createRoute({
    method: 'post',
    path: '/api/portfolio',
    tags: ['Portfolio'],
    summary: 'Create a new portfolio',
    description: 'Create a new portfolio with a unique name',
    request: {
      body: {
        content: {
          'application/json': {
            schema: CreatePortfolioRequestSchema,
          },
        },
      },
    },
    responses: {
      201: {
        content: {
          'application/json': {
            schema: PortfolioResponseSchema,
          },
        },
        description: 'Portfolio created successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid request parameters',
      },
      409: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Portfolio name already exists',
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

  app.openapi(createPortfolioRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const body = c.req.valid('json');

    try {
      const portfolio = await getPortfolioService().createPortfolio(body);
      return c.json(serializePortfolio(portfolio), 201);
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'create portfolio', undefined, [400, 409, 500] as const);
    }
  });

  // Get portfolio with items
  const getPortfolioRoute = createRoute({
    method: 'get',
    path: '/api/portfolio/{id}',
    tags: ['Portfolio'],
    summary: 'Get portfolio details',
    description: 'Retrieve a portfolio with all its items',
    request: {
      params: PortfolioIdParamSchema,
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: PortfolioWithItemsResponseSchema,
          },
        },
        description: 'Portfolio retrieved successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid portfolio ID',
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

  app.openapi(getPortfolioRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const portfolioId = safeParseInt(id, 'portfolioId');

    try {
      const portfolio = await getPortfolioService().getPortfolioWithItems(portfolioId);
      return c.json(
        {
          ...serializePortfolio(portfolio),
          items: portfolio.items.map((item) => serializeItem(item)),
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'get portfolio', { portfolioId }, [400, 404, 500] as const);
    }
  });

  // Update portfolio
  const updatePortfolioRoute = createRoute({
    method: 'put',
    path: '/api/portfolio/{id}',
    tags: ['Portfolio'],
    summary: 'Update portfolio',
    description: 'Update portfolio name or description',
    request: {
      params: PortfolioIdParamSchema,
      body: {
        content: {
          'application/json': {
            schema: UpdatePortfolioRequestSchema,
          },
        },
      },
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: PortfolioResponseSchema,
          },
        },
        description: 'Portfolio updated successfully',
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
        description: 'Portfolio name already exists',
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

  app.openapi(updatePortfolioRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const portfolioId = safeParseInt(id, 'portfolioId');
    const body = c.req.valid('json');

    try {
      const portfolio = await getPortfolioService().updatePortfolio(portfolioId, body);
      return c.json(serializePortfolio(portfolio), 200);
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'update portfolio', { portfolioId }, [
        400, 404, 409, 500,
      ] as const);
    }
  });

  // Delete portfolio
  const deletePortfolioRoute = createRoute({
    method: 'delete',
    path: '/api/portfolio/{id}',
    tags: ['Portfolio'],
    summary: 'Delete portfolio',
    description: 'Delete a portfolio and all its items',
    request: {
      params: PortfolioIdParamSchema,
    },
    responses: {
      200: {
        content: {
          'application/json': {
            schema: DeleteSuccessResponseSchema,
          },
        },
        description: 'Portfolio deleted successfully',
      },
      400: {
        content: {
          'application/json': {
            schema: ErrorResponseSchema,
          },
        },
        description: 'Invalid portfolio ID',
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

  app.openapi(deletePortfolioRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const portfolioId = safeParseInt(id, 'portfolioId');

    try {
      await getPortfolioService().deletePortfolio(portfolioId);
      return c.json(
        {
          success: true,
          message: 'Portfolio deleted successfully',
        },
        200
      );
    } catch (error) {
      return handlePortfolioError(c, error, correlationId, 'delete portfolio', { portfolioId }, [
        400, 404, 500,
      ] as const);
    }
  });

  return app;
}
