import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  ApiStockDataResponseSchema,
  StockQuerySchema,
  StockSearchQuerySchema,
  StockSearchResponseSchema,
  StockSymbolParamSchema,
} from '../../schemas/stock';
import { StockDataService } from '../../services/stock-data';
import { createErrorResponse, createOpenAPIApp } from '../../utils';

const stockDataService = new StockDataService();

const stocksApp = createOpenAPIApp();

/**
 * Stock search route
 *
 * 🔍 Search stocks by code or company name (fuzzy search)
 *
 * IMPORTANT: This route MUST be registered BEFORE the /api/chart/stocks/{symbol} route
 * to prevent "search" from being interpreted as a symbol parameter.
 */
const searchStocksRoute = createRoute({
  method: 'get',
  path: '/api/chart/stocks/search',
  tags: ['Chart'],
  summary: '🔍 Search stocks by code or company name',
  description: `**Fuzzy search for stocks** - Search by stock code or company name (Japanese/English)

**Use Cases**:
- Autocomplete stock symbol input
- Find stocks by company name
- Quick stock lookup

**Search Priority**:
1. Exact code match
2. Code prefix match
3. Company name (Japanese) match
4. Company name (English) match

**Example Queries**:
- \`7203\` → Toyota Motor Corporation
- \`トヨタ\` → Toyota Motor Corporation
- \`Toyota\` → Toyota Motor Corporation
- \`銀行\` → All banks

**Response**:
\`\`\`json
{
  "query": "トヨタ",
  "results": [
    {
      "code": "7203",
      "companyName": "トヨタ自動車",
      "companyNameEnglish": "TOYOTA MOTOR CORPORATION",
      "marketCode": "prime",
      "marketName": "プライム",
      "sector33Name": "輸送用機器"
    }
  ],
  "count": 1
}
\`\`\``,
  request: {
    query: StockSearchQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: StockSearchResponseSchema,
        },
      },
      description: 'Search results returned successfully',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid search parameters',
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
 * Stock search handler - MUST be registered first
 */
stocksApp.openapi(searchStocksRoute, (c) => {
  const { q, limit } = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const results = stockDataService.searchStocks(q, limit);

    return c.json(
      {
        query: q,
        results,
        count: results.length,
      },
      200
    );
  } catch (error) {
    logger.error('Stock search failed', {
      correlationId,
      query: q,
      limit,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Stock search failed',
        correlationId,
      }),
      500
    );
  }
});

/**
 * Get stock price data route
 *
 * 🚀 Layer 2: Chart & Analytics API
 *
 * Purpose: Returns chart-ready, optimized data for production applications
 *
 * Use Cases:
 * - Web UI chart rendering (lightweight-charts)
 * - CLI data visualization
 * - Production data consumption
 * - Performance-critical operations
 *
 * ✅ This is the recommended endpoint for production use
 */
const getStockDataRoute = createRoute({
  method: 'get',
  path: '/api/chart/stocks/{symbol}',
  tags: ['Chart'],
  summary: '🚀 Get stock chart data (optimized for production)',
  description: `**Layer 2: Chart & Analytics API** - Optimized data for production applications

**Purpose**: Returns processed, chart-ready data optimized for visualization and performance.

**Use Cases**:
- Web UI chart rendering with lightweight-charts
- CLI data display and reporting
- Production application data consumption
- Performance-critical operations

✅ **This is the recommended endpoint for production applications.**

**Characteristics**:
- Chart-ready format (camelCase: "time", "open", "high", "low", "close", "volume")
- Optimized for lightweight-charts library
- Caching enabled for performance
- Consistent data structure
- Company name included automatically

**Data Format**:
\`\`\`json
{
  "symbol": "7203",
  "companyName": "トヨタ自動車",
  "timeframe": "daily",
  "data": [
    {
      "time": "2024-12-01",
      "open": 2850.0,
      "high": 2900.0,
      "low": 2840.0,
      "close": 2880.0,
      "volume": 15230000
    }
  ],
  "lastUpdated": "2024-12-16T10:00:00Z"
}
\`\`\`

🔧 **For debugging raw JQuants data**, use \`/api/jquants/daily-quotes\` instead.`,
  request: {
    params: StockSymbolParamSchema,
    query: StockQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiStockDataResponseSchema,
        },
      },
      description: 'Stock data retrieved successfully',
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
      description: 'Stock symbol not found',
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
 * Get stock price data handler
 */
stocksApp.openapi(getStockDataRoute, async (c) => {
  const { symbol } = c.req.valid('param');
  const { timeframe, adjusted } = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const stockData = await stockDataService.getStockData(symbol, timeframe, adjusted);

    // Check if stock data is empty (symbol not found)
    if (!stockData || stockData.data.length === 0) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Stock symbol '${symbol}' not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(stockData, 200);
  } catch (error) {
    logger.error('Failed to fetch stock data', {
      correlationId,
      symbol,
      timeframe,
      adjusted,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch stock data',
        correlationId,
      }),
      500
    );
  }
});

export default stocksApp;
