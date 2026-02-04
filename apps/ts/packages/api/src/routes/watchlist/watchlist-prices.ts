import { createRoute } from '@hono/zod-openapi';
import { getMarketDbPath } from '@trading25/shared';
import { MarketDataReader } from '@trading25/shared/market-sync';
import { logger } from '@trading25/shared/utils/logger';
import type { WatchlistStockPrice } from '@trading25/shared/watchlist';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { WatchlistIdParamSchema, WatchlistPricesResponseSchema } from '../../schemas/watchlist';
import type { WatchlistService } from '../../services/watchlist-service';
import { createOpenAPIApp, safeParseInt } from '../../utils';
import { handleWatchlistError } from './watchlist-helpers';

function getPrevClose(reader: MarketDataReader, jquantsCode: string, prevDate: Date | null): number | null {
  if (!prevDate) return null;
  const prevData = reader.getStockData(jquantsCode, { from: prevDate, to: prevDate });
  const prevRecord = prevData[prevData.length - 1];
  return prevRecord ? prevRecord.close : null;
}

function calcChangePercent(close: number, prevClose: number | null): number | null {
  if (prevClose === null || prevClose === 0) return null;
  return Number((((close - prevClose) / prevClose) * 100).toFixed(2));
}

function fetchSingleStockPrice(
  reader: MarketDataReader,
  code: string,
  latestDate: Date,
  prevDate: Date | null
): WatchlistStockPrice | null {
  const jquantsCode = code.length === 4 ? `${code}0` : code;
  const latestData = reader.getStockData(jquantsCode, { from: latestDate, to: latestDate });
  const latest = latestData[latestData.length - 1];
  if (!latest) return null;

  const prevClose = getPrevClose(reader, jquantsCode, prevDate);

  return {
    code,
    close: latest.close,
    prevClose,
    changePercent: calcChangePercent(latest.close, prevClose),
    volume: latest.volume,
    date: latest.date.toISOString().split('T')[0] ?? '',
  };
}

function fetchStockPrices(codes: string[]): WatchlistStockPrice[] {
  if (codes.length === 0) return [];

  try {
    const marketDbPath = getMarketDbPath();
    const reader = new MarketDataReader(marketDbPath);

    try {
      const latestDate = reader.getLatestTradingDate();
      if (!latestDate) return [];

      const prevDate = reader.getPreviousTradingDate(latestDate);
      const prices: WatchlistStockPrice[] = [];

      for (const code of codes) {
        const price = fetchSingleStockPrice(reader, code, latestDate, prevDate);
        if (price) prices.push(price);
      }

      return prices;
    } finally {
      reader.close();
    }
  } catch (error) {
    logger.warn('Failed to fetch stock prices from market database', {
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  }
}

export function createWatchlistPricesRoutes(getWatchlistService: () => WatchlistService) {
  const app = createOpenAPIApp();

  const getPricesRoute = createRoute({
    method: 'get',
    path: '/api/watchlist/{id}/prices',
    tags: ['Watchlist'],
    summary: 'Get stock prices for watchlist',
    description: 'Retrieve latest stock prices for all stocks in a watchlist',
    request: { params: WatchlistIdParamSchema },
    responses: {
      200: {
        content: { 'application/json': { schema: WatchlistPricesResponseSchema } },
        description: 'Prices retrieved successfully',
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

  app.openapi(getPricesRoute, async (c) => {
    const correlationId = getCorrelationId(c);
    const { id } = c.req.valid('param');
    const watchlistId = safeParseInt(id, 'watchlistId');

    try {
      const watchlist = await getWatchlistService().getWatchlistWithItems(watchlistId);
      const codes = watchlist.items.map((item) => item.code);
      const prices = fetchStockPrices(codes);

      return c.json({ prices }, 200);
    } catch (error) {
      return handleWatchlistError(c, error, correlationId, 'get watchlist prices', { watchlistId }, [
        404, 500,
      ] as const);
    }
  });

  return app;
}
