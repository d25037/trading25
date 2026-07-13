import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import type { ApiDailyValuationDataPoint } from '@trading25/contracts/types/api-types';
import type { ShikihoQuoteV1 } from '@trading25/shikiho-extension/contract';
import type { ChartHeaderMarketCaps } from '@/pages/SymbolWorkbenchHeader';
import type { IndicatorValue, StockDataPoint } from '@/types/chart';

export interface ShikihoDailyOverlayProvenance {
  provisional: true;
  tradingDate: string;
  observedAt: string;
  delayMinutes: 15;
  sourceLabel: '会社四季報オンライン';
}

export interface ShikihoDailyOverlayInput {
  selectedSymbol: string | null;
  quoteCode: string | null;
  quote: ShikihoQuoteV1 | null | undefined;
  snapshotCapturedAt: string | null | undefined;
  dailyBars: StockDataPoint[];
  rankingResponse: MarketRankingSymbolResponse | undefined;
  latestValuation: ApiDailyValuationDataPoint | null | undefined;
  marketCaps: ChartHeaderMarketCaps;
  relativeMode: boolean;
  chartSmaPeriod?: number;
  now?: Date;
}

export interface ShikihoDailyOverlayResult {
  dailyBars: StockDataPoint[];
  rankingResponse: MarketRankingSymbolResponse | undefined;
  marketCaps: ChartHeaderMarketCaps;
  chartSmaPoint: IndicatorValue | null;
  provenance: ShikihoDailyOverlayProvenance | null;
}

function currentJstDate(now: Date): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
}

function isValidQuote(quote: ShikihoQuoteV1): boolean {
  const values = [quote.currentPrice, quote.open, quote.high, quote.low, quote.previousClose];
  return (
    values.every((value) => Number.isFinite(value) && value > 0) &&
    quote.high >= Math.max(quote.open, quote.currentPrice) &&
    quote.low <= Math.min(quote.open, quote.currentPrice) &&
    (quote.volume === null || (Number.isFinite(quote.volume) && quote.volume >= 0))
  );
}

function scale(value: number | null | undefined, ratio: number): number | null | undefined {
  return value == null ? value : value * ratio;
}

function positiveRatio(numerator: number, denominator: number | null | undefined): number | null {
  return denominator != null && denominator > 0 ? numerator / denominator : null;
}

function calculateSma5(closes: readonly number[], index: number): number | null {
  if (index < 4) return null;
  const window = closes.slice(index - 4, index + 1);
  return window.reduce((sum, value) => sum + value, 0) / 5;
}

function calculateCurrentSma(closes: readonly number[], period: number | undefined): number | null {
  if (period === undefined || !Number.isInteger(period) || period <= 0 || closes.length < period) return null;
  const window = closes.slice(-period);
  return window.reduce((sum, value) => sum + value, 0) / period;
}

function calculateSmaMetrics(closes: readonly number[]): {
  point: number | null;
  aboveCount: number | null;
  belowStreak: number | null;
} {
  const currentIndex = closes.length - 1;
  const point = calculateSma5(closes, currentIndex);
  if (point === null || closes.length < 9) return { point, aboveCount: null, belowStreak: null };

  let aboveCount = 0;
  for (let index = currentIndex - 4; index <= currentIndex; index += 1) {
    const sma = calculateSma5(closes, index);
    if (sma !== null && (closes[index] ?? 0) > sma) aboveCount += 1;
  }

  let belowStreak = 0;
  for (let index = currentIndex; index >= 4; index -= 1) {
    const sma = calculateSma5(closes, index);
    if (sma === null || (closes[index] ?? 0) >= sma) break;
    belowStreak += 1;
  }
  return { point, aboveCount, belowStreak };
}

function unchanged(input: ShikihoDailyOverlayInput): ShikihoDailyOverlayResult {
  return {
    dailyBars: input.dailyBars,
    rankingResponse: input.rankingResponse,
    marketCaps: input.marketCaps,
    chartSmaPoint: null,
    provenance: null,
  };
}

function canOverlay(
  input: ShikihoDailyOverlayInput,
  quote: ShikihoQuoteV1 | null | undefined
): quote is ShikihoQuoteV1 {
  if (input.relativeMode || quote == null || input.selectedSymbol == null) return false;
  if (input.selectedSymbol !== input.quoteCode || quote.tradingDate !== currentJstDate(input.now ?? new Date())) {
    return false;
  }
  const now = (input.now ?? new Date()).getTime();
  const observedAt = Date.parse(quote.observedAt);
  const capturedAt = Date.parse(input.snapshotCapturedAt ?? '');
  const observedAge = now - observedAt;
  const captureAge = now - capturedAt;
  return (
    Number.isFinite(observedAt) &&
    observedAge >= 0 &&
    Number.isFinite(capturedAt) &&
    captureAge >= 0 &&
    captureAge < 15 * 60 * 1_000 &&
    isValidQuote(quote) &&
    !input.dailyBars.some((bar) => bar.time >= quote.tradingDate)
  );
}

function composeRankingItem(
  input: ShikihoDailyOverlayInput,
  quote: ShikihoQuoteV1,
  sma: ReturnType<typeof calculateSmaMetrics>,
  issuedMarketCap: number | null | undefined,
  priceRatio: number
): MarketRankingSymbolResponse['item'] {
  const item = input.rankingResponse?.item;
  if (item == null) return null;
  const valuation = input.latestValuation;
  return {
    ...item,
    currentPrice: quote.currentPrice,
    previousPrice: quote.previousClose,
    changeAmount: quote.currentPrice - quote.previousClose,
    changePercentage: ((quote.currentPrice - quote.previousClose) / quote.previousClose) * 100,
    volume: quote.volume ?? item.volume,
    sma5AboveCount5d: sma.aboveCount,
    sma5BelowStreak: sma.belowStreak,
    per: positiveRatio(quote.currentPrice, valuation?.eps) ?? scale(item.per, priceRatio),
    forwardPer: positiveRatio(quote.currentPrice, valuation?.forwardEps) ?? scale(item.forwardPer, priceRatio),
    pbr: positiveRatio(quote.currentPrice, valuation?.bps) ?? scale(item.pbr, priceRatio),
    psr:
      issuedMarketCap != null && valuation?.sales != null && valuation.sales > 0
        ? issuedMarketCap / valuation.sales
        : scale(item.psr, priceRatio),
    forwardPsr:
      issuedMarketCap != null && valuation?.forwardSales != null && valuation.forwardSales > 0
        ? issuedMarketCap / valuation.forwardSales
        : scale(item.forwardPsr, priceRatio),
    marketCap: issuedMarketCap ?? scale(item.marketCap, priceRatio),
  };
}

export function composeShikihoDailyOverlay(input: ShikihoDailyOverlayInput): ShikihoDailyOverlayResult {
  const quote = input.quote;
  if (!canOverlay(input, quote)) return unchanged(input);

  const provisionalBar = {
    time: quote.tradingDate,
    open: quote.open,
    high: quote.high,
    low: quote.low,
    close: quote.currentPrice,
    ...(quote.volume === null ? {} : { volume: quote.volume }),
  } as StockDataPoint;
  const dailyBars = [...input.dailyBars, provisionalBar];
  const sma = calculateSmaMetrics(dailyBars.map((bar) => bar.close));
  const officialPrice = input.latestValuation?.close ?? input.rankingResponse?.item?.currentPrice ?? null;
  const priceRatio = officialPrice != null && officialPrice > 0 ? quote.currentPrice / officialPrice : null;
  const valuation = input.latestValuation;
  const shares = valuation?.marketCap != null && valuation.close > 0 ? valuation.marketCap / valuation.close : null;
  const issuedMarketCap =
    shares === null ? scale(input.marketCaps.issuedShares, priceRatio ?? 1) : shares * quote.currentPrice;
  const freeFloatMarketCap = scale(input.marketCaps.freeFloat, priceRatio ?? 1);
  const rankingItem = composeRankingItem(input, quote, sma, issuedMarketCap, priceRatio ?? 1);

  return {
    dailyBars,
    rankingResponse:
      input.rankingResponse === undefined
        ? undefined
        : { ...input.rankingResponse, date: quote.tradingDate, item: rankingItem },
    marketCaps: { issuedShares: issuedMarketCap ?? null, freeFloat: freeFloatMarketCap ?? null },
    chartSmaPoint: (() => {
      const value = calculateCurrentSma(
        dailyBars.map((bar) => bar.close),
        input.chartSmaPeriod
      );
      return value === null ? null : { time: quote.tradingDate, value };
    })(),
    provenance: {
      provisional: true,
      tradingDate: quote.tradingDate,
      observedAt: quote.observedAt,
      delayMinutes: quote.delayMinutes,
      sourceLabel: quote.sourceLabel,
    },
  };
}
