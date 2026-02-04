/**
 * Market Data Reader
 *
 * Re-exports Drizzle ORM implementation for backward compatibility.
 * All new code should import directly from '../db'.
 */

export { DrizzleMarketDataReader as MarketDataReader, type RankingItem } from '../db/drizzle-market-reader';
