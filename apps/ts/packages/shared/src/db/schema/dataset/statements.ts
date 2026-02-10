/**
 * Dataset-specific Statements Schema
 *
 * Financial statements data (earnings, equity, etc.)
 */

import { index, primaryKey, real, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../../columns/stock-code';
import { stocks } from '../common/stocks';

/**
 * Financial statements table - earnings and equity data
 * Uses composite primary key (code, disclosed_date) instead of autoincrement id
 *
 * Extended with additional financial metrics from JQuants API:
 * - BPS (Book Value Per Share)
 * - Sales (Net Sales)
 * - Operating Profit
 * - Cash Flow from Operations
 * - Dividend (Fiscal Year)
 * - Forecast EPS
 */
export const statements = sqliteTable(
  'statements',
  {
    code: stockCode('code')
      .notNull()
      .references(() => stocks.code, { onDelete: 'cascade' }),
    disclosedDate: text('disclosed_date').notNull(),
    earningsPerShare: real('earnings_per_share'),
    profit: real('profit'),
    equity: real('equity'),
    typeOfCurrentPeriod: text('type_of_current_period'),
    typeOfDocument: text('type_of_document'),
    nextYearForecastEarningsPerShare: real('next_year_forecast_earnings_per_share'),
    // Extended financial metrics (added 2026-01)
    bps: real('bps'), // Book Value Per Share (1株当たり純資産)
    sales: real('sales'), // Net Sales (売上高)
    operatingProfit: real('operating_profit'), // Operating Profit (営業利益)
    ordinaryProfit: real('ordinary_profit'), // Ordinary Profit (経常利益)
    operatingCashFlow: real('operating_cash_flow'), // Cash Flow from Operations (営業CF)
    dividendFY: real('dividend_fy'), // Dividend Per Share Fiscal Year (通期配当)
    forecastEps: real('forecast_eps'), // Forecast EPS for current FY (EPS予想)
    // Cash flow extended metrics (added 2026-01)
    investingCashFlow: real('investing_cash_flow'), // Cash Flow from Investing (投資CF)
    financingCashFlow: real('financing_cash_flow'), // Cash Flow from Financing (財務CF)
    cashAndEquivalents: real('cash_and_equivalents'), // Cash and Cash Equivalents (現金及び現金同等物)
    totalAssets: real('total_assets'), // Total Assets (総資産)
    sharesOutstanding: real('shares_outstanding'), // Shares Outstanding at FY End (発行済株式数)
    treasuryShares: real('treasury_shares'), // Treasury Shares at FY End (自己株式数)
  },
  (table) => [
    primaryKey({ columns: [table.code, table.disclosedDate] }),
    index('idx_statements_date').on(table.disclosedDate),
    index('idx_statements_code').on(table.code),
  ]
);

/**
 * Type inference helpers
 */
export type StatementsRow = typeof statements.$inferSelect;
export type StatementsInsert = typeof statements.$inferInsert;
