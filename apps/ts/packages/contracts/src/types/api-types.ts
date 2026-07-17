/**
 * Stable public aliases for FastAPI wire contracts.
 *
 * Display-only indicator points remain handwritten because they are normalized
 * chart models rather than OpenAPI request or response bodies.
 */

import type { components as BtApiComponents } from '../clients/backtest/generated/bt-api-types';

type BtApiSchemas = BtApiComponents['schemas'];

export type ApiStockDataPoint = BtApiSchemas['StockDataPoint'];
export type ApiStockDataResponse = BtApiSchemas['StockDataResponse'];
export type ResponseDiagnostics = BtApiSchemas['ResponseDiagnostics'];
export type DataProvenance = BtApiSchemas['DataProvenance'];

export interface IndicatorValue {
  time: string;
  value: number;
}

export interface MACDIndicatorData {
  time: string;
  macd: number;
  signal: number;
  histogram: number;
}

export interface PPOIndicatorData {
  time: string;
  ppo: number;
  signal: number;
  histogram: number;
}

export type IndicatorData = IndicatorValue | MACDIndicatorData | PPOIndicatorData;

export type ApiMarginVolumeRatioData = BtApiSchemas['MarginVolumeRatioData'];
export type ApiMarginVolumeRatioResponse = BtApiSchemas['MarginVolumeRatioResponse'];
export type ApiMarginLongPressureData = BtApiSchemas['MarginLongPressureData'];
export type ApiMarginFlowPressureData = BtApiSchemas['MarginFlowPressureData'];
export type ApiMarginTurnoverDaysData = BtApiSchemas['MarginTurnoverDaysData'];
export type ApiMarginPressureIndicatorsResponse = BtApiSchemas['MarginPressureIndicatorsResponse'];
export type ApiTopixDataPoint = BtApiSchemas['TopixDataPoint'];
export type ApiTopixDataResponse = BtApiSchemas['TopixDataResponse'];
export type ApiDailyValuationDataPoint = BtApiSchemas['DailyValuationDataPoint'];
export type ApiLatestMetricsSourceItem = BtApiSchemas['LatestMetricsSourceItem'];
export type ApiLatestMetricsSource = BtApiSchemas['LatestMetricsSource'];
export type ApiLiquidityProfileWindow = BtApiSchemas['LiquidityProfileWindow'];
export type ApiLiquidityProfile = BtApiSchemas['LiquidityProfile'];
export type ApiFundamentalDataPoint = BtApiSchemas['FundamentalDataPoint'];
export type ApiFundamentalsResponse = BtApiSchemas['FundamentalsComputeResponse'];
