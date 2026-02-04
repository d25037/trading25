/**
 * Chart color constants
 * Standard TradingView-like color scheme: green for up, red for down
 */
export const CHART_COLORS = {
  /** Price increase color (green) */
  UP: '#26a69a',
  /** Price decrease color (red) */
  DOWN: '#ef5350',
  /** Bollinger Bands color (blue) */
  BOLLINGER: '#2962FF',
  /** N-Bar Support line color (red) */
  N_BAR_SUPPORT: '#F23645',
  /** ATR Support line color (red) */
  ATR_SUPPORT: '#ef5350',
  /** Grid line color */
  GRID: '#e1e1e1',
  /** Text color */
  TEXT: '#333',
} as const;

/**
 * Chart dimension constants
 */
export const CHART_DIMENSIONS = {
  /** Default chart height in pixels */
  DEFAULT_HEIGHT: 500,
  /** Minimum chart height in pixels (prevents collapse) */
  MIN_HEIGHT: 200,
  /** PPO chart height in pixels */
  PPO_HEIGHT: 384,
  /** Sub-chart height in pixels (Volume Comparison, Trading Value MA) */
  SUB_CHART_HEIGHT: 200,
  /** Main OHLC chart height in pixels */
  MAIN_CHART_HEIGHT: 512,
} as const;

/**
 * Chart series line widths
 */
export const CHART_LINE_WIDTHS = {
  /** Standard indicator line width */
  STANDARD: 1,
  /** Emphasized indicator line width */
  EMPHASIZED: 2,
} as const;

/**
 * Volume scale margins (percentage of chart height)
 */
export const VOLUME_SCALE_MARGINS = {
  TOP: 0.8,
  BOTTOM: 0,
} as const;
