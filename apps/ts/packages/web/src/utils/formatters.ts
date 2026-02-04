/**
 * Centralized formatting utilities for consistent display across the application.
 * All formatters are designed to handle edge cases (null, undefined, negative values).
 */

/**
 * Format price with dynamic precision based on value magnitude.
 * Higher prices (>=10000) show no decimals, medium prices (>=1000) show 1 decimal,
 * lower prices show 2 decimals.
 */
export function formatPrice(value: number): string {
  if (!Number.isFinite(value)) return '-';

  let fractionDigits = 2;
  if (value >= 10000) {
    fractionDigits = 0;
  } else if (value >= 1000) {
    fractionDigits = 1;
  }

  return value.toLocaleString('ja-JP', { maximumFractionDigits: fractionDigits });
}

/**
 * Format price as Japanese Yen currency with no decimals.
 */
export function formatPriceJPY(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP', { style: 'currency', currency: 'JPY', maximumFractionDigits: 0 });
}

/**
 * Format trading value with T/B/M suffixes for large numbers.
 * Returns '-' for undefined or non-finite values.
 */
export function formatTradingValue(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}T`;
  if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
  if (value >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
  return value.toLocaleString();
}

/**
 * Format percentage with optional sign prefix.
 * @param value - The percentage value (already in percent form, not decimal)
 * @param options - Configuration options
 * @param options.showSign - Whether to show +/- sign (default: true)
 * @param options.decimals - Number of decimal places (default: 2)
 */
export function formatPercentage(
  value: number | undefined,
  options: { showSign?: boolean; decimals?: number } = {}
): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  const { showSign = true, decimals = 2 } = options;
  const sign = showSign && value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(decimals)}%`;
}

/**
 * Format a rate (decimal) as percentage with sign.
 * @param rate - The rate as a decimal (e.g., 0.05 for 5%)
 */
export function formatRate(rate: number): string {
  if (!Number.isFinite(rate)) return '-';
  const percentage = rate * 100;
  const sign = percentage >= 0 ? '+' : '';
  return `${sign}${percentage.toFixed(2)}%`;
}

/**
 * Format volume ratio with 'x' suffix.
 */
export function formatVolumeRatio(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}x`;
}

/**
 * Format volume with B/M/K suffixes for readability.
 */
export function formatVolume(value: number): string {
  if (!Number.isFinite(value)) return '-';
  if (value >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(1)}B`;
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return value.toFixed(0);
}

/**
 * Format currency value as Japanese locale number (no currency symbol).
 */
export function formatCurrency(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP');
}

/**
 * Format integer value with locale separators (ja-JP).
 */
export function formatInteger(value: number): string {
  if (!Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP', { maximumFractionDigits: 0 });
}

/**
 * Format date string to localized short format (MM/DD).
 */
export function formatDateShort(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('ja-JP', { month: '2-digit', day: '2-digit' });
}

/**
 * Format fundamental metrics with appropriate units.
 * Used for displaying financial metrics in FundamentalsSummaryCard.
 */
export function formatFundamentalValue(value: number | null, format: 'percent' | 'times' | 'yen' | 'millions'): string {
  if (value === null || !Number.isFinite(value)) return '-';

  switch (format) {
    case 'percent':
      return `${value.toFixed(1)}%`;
    case 'times':
      return `${value.toFixed(2)}x`;
    case 'yen':
      return value >= 10000 ? `${(value / 1000).toFixed(1)}k` : value.toFixed(0);
    case 'millions': {
      // Input is in millions of JPY; convert to Japanese units (兆/億/百万)
      const absVal = Math.abs(value);
      if (absVal >= 1_000_000) {
        return `${(value / 1_000_000).toFixed(1)}兆`;
      }
      if (absVal >= 100) {
        return `${(value / 100).toFixed(0)}億`;
      }
      return `${value.toLocaleString()}百万`;
    }
    default:
      return value.toFixed(2);
  }
}

/**
 * Format return percentage for future price points.
 * Handles null/undefined values and includes sign prefix.
 */
export function formatReturnPercent(changePercent: number | null | undefined): string {
  if (changePercent === null || changePercent === undefined || !Number.isFinite(changePercent)) return 'N/A';
  const sign = changePercent >= 0 ? '+' : '';
  return `${sign}${changePercent.toFixed(1)}%`;
}

/**
 * Format byte count to human-readable string (B, KB, MB, GB).
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}
