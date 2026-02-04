import { describe, expect, it } from 'vitest';
import {
  formatBytes,
  formatCurrency,
  formatDateShort,
  formatFundamentalValue,
  formatInteger,
  formatPercentage,
  formatPrice,
  formatPriceJPY,
  formatRate,
  formatReturnPercent,
  formatTradingValue,
  formatVolume,
  formatVolumeRatio,
} from './formatters';

describe('formatPrice', () => {
  it('shows no decimals for values >= 10000', () => {
    expect(formatPrice(12345)).toBe('12,345');
  });

  it('shows 1 decimal for values >= 1000 and < 10000', () => {
    expect(formatPrice(1234.56)).toBe('1,234.6');
  });

  it('shows 2 decimals for values < 1000', () => {
    expect(formatPrice(123.456)).toBe('123.46');
  });

  it('returns - for NaN', () => {
    expect(formatPrice(Number.NaN)).toBe('-');
  });

  it('returns - for Infinity', () => {
    expect(formatPrice(Number.POSITIVE_INFINITY)).toBe('-');
  });
});

describe('formatPriceJPY', () => {
  it('formats as JPY currency', () => {
    const result = formatPriceJPY(1234);
    expect(result).toContain('1,234');
  });

  it('returns - for NaN', () => {
    expect(formatPriceJPY(Number.NaN)).toBe('-');
  });
});

describe('formatTradingValue', () => {
  it('returns T suffix for trillions', () => {
    expect(formatTradingValue(1.5e12)).toBe('1.50T');
  });

  it('returns B suffix for billions', () => {
    expect(formatTradingValue(2.3e9)).toBe('2.30B');
  });

  it('returns M suffix for millions', () => {
    expect(formatTradingValue(4.5e6)).toBe('4.50M');
  });

  it('returns locale string for smaller values', () => {
    expect(formatTradingValue(1234)).toBe('1,234');
  });

  it('returns - for undefined', () => {
    expect(formatTradingValue(undefined)).toBe('-');
  });

  it('returns - for NaN', () => {
    expect(formatTradingValue(Number.NaN)).toBe('-');
  });
});

describe('formatPercentage', () => {
  it('shows + sign for positive values by default', () => {
    expect(formatPercentage(5.123)).toBe('+5.12%');
  });

  it('shows - sign for negative values', () => {
    expect(formatPercentage(-3.5)).toBe('-3.50%');
  });

  it('shows + for zero', () => {
    expect(formatPercentage(0)).toBe('+0.00%');
  });

  it('hides sign when showSign is false', () => {
    expect(formatPercentage(5, { showSign: false })).toBe('5.00%');
  });

  it('respects decimals option', () => {
    expect(formatPercentage(5.1234, { decimals: 1 })).toBe('+5.1%');
  });

  it('returns - for undefined', () => {
    expect(formatPercentage(undefined)).toBe('-');
  });

  it('returns - for NaN', () => {
    expect(formatPercentage(Number.NaN)).toBe('-');
  });
});

describe('formatRate', () => {
  it('converts decimal to percentage with sign', () => {
    expect(formatRate(0.05)).toBe('+5.00%');
  });

  it('handles negative rates', () => {
    expect(formatRate(-0.03)).toBe('-3.00%');
  });

  it('returns - for NaN', () => {
    expect(formatRate(Number.NaN)).toBe('-');
  });
});

describe('formatVolumeRatio', () => {
  it('formats with x suffix', () => {
    expect(formatVolumeRatio(1.5)).toBe('1.50x');
  });

  it('returns - for undefined', () => {
    expect(formatVolumeRatio(undefined)).toBe('-');
  });

  it('returns - for NaN', () => {
    expect(formatVolumeRatio(Number.NaN)).toBe('-');
  });
});

describe('formatVolume', () => {
  it('returns B suffix for billions', () => {
    expect(formatVolume(1_500_000_000)).toBe('1.5B');
  });

  it('returns M suffix for millions', () => {
    expect(formatVolume(2_500_000)).toBe('2.5M');
  });

  it('returns K suffix for thousands', () => {
    expect(formatVolume(3_500)).toBe('3.5K');
  });

  it('returns integer for small values', () => {
    expect(formatVolume(500)).toBe('500');
  });

  it('returns - for NaN', () => {
    expect(formatVolume(Number.NaN)).toBe('-');
  });
});

describe('formatCurrency', () => {
  it('formats with locale separators', () => {
    expect(formatCurrency(1234567)).toBe('1,234,567');
  });

  it('returns - for NaN', () => {
    expect(formatCurrency(Number.NaN)).toBe('-');
  });
});

describe('formatInteger', () => {
  it('formats with locale separators and no decimals', () => {
    expect(formatInteger(1234.56)).toBe('1,235');
  });

  it('returns - for NaN', () => {
    expect(formatInteger(Number.NaN)).toBe('-');
  });
});

describe('formatDateShort', () => {
  it('formats date as MM/DD', () => {
    const result = formatDateShort('2024-03-15');
    expect(result).toBe('03/15');
  });
});

describe('formatFundamentalValue', () => {
  it('returns - for null', () => {
    expect(formatFundamentalValue(null, 'percent')).toBe('-');
  });

  it('returns - for NaN', () => {
    expect(formatFundamentalValue(Number.NaN, 'percent')).toBe('-');
  });

  it('formats percent', () => {
    expect(formatFundamentalValue(15.5, 'percent')).toBe('15.5%');
  });

  it('formats times', () => {
    expect(formatFundamentalValue(2.35, 'times')).toBe('2.35x');
  });

  it('formats yen for large values', () => {
    expect(formatFundamentalValue(15000, 'yen')).toBe('15.0k');
  });

  it('formats yen for small values', () => {
    expect(formatFundamentalValue(500, 'yen')).toBe('500');
  });

  it('formats millions as 兆', () => {
    expect(formatFundamentalValue(1_500_000, 'millions')).toBe('1.5兆');
  });

  it('formats millions as 億', () => {
    expect(formatFundamentalValue(500, 'millions')).toBe('5億');
  });

  it('formats millions as 百万', () => {
    expect(formatFundamentalValue(50, 'millions')).toBe('50百万');
  });
});

describe('formatReturnPercent', () => {
  it('formats positive return with + sign', () => {
    expect(formatReturnPercent(5.5)).toBe('+5.5%');
  });

  it('formats negative return with - sign', () => {
    expect(formatReturnPercent(-3.2)).toBe('-3.2%');
  });

  it('returns N/A for null', () => {
    expect(formatReturnPercent(null)).toBe('N/A');
  });

  it('returns N/A for undefined', () => {
    expect(formatReturnPercent(undefined)).toBe('N/A');
  });

  it('returns N/A for NaN', () => {
    expect(formatReturnPercent(Number.NaN)).toBe('N/A');
  });
});

describe('formatBytes', () => {
  it('formats bytes', () => {
    expect(formatBytes(500)).toBe('500 B');
  });

  it('formats kilobytes', () => {
    expect(formatBytes(1536)).toBe('1.5 KB');
  });

  it('formats megabytes', () => {
    expect(formatBytes(1_572_864)).toBe('1.5 MB');
  });

  it('formats gigabytes', () => {
    expect(formatBytes(1_610_612_736)).toBe('1.50 GB');
  });
});
