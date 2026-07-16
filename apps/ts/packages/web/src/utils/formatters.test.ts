import { describe, expect, it } from 'vitest';
import {
  formatBytes,
  formatCount,
  formatCurrency,
  formatDateRangeText,
  formatDateShort,
  formatDateTimeLong,
  formatDateTimeShort,
  formatElapsedSeconds,
  formatFundamentalValue,
  formatInteger,
  formatOptionalDate,
  formatOptionalDateRange,
  formatOptionalTimestamp,
  formatPercentage,
  formatPrice,
  formatPriceJPY,
  formatRate,
  formatRatioPercentage,
  formatReturnPercent,
  formatShortId,
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
  it('formats as JPY currency with the canonical full-width yen symbol', () => {
    expect(formatPriceJPY(1234)).toBe('￥1,234');
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

describe('formatRatioPercentage', () => {
  it('converts a decimal ratio to an unsigned percentage', () => {
    expect(formatRatioPercentage(0.1234)).toBe('12.3%');
  });

  it('respects decimals option', () => {
    expect(formatRatioPercentage(0.1234, { decimals: 2 })).toBe('12.34%');
  });

  it('uses fallback for missing or invalid values', () => {
    expect(formatRatioPercentage(null)).toBe('-');
    expect(formatRatioPercentage(undefined, { fallback: 'N/A' })).toBe('N/A');
    expect(formatRatioPercentage(Number.NaN, { fallback: '0.0%' })).toBe('0.0%');
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

describe('formatCount', () => {
  it('formats counts with locale separators', () => {
    expect(formatCount(1234)).toBe('1,234');
  });

  it('returns zero for missing or invalid counts', () => {
    expect(formatCount(null)).toBe('0');
    expect(formatCount(undefined)).toBe('0');
    expect(formatCount(Number.NaN)).toBe('0');
  });
});

describe('formatDateShort', () => {
  it('formats date as MM/DD', () => {
    const result = formatDateShort('2024-03-15');
    expect(result).toBe('03/15');
  });
});

describe('formatDateTimeShort', () => {
  it('formats timestamp as short date and time', () => {
    expect(formatDateTimeShort('2024-03-15T09:30:00')).toContain('03/15');
    expect(formatDateTimeShort('2024-03-15T09:30:00')).toContain('09:30');
  });

  it('returns - for missing timestamp', () => {
    expect(formatDateTimeShort(null)).toBe('-');
    expect(formatDateTimeShort(undefined)).toBe('-');
  });
});

describe('formatDateTimeLong', () => {
  it('formats timestamp with year and minute precision', () => {
    const value = formatDateTimeLong('2024-03-15T09:30:45');
    expect(value).toContain('2024');
    expect(value).toContain('03/15');
    expect(value).toContain('09:30');
  });
});

describe('formatOptionalTimestamp', () => {
  it('formats valid timestamps', () => {
    expect(formatOptionalTimestamp('2024-03-15T09:30:00')).toContain('2024');
  });

  it('returns n/a for missing timestamps', () => {
    expect(formatOptionalTimestamp(null)).toBe('n/a');
    expect(formatOptionalTimestamp(undefined)).toBe('n/a');
  });

  it('preserves invalid timestamp strings', () => {
    expect(formatOptionalTimestamp('not-a-date')).toBe('not-a-date');
  });
});

describe('formatOptionalDate', () => {
  it('formats valid date values', () => {
    expect(formatOptionalDate('2024-03-15T09:30:00')).toContain('2024');
  });

  it('returns n/a for missing date values', () => {
    expect(formatOptionalDate(null)).toBe('n/a');
    expect(formatOptionalDate(undefined)).toBe('n/a');
  });

  it('preserves invalid date strings', () => {
    expect(formatOptionalDate('not-a-date')).toBe('not-a-date');
  });
});

describe('formatDateRangeText', () => {
  it('formats start and end dates', () => {
    expect(formatDateRangeText('2024-01-01', '2024-03-31')).toBe('2024-01-01 -> 2024-03-31');
  });

  it('returns n/a when either side is missing', () => {
    expect(formatDateRangeText('2024-01-01', null)).toBe('n/a');
    expect(formatDateRangeText(undefined, '2024-03-31')).toBe('n/a');
  });
});

describe('formatOptionalDateRange', () => {
  it('formats range objects', () => {
    expect(formatOptionalDateRange({ min: '2024-01-01', max: '2024-03-31' })).toBe('2024-01-01 -> 2024-03-31');
  });

  it('returns n/a for missing range objects', () => {
    expect(formatOptionalDateRange(null)).toBe('n/a');
    expect(formatOptionalDateRange(undefined)).toBe('n/a');
  });
});

describe('formatShortId', () => {
  it('truncates long IDs', () => {
    expect(formatShortId('abcdefghijk')).toBe('abcdefgh...');
  });

  it('keeps short IDs unchanged', () => {
    expect(formatShortId('abcd')).toBe('abcd');
  });

  it('supports custom visible length', () => {
    expect(formatShortId('abcdefghijk', 4)).toBe('abcd...');
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
    expect(formatBytes(1_610_612_736)).toBe('1.5 GB');
  });

  it('formats large byte counts up to terabytes', () => {
    expect(formatBytes(1_099_511_627_776)).toBe('1.0 TB');
  });

  it('returns zero bytes for missing or invalid byte counts', () => {
    expect(formatBytes(null)).toBe('0 B');
    expect(formatBytes(undefined)).toBe('0 B');
    expect(formatBytes(Number.NaN)).toBe('0 B');
    expect(formatBytes(-1)).toBe('0 B');
  });
});

describe('formatElapsedSeconds', () => {
  it('formats elapsed seconds as m:ss', () => {
    expect(formatElapsedSeconds(0)).toBe('0:00');
    expect(formatElapsedSeconds(9)).toBe('0:09');
    expect(formatElapsedSeconds(65)).toBe('1:05');
    expect(formatElapsedSeconds(3600)).toBe('60:00');
  });
});
