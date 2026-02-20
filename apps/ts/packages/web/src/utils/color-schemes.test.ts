import { describe, expect, it } from 'vitest';
import {
  getCfoMarginColor,
  getCfoYieldColor,
  getCashFlowColor,
  getFcfMarginColor,
  getFcfYieldColor,
  getFundamentalColor,
  getPbrColor,
  getPerColor,
  getPositiveNegativeColor,
  getReturnColor,
  getRoeColor,
} from './color-schemes';

describe('getPositiveNegativeColor', () => {
  it('returns green for positive', () => {
    expect(getPositiveNegativeColor(1)).toBe('text-green-500');
  });

  it('returns red for negative', () => {
    expect(getPositiveNegativeColor(-1)).toBe('text-red-500');
  });

  it('returns muted for zero', () => {
    expect(getPositiveNegativeColor(0)).toBe('text-muted-foreground');
  });
});

describe('getRoeColor', () => {
  it('returns green for >= 10', () => {
    expect(getRoeColor(10)).toBe('text-green-500');
    expect(getRoeColor(15)).toBe('text-green-500');
  });

  it('returns yellow for >= 5 and < 10', () => {
    expect(getRoeColor(5)).toBe('text-yellow-500');
    expect(getRoeColor(9.9)).toBe('text-yellow-500');
  });

  it('returns red for < 5', () => {
    expect(getRoeColor(4.9)).toBe('text-red-500');
    expect(getRoeColor(0)).toBe('text-red-500');
  });
});

describe('getPerColor', () => {
  it('returns red for negative', () => {
    expect(getPerColor(-5)).toBe('text-red-500');
  });

  it('returns red for > 25', () => {
    expect(getPerColor(30)).toBe('text-red-500');
  });

  it('returns green for <= 15', () => {
    expect(getPerColor(10)).toBe('text-green-500');
    expect(getPerColor(15)).toBe('text-green-500');
  });

  it('returns yellow for 15-25', () => {
    expect(getPerColor(20)).toBe('text-yellow-500');
    expect(getPerColor(25)).toBe('text-yellow-500');
  });
});

describe('getPbrColor', () => {
  it('returns green for < 1', () => {
    expect(getPbrColor(0.5)).toBe('text-green-500');
  });

  it('returns yellow for 1-2', () => {
    expect(getPbrColor(1)).toBe('text-yellow-500');
    expect(getPbrColor(2)).toBe('text-yellow-500');
  });

  it('returns red for > 2', () => {
    expect(getPbrColor(3)).toBe('text-red-500');
  });
});

describe('getCashFlowColor', () => {
  it('returns green for positive', () => {
    expect(getCashFlowColor(100)).toBe('text-green-500');
  });

  it('returns red for negative', () => {
    expect(getCashFlowColor(-100)).toBe('text-red-500');
  });

  it('returns muted for zero', () => {
    expect(getCashFlowColor(0)).toBe('text-muted-foreground');
  });
});

describe('getReturnColor', () => {
  it('returns green for positive', () => {
    expect(getReturnColor(5)).toBe('text-green-600 dark:text-green-400');
  });

  it('returns red for negative', () => {
    expect(getReturnColor(-5)).toBe('text-red-600 dark:text-red-400');
  });

  it('returns muted for zero', () => {
    expect(getReturnColor(0)).toBe('text-muted-foreground');
  });

  it('returns muted for null', () => {
    expect(getReturnColor(null)).toBe('text-muted-foreground');
  });

  it('returns muted for undefined', () => {
    expect(getReturnColor(undefined)).toBe('text-muted-foreground');
  });
});

describe('getFcfYieldColor', () => {
  it('returns green for >= 5', () => {
    expect(getFcfYieldColor(5)).toBe('text-green-500');
  });

  it('returns yellow for >= 2 and < 5', () => {
    expect(getFcfYieldColor(3)).toBe('text-yellow-500');
  });

  it('returns red for < 2', () => {
    expect(getFcfYieldColor(1)).toBe('text-red-500');
  });
});

describe('getFcfMarginColor', () => {
  it('returns green for >= 10', () => {
    expect(getFcfMarginColor(10)).toBe('text-green-500');
  });

  it('returns yellow for >= 5 and < 10', () => {
    expect(getFcfMarginColor(7)).toBe('text-yellow-500');
  });

  it('returns red for < 5', () => {
    expect(getFcfMarginColor(3)).toBe('text-red-500');
  });
});

describe('getCfoYieldColor', () => {
  it('returns green for >= 5', () => {
    expect(getCfoYieldColor(5)).toBe('text-green-500');
  });

  it('returns yellow for >= 2 and < 5', () => {
    expect(getCfoYieldColor(3)).toBe('text-yellow-500');
  });

  it('returns red for < 2', () => {
    expect(getCfoYieldColor(1)).toBe('text-red-500');
  });
});

describe('getCfoMarginColor', () => {
  it('returns green for >= 10', () => {
    expect(getCfoMarginColor(10)).toBe('text-green-500');
  });

  it('returns yellow for >= 5 and < 10', () => {
    expect(getCfoMarginColor(7)).toBe('text-yellow-500');
  });

  it('returns red for < 5', () => {
    expect(getCfoMarginColor(3)).toBe('text-red-500');
  });
});

describe('getFundamentalColor', () => {
  it('returns muted for null value', () => {
    expect(getFundamentalColor(null, 'roe')).toBe('text-muted-foreground');
  });

  it('returns foreground for neutral scheme', () => {
    expect(getFundamentalColor(10, 'neutral')).toBe('text-foreground');
  });

  it('dispatches to roe color', () => {
    expect(getFundamentalColor(15, 'roe')).toBe('text-green-500');
  });

  it('dispatches to per color', () => {
    expect(getFundamentalColor(10, 'per')).toBe('text-green-500');
  });

  it('dispatches to pbr color', () => {
    expect(getFundamentalColor(0.5, 'pbr')).toBe('text-green-500');
  });

  it('dispatches to cashFlow color', () => {
    expect(getFundamentalColor(100, 'cashFlow')).toBe('text-green-500');
  });

  it('dispatches to fcfYield color', () => {
    expect(getFundamentalColor(6, 'fcfYield')).toBe('text-green-500');
  });

  it('dispatches to fcfMargin color', () => {
    expect(getFundamentalColor(12, 'fcfMargin')).toBe('text-green-500');
  });

  it('dispatches to cfoYield color', () => {
    expect(getFundamentalColor(6, 'cfoYield')).toBe('text-green-500');
  });

  it('dispatches to cfoMargin color', () => {
    expect(getFundamentalColor(12, 'cfoMargin')).toBe('text-green-500');
  });
});
