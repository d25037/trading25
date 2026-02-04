import { beforeEach, describe, expect, it, mock } from 'bun:test';
import type { JQuantsDailyQuote } from '@trading25/shared';
import { displayDailyQuotes, displayIndices, displayListedInfo, displayMarginInterest, displayTOPIX } from './display';

// Mock chalk
mock.module('chalk', () => ({
  default: {
    red: mock((text: string) => text),
    green: mock((text: string) => text),
    yellow: mock((text: string) => text),
    cyan: mock((text: string) => text),
    white: mock((text: string) => text),
    gray: mock((text: string) => text),
  },
}));

// Import the mocked chalk (will be the mocked version)
import chalk from 'chalk';

const mockChalk = chalk; // Alias for easier test migration

// Mock console
const mockConsole = {
  log: mock(),
  error: mock(),
};
global.console = mockConsole as unknown as Console;

// Test data using v2 field names
const mockDailyQuotes = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      O: 2750,
      H: 2780,
      L: 2740,
      C: 2765,
      Vo: 1250000,
      Va: 3456789000,
      AdjFactor: 1.0,
      AdjO: 2750,
      AdjH: 2780,
      AdjL: 2740,
      AdjC: 2765,
      AdjVo: 1250000,
    },
  ],
};

const mockTOPIXData = {
  data: [
    {
      Date: '2025-01-10',
      O: 2359.28,
      H: 2380.1,
      L: 2335.58,
      C: 2378.79,
    },
    {
      Date: '2025-01-11',
      O: 2387.88,
      H: 2400.53,
      L: 2382.79,
      C: 2393.54,
    },
  ],
};

const mockStockInfo = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      CoName: 'トヨタ自動車',
      CoNameEn: 'TOYOTA MOTOR CORPORATION',
      S17: '050',
      S17Nm: '自動車',
      S33: '3300',
      S33Nm: '自動車',
      Mkt: '111',
      MktNm: 'プライム',
      ScaleCat: '1',
    },
  ],
};

const mockMarginInterest = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      ShrtVol: 5000000,
      LongVol: 8000000,
      ShrtNegVol: 3000000,
      LongNegVol: 4000000,
      ShrtStdVol: 2000000,
      LongStdVol: 4000000,
      IssType: '1',
    },
  ],
};

const mockIndicesData = {
  data: [
    {
      Date: '2025-01-10',
      Code: 'I101',
      O: 1500.5,
      H: 1520.3,
      L: 1495.2,
      C: 1510.8,
    },
  ],
};

describe('Display Functions', () => {
  beforeEach(() => {
    // Clear mock call history for Bun
    mockConsole.log.mockClear?.();
    mockConsole.error.mockClear?.();
    if (typeof mockChalk.cyan === 'function' && 'mockClear' in mockChalk.cyan) {
      (mockChalk.cyan as unknown as ReturnType<typeof mock>).mockClear?.();
    }
    if (typeof mockChalk.white === 'function' && 'mockClear' in mockChalk.white) {
      (mockChalk.white as unknown as ReturnType<typeof mock>).mockClear?.();
    }
    if (typeof mockChalk.yellow === 'function' && 'mockClear' in mockChalk.yellow) {
      (mockChalk.yellow as unknown as ReturnType<typeof mock>).mockClear?.();
    }
  });

  describe('displayDailyQuotes', () => {
    it('should display daily quotes data correctly', () => {
      displayDailyQuotes(mockDailyQuotes.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Daily Quotes'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-10 (7203)'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Open:  ¥2,750'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('High:  ¥2,780'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Low:   ¥2,740'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Close: ¥2,765'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Volume: 1,250,000'));
    });

    it('should handle null values in daily quotes', () => {
      const quotesWithNulls = [
        {
          Date: '2025-01-10',
          Code: '7203',
          O: null,
          H: null,
          L: null,
          C: null,
          Vo: null,
          Va: null,
          AdjFactor: null,
          AdjO: null,
          AdjH: null,
          AdjL: null,
          AdjC: null,
          AdjVo: null,
        },
      ] as unknown as JQuantsDailyQuote[];

      displayDailyQuotes(quotesWithNulls);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Open:  ¥N/A'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('High:  ¥N/A'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Low:   ¥N/A'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Close: ¥N/A'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Volume: N/A'));
    });

    it('should handle empty daily quotes array', () => {
      displayDailyQuotes([]);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Daily Quotes'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('━'.repeat(60)));
    });

    it('should use chalk colors correctly', () => {
      displayDailyQuotes(mockDailyQuotes.data);

      expect(mockChalk.cyan).toHaveBeenCalledWith(expect.stringContaining('Daily Quotes'));
      expect(mockChalk.white).toHaveBeenCalledWith('━'.repeat(60));
      expect(mockChalk.yellow).toHaveBeenCalledWith(expect.stringContaining('2025-01-10 (7203)'));
    });
  });

  describe('displayTOPIX', () => {
    it('should display TOPIX data correctly', () => {
      displayTOPIX(mockTOPIXData.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('TOPIX Index'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-10'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Open:  2,359.28'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('High:  2,380.1'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Low:   2,335.58'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Close: 2,378.79'));
    });

    it('should display multiple TOPIX records', () => {
      displayTOPIX(mockTOPIXData.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-10'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-11'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2,387.88'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2,393.54'));
    });

    it('should handle empty TOPIX array', () => {
      displayTOPIX([]);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('TOPIX Index'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('━'.repeat(60)));
    });

    it('should format numbers with commas', () => {
      const topixWithLargeNumbers = [
        {
          Date: '2025-01-10',
          O: 12345.67,
          H: 23456.78,
          L: 11234.56,
          C: 22345.67,
        },
      ];

      displayTOPIX(topixWithLargeNumbers);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('12,345.67'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('23,456.78'));
    });

    it('should use chalk colors correctly', () => {
      displayTOPIX(mockTOPIXData.data);

      expect(mockChalk.cyan).toHaveBeenCalledWith(expect.stringContaining('TOPIX Index'));
      expect(mockChalk.white).toHaveBeenCalledWith('━'.repeat(60));
      expect(mockChalk.yellow).toHaveBeenCalledWith(expect.stringContaining('2025-01-10'));
    });
  });

  describe('displayListedInfo', () => {
    it('should display listed stock information correctly', () => {
      displayListedInfo(mockStockInfo.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Listed Stocks'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('7203: トヨタ自動車'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('English: TOYOTA MOTOR CORPORATION'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Market: プライム'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Sector: 自動車'));
    });

    it('should handle empty listed info array', () => {
      displayListedInfo([]);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Listed Stocks'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('━'.repeat(60)));
    });
  });

  describe('displayMarginInterest', () => {
    it('should display margin interest data correctly', () => {
      displayMarginInterest(mockMarginInterest.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Margin Interest'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-10 (7203)'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Short Margin: 5,000,000'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Long Margin:  8,000,000'));
    });

    it('should handle empty margin interest array', () => {
      displayMarginInterest([]);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Margin Interest'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('━'.repeat(60)));
    });
  });

  describe('displayIndices', () => {
    it('should display indices data correctly', () => {
      displayIndices(mockIndicesData.data);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Index Data'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('2025-01-10'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Open:  1500.5'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('High:  1520.3'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Low:   1495.2'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Close: 1510.8'));
    });

    it('should handle empty indices array', () => {
      displayIndices([]);

      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Index Data'));
      expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('━'.repeat(60)));
    });
  });

  describe('common formatting tests', () => {
    it('should use consistent header formatting across all display functions', () => {
      displayDailyQuotes(mockDailyQuotes.data);
      displayTOPIX(mockTOPIXData.data);
      displayListedInfo(mockStockInfo.data);
      displayMarginInterest(mockMarginInterest.data);
      displayIndices(mockIndicesData.data);

      // Each function should call cyan for headers
      expect(mockChalk.cyan).toHaveBeenCalledTimes(5);

      // Each function should call white for separator lines
      expect(mockChalk.white).toHaveBeenCalledTimes(5);

      // Each function should use yellow for main data items
      expect(mockChalk.yellow).toHaveBeenCalled();
    });

    it('should use consistent separator line length', () => {
      displayDailyQuotes([]);
      displayTOPIX([]);
      displayListedInfo([]);
      displayMarginInterest([]);
      displayIndices([]);

      const whiteFunc = mockChalk.white as unknown as ReturnType<typeof mock>;
      const separatorCalls = whiteFunc.mock.calls.filter((call) => call[0]?.includes('━'));

      // All separator lines should be the same length
      separatorCalls.forEach((call) => {
        expect(call[0]).toBe('━'.repeat(60));
      });
    });
  });
});
