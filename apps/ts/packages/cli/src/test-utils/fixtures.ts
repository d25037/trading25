export const mockEngineStatus = {
  isRunning: false,
  mode: 'paper' as const,
  exchange: 'jquants',
  startedAt: new Date('2025-01-10T10:00:00Z'),
  activeStrategies: 2,
  totalTrades: 156,
  profitLoss: 12500,
};

export const mockEngineStatusRunning = {
  ...mockEngineStatus,
  isRunning: true,
};

export const mockAuthStatus = {
  authenticated: true,
  hasApiKey: true,
};

export const mockStockInfo = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      CoName: 'トヨタ自動車',
      CoNameEn: 'TOYOTA MOTOR CORPORATION',
      S17: '050',
      S17Nm: '自動車・輸送機',
      S33: '1050',
      S33Nm: '自動車',
      ScaleCat: 'TOPIX Large70',
      Mkt: '111',
      MktNm: 'プライム',
    },
  ],
};

export const mockDailyQuotes = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      O: 2750,
      H: 2780,
      L: 2740,
      C: 2765,
      Vo: 1250000,
      Va: 3453750000,
      AdjFactor: 1.0,
      AdjO: 2750,
      AdjH: 2780,
      AdjL: 2740,
      AdjC: 2765,
      AdjVo: 1250000,
    },
  ],
};

export const mockMarginInterest = {
  data: [
    {
      Date: '2025-01-10',
      Code: '7203',
      ShrtVol: 5000000,
      LongVol: 8000000,
      ShrtNegVol: 3000000,
      LongNegVol: 6000000,
      ShrtStdVol: 2000000,
      LongStdVol: 2000000,
      IssType: '1',
    },
  ],
};

export const mockTOPIXData = {
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

export const mockIndicesData = {
  data: [
    {
      Date: '2025-01-10',
      Code: '0000',
      O: 1500.5,
      H: 1520.3,
      L: 1495.2,
      C: 1510.8,
    },
  ],
};

export const mockTokens = {
  API_KEY: 'mock_api_key_123',
};

export const mockCsvData = `Date,Open,High,Low,Close
2025-01-10,2359.28,2380.1,2335.58,2378.79
2025-01-11,2387.88,2400.53,2382.79,2393.54`;

export const mockFetchOptions = {
  from: '2025-01-01',
  to: '2025-01-31',
  csv: false,
  json: true,
  output: './test-data',
  date: '2025-01-10',
  code: '7203',
};
