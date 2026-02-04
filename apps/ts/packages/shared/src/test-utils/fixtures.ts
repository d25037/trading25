import type {
  JQuantsDailyQuote,
  JQuantsDailyQuotesResponse,
  JQuantsIndex,
  JQuantsIndicesResponse,
  JQuantsListedInfo,
  JQuantsListedInfoResponse,
  JQuantsStatement,
  JQuantsStatementsResponse,
  JQuantsTOPIX,
  JQuantsTOPIXResponse,
  JQuantsTradingCalendar,
  JQuantsTradingCalendarResponse,
  JQuantsWeeklyMarginInterest,
  JQuantsWeeklyMarginInterestResponse,
} from '../types/jquants';

export const mockListedInfo: JQuantsListedInfo = {
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
  Mrgn: '1',
  MrgnNm: 'プレミアム',
};

export const mockListedInfoResponse: JQuantsListedInfoResponse = {
  data: [mockListedInfo],
};

export const mockDailyQuote: JQuantsDailyQuote = {
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
  UL: 3500,
  LL: 2000,
};

export const mockDailyQuotesResponse: JQuantsDailyQuotesResponse = {
  data: [mockDailyQuote],
};

export const mockWeeklyMarginInterest: JQuantsWeeklyMarginInterest = {
  Date: '2025-01-10',
  Code: '7203',
  ShrtVol: 5000000,
  LongVol: 8000000,
  ShrtNegVol: 3000000,
  LongNegVol: 6000000,
  ShrtStdVol: 2000000,
  LongStdVol: 2000000,
  IssType: '1',
};

export const mockWeeklyMarginInterestResponse: JQuantsWeeklyMarginInterestResponse = {
  data: [mockWeeklyMarginInterest],
};

// Trading Calendar fixtures
export const mockTradingCalendar: JQuantsTradingCalendar = {
  Date: '2025-01-10',
  HolDiv: '0', // 0: Business day, 1: Holiday
};

export const mockTradingCalendarResponse: JQuantsTradingCalendarResponse = {
  data: [mockTradingCalendar],
};

// Index Data fixtures
export const mockIndex: JQuantsIndex = {
  Date: '2025-01-10',
  Code: '0028', // Nikkei 225
  O: 38500.0,
  H: 38650.0,
  L: 38420.0,
  C: 38580.0,
};

export const mockIndicesResponse: JQuantsIndicesResponse = {
  data: [mockIndex],
  pagination_key: 'next_page_key_123',
};

// TOPIX fixtures
export const mockTOPIX: JQuantsTOPIX = {
  Date: '2025-01-10',
  O: 2750.5,
  H: 2765.2,
  L: 2745.8,
  C: 2760.1,
};

export const mockTOPIXResponse: JQuantsTOPIXResponse = {
  data: [mockTOPIX],
  pagination_key: 'topix_next_key_456',
};

// Financial Statements fixtures (using v2 abbreviated field names)
export const mockStatement: JQuantsStatement = {
  DiscDate: '2023-01-30',
  DiscTime: '15:30:00',
  Code: '86970',
  DiscNo: '20230130401234',
  DocType: 'FY',
  CurPerType: 'FY',
  CurPerSt: '2022-04-01',
  CurPerEn: '2023-03-31',
  CurFYSt: '2022-04-01',
  CurFYEn: '2023-03-31',
  NxtFYSt: '2023-04-01',
  NxtFYEn: '2024-03-31',
  // Financial Performance
  Sales: 1000000,
  OP: 100000,
  OdP: 95000,
  NP: 70000,
  EPS: 350.5,
  DEPS: 345.2,
  // Financial Position
  TA: 5000000,
  Eq: 3000000,
  EqAR: 60.0,
  BPS: 1500.0,
  // Cash Flow
  CFO: 150000,
  CFI: -50000,
  CFF: -30000,
  CashEq: 500000,
  // Dividend Information - Results
  Div1Q: 0,
  Div2Q: 25.0,
  Div3Q: 0,
  DivFY: 30.0,
  DivAnn: 55.0,
  DivUnit: null,
  DivTotalAnn: 110000000,
  PayoutRatioAnn: 15.7,
  // Dividend Information - Forecasts
  FDiv1Q: 0,
  FDiv2Q: 30.0,
  FDiv3Q: 0,
  FDivFY: 30.0,
  FDivAnn: 60.0,
  FDivUnit: null,
  FDivTotalAnn: 120000000,
  FPayoutRatioAnn: 17.1,
  // Next Year Dividend Forecasts
  NxFDiv1Q: null,
  NxFDiv2Q: null,
  NxFDiv3Q: null,
  NxFDivFY: null,
  NxFDivAnn: null,
  NxFDivUnit: null,
  NxFPayoutRatioAnn: null,
  // Performance Forecasts - 2Q
  FSales2Q: null,
  FOP2Q: null,
  FOdP2Q: null,
  FNP2Q: null,
  FEPS2Q: null,
  // Next Year Performance Forecasts - 2Q
  NxFSales2Q: null,
  NxFOP2Q: null,
  NxFOdP2Q: null,
  NxFNp2Q: null,
  NxFEPS2Q: null,
  // Performance Forecasts - FY
  FSales: 1100000,
  FOP: 110000,
  FOdP: 105000,
  FNP: 77000,
  FEPS: 385.0,
  // Next Year Performance Forecasts - FY
  NxFSales: null,
  NxFOP: null,
  NxFOdP: null,
  NxFNp: null,
  NxFEPS: null,
  // Changes and Adjustments
  MatChgSub: false,
  ChgByASRev: false,
  ChgNoASRev: false,
  ChgAcEst: false,
  RetroRst: false,
  // Share Information
  ShOutFY: 200000000,
  TrShFY: 0,
  AvgSh: 200000000,
  // Non-Consolidated Data
  NCSales: null,
  NCOP: null,
  NCOdP: null,
  NCNP: null,
  NCEPS: null,
  NCTA: null,
  NCEq: null,
  NCEqAR: null,
  NCBPS: null,
  // Non-Consolidated Forecasts - 2Q
  FNCSales2Q: null,
  FNCOP2Q: null,
  FNCOdP2Q: null,
  FNCNP2Q: null,
  FNCEPS2Q: null,
  // Non-Consolidated Next Year Forecasts - 2Q
  NxFNCSales2Q: null,
  NxFNCOP2Q: null,
  NxFNCOdP2Q: null,
  NxFNCNP2Q: null,
  NxFNCEPS2Q: null,
  // Non-Consolidated Forecasts - FY
  FNCSales: null,
  FNCOP: null,
  FNCOdP: null,
  FNCNP: null,
  FNCEPS: null,
  // Non-Consolidated Next Year Forecasts - FY
  NxFNCSales: null,
  NxFNCOP: null,
  NxFNCOdP: null,
  NxFNCNP: null,
  NxFNCEPS: null,
};

export const mockStatementsResponse: JQuantsStatementsResponse = {
  data: [mockStatement],
  pagination_key: 'statements_next_key_789',
};

export const mockJQuantsConfig = {
  apiKey: 'mock_api_key_12345',
};
