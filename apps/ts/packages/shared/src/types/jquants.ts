// JQuants API v2 Types based on official documentation

// Authentication Types (v2 uses API key only)
export interface JQuantsConfig {
  apiKey: string;
}

// Listed Stock Information Types (eq-master)
export interface JQuantsListedInfo {
  Date: string; // YYYY-MM-DD
  Code: string;
  CoName: string;
  CoNameEn: string;
  S17: string; // Sector17Code
  S17Nm: string; // Sector17CodeName
  S33: string; // Sector33Code
  S33Nm: string; // Sector33CodeName
  ScaleCat: string; // ScaleCategory
  Mkt: string; // MarketCode
  MktNm: string; // MarketCodeName
  Mrgn?: string; // MarginCode (Standard/Premium plan only)
  MrgnNm?: string; // MarginCodeName (Standard/Premium plan only)
}

export interface JQuantsListedInfoResponse {
  data: JQuantsListedInfo[];
  pagination_key?: string;
}

// Daily Stock Quotes Types (eq-bars-daily)
export interface JQuantsDailyQuote {
  Date: string; // YYYY-MM-DD
  Code: string;
  O: number | null; // Open
  H: number | null; // High
  L: number | null; // Low
  C: number | null; // Close
  UL: number | null; // Upper Limit
  LL: number | null; // Lower Limit
  Vo: number | null; // Volume
  Va: number | null; // Value (TurnoverValue)
  AdjFactor: number; // Adjustment Factor
  AdjO: number | null; // Adjusted Open
  AdjH: number | null; // Adjusted High
  AdjL: number | null; // Adjusted Low
  AdjC: number | null; // Adjusted Close
  AdjVo: number | null; // Adjusted Volume
  // Morning session (Premium plan)
  MO?: number | null; // Morning Open
  MH?: number | null; // Morning High
  ML?: number | null; // Morning Low
  MC?: number | null; // Morning Close
  MUL?: number | null; // Morning Upper Limit
  MLL?: number | null; // Morning Lower Limit
  MVo?: number | null; // Morning Volume
  MVa?: number | null; // Morning Value
  MAdjO?: number | null; // Morning Adjusted Open
  MAdjH?: number | null; // Morning Adjusted High
  MAdjL?: number | null; // Morning Adjusted Low
  MAdjC?: number | null; // Morning Adjusted Close
  MAdjVo?: number | null; // Morning Adjusted Volume
  // Afternoon session (Premium plan)
  AO?: number | null; // Afternoon Open
  AH?: number | null; // Afternoon High
  AL?: number | null; // Afternoon Low
  AC?: number | null; // Afternoon Close
  AUL?: number | null; // Afternoon Upper Limit
  ALL?: number | null; // Afternoon Lower Limit
  AVo?: number | null; // Afternoon Volume
  AVa?: number | null; // Afternoon Value
  AAdjO?: number | null; // Afternoon Adjusted Open
  AAdjH?: number | null; // Afternoon Adjusted High
  AAdjL?: number | null; // Afternoon Adjusted Low
  AAdjC?: number | null; // Afternoon Adjusted Close
  AAdjVo?: number | null; // Afternoon Adjusted Volume
}

export interface JQuantsDailyQuotesResponse {
  data: JQuantsDailyQuote[];
  pagination_key?: string;
}

// Weekly Margin Interest Types (mkt-margin-int)
export interface JQuantsWeeklyMarginInterest {
  Date: string; // YYYY-MM-DD
  Code: string;
  ShrtVol: number; // Short Margin Trade Volume
  LongVol: number; // Long Margin Trade Volume
  ShrtNegVol: number; // Short Negotiable Margin Trade Volume
  LongNegVol: number; // Long Negotiable Margin Trade Volume
  ShrtStdVol: number; // Short Standardized Margin Trade Volume
  LongStdVol: number; // Long Standardized Margin Trade Volume
  IssType: string; // Issue Type: "1": Credit, "2": Lending/Borrowing, "3": Other
}

export interface JQuantsWeeklyMarginInterestResponse {
  data: JQuantsWeeklyMarginInterest[];
  pagination_key?: string;
}

// Query Parameters Types
export interface JQuantsListedInfoParams {
  code?: string; // 4-5 digit stock code
  date?: string; // YYYYMMDD or YYYY-MM-DD
  pagination_key?: string;
}

export interface JQuantsDailyQuotesParams {
  code?: string;
  from?: string; // YYYYMMDD or YYYY-MM-DD
  to?: string; // YYYYMMDD or YYYY-MM-DD
  date?: string; // YYYYMMDD or YYYY-MM-DD
  pagination_key?: string;
}

export interface JQuantsWeeklyMarginInterestParams {
  code?: string;
  from?: string; // YYYYMMDD or YYYY-MM-DD
  to?: string; // YYYYMMDD or YYYY-MM-DD
  date?: string; // YYYYMMDD or YYYY-MM-DD
  pagination_key?: string;
}

// Trading Calendar Types (mkt-cal)
export interface JQuantsTradingCalendar {
  Date: string; // YYYY-MM-DD
  HolDiv: string; // Holiday Division
}

export interface JQuantsTradingCalendarResponse {
  data: JQuantsTradingCalendar[];
}

export interface JQuantsTradingCalendarParams {
  hol_div?: string; // v2 parameter name
  from?: string; // YYYYMMDD or YYYY-MM-DD
  to?: string; // YYYYMMDD or YYYY-MM-DD
}

// Index Data Types (idx-bars-daily)
export interface JQuantsIndex {
  Date: string; // YYYY-MM-DD
  Code: string;
  O: number | null; // Open
  H: number | null; // High
  L: number | null; // Low
  C: number | null; // Close
}

export interface JQuantsIndicesResponse {
  data: JQuantsIndex[];
  pagination_key?: string;
}

export interface JQuantsIndicesParams {
  code?: string;
  date?: string; // YYYYMMDD or YYYY-MM-DD
  from?: string; // YYYYMMDD or YYYY-MM-DD
  to?: string; // YYYYMMDD or YYYY-MM-DD
  pagination_key?: string;
}

// TOPIX Types (idx-bars-daily-topix)
export interface JQuantsTOPIX {
  Date: string; // YYYY-MM-DD
  O: number; // Open
  H: number; // High
  L: number; // Low
  C: number; // Close
}

export interface JQuantsTOPIXResponse {
  data: JQuantsTOPIX[];
  pagination_key?: string;
}

export interface JQuantsTOPIXParams {
  from?: string; // YYYYMMDD or YYYY-MM-DD
  to?: string; // YYYYMMDD or YYYY-MM-DD
  pagination_key?: string;
}

// Financial Statements Types (fin-summary)
export interface JQuantsStatement {
  DiscDate: string; // Disclosed Date (YYYY-MM-DD)
  DiscTime: string; // Disclosed Time (HH:MM:SS)
  Code: string; // Stock Code
  DiscNo: string; // Disclosure Number
  DocType: string; // Document Type (Q1, Q2, Q3, FY, etc.)
  CurPerType: string; // Current Period Type
  CurPerSt: string; // Current Period Start Date
  CurPerEn: string; // Current Period End Date
  CurFYSt: string; // Current Fiscal Year Start Date
  CurFYEn: string; // Current Fiscal Year End Date
  NxtFYSt: string | null; // Next Fiscal Year Start Date
  NxtFYEn: string | null; // Next Fiscal Year End Date
  // Financial Performance
  Sales: number | null; // Net Sales
  OP: number | null; // Operating Profit
  OdP: number | null; // Ordinary Profit
  NP: number | null; // Net Profit
  EPS: number | null; // Earnings Per Share
  DEPS: number | null; // Diluted EPS
  // Financial Position
  TA: number | null; // Total Assets
  Eq: number | null; // Equity
  EqAR: number | null; // Equity to Asset Ratio
  BPS: number | null; // Book Value Per Share
  // Cash Flow
  CFO: number | null; // Cash Flow from Operating Activities
  CFI: number | null; // Cash Flow from Investing Activities
  CFF: number | null; // Cash Flow from Financing Activities
  CashEq: number | null; // Cash and Equivalents
  // Dividend Information - Results
  Div1Q: number | null; // Result Dividend Per Share 1st Quarter
  Div2Q: number | null; // Result Dividend Per Share 2nd Quarter
  Div3Q: number | null; // Result Dividend Per Share 3rd Quarter
  DivFY: number | null; // Result Dividend Per Share Fiscal Year End
  DivAnn: number | null; // Result Dividend Per Share Annual
  DivUnit: number | null; // Distributions Per Unit
  DivTotalAnn: number | null; // Result Total Dividend Paid Annual
  PayoutRatioAnn: number | null; // Result Payout Ratio Annual
  // Dividend Information - Forecasts
  FDiv1Q: number | null; // Forecast Dividend Per Share 1st Quarter
  FDiv2Q: number | null; // Forecast Dividend Per Share 2nd Quarter
  FDiv3Q: number | null; // Forecast Dividend Per Share 3rd Quarter
  FDivFY: number | null; // Forecast Dividend Per Share Fiscal Year End
  FDivAnn: number | null; // Forecast Dividend Per Share Annual
  FDivUnit: number | null; // Forecast Distributions Per Unit
  FDivTotalAnn: number | null; // Forecast Total Dividend Paid Annual
  FPayoutRatioAnn: number | null; // Forecast Payout Ratio Annual
  // Next Year Dividend Forecasts
  NxFDiv1Q: number | null;
  NxFDiv2Q: number | null;
  NxFDiv3Q: number | null;
  NxFDivFY: number | null;
  NxFDivAnn: number | null;
  NxFDivUnit: number | null;
  NxFPayoutRatioAnn: number | null;
  // Performance Forecasts - 2Q
  FSales2Q: number | null;
  FOP2Q: number | null;
  FOdP2Q: number | null;
  FNP2Q: number | null;
  FEPS2Q: number | null;
  // Next Year Performance Forecasts - 2Q
  NxFSales2Q: number | null;
  NxFOP2Q: number | null;
  NxFOdP2Q: number | null;
  NxFNp2Q: number | null;
  NxFEPS2Q: number | null;
  // Performance Forecasts - FY
  FSales: number | null;
  FOP: number | null;
  FOdP: number | null;
  FNP: number | null;
  FEPS: number | null;
  // Next Year Performance Forecasts - FY
  NxFSales: number | null;
  NxFOP: number | null;
  NxFOdP: number | null;
  NxFNp: number | null;
  NxFEPS: number | null;
  // Changes and Adjustments
  MatChgSub: boolean | null; // Material Changes In Subsidiaries
  ChgByASRev: boolean | null; // Changes by Accounting Standards Revision
  ChgNoASRev: boolean | null; // Changes Not by Accounting Standards Revision
  ChgAcEst: boolean | null; // Changes In Accounting Estimates
  RetroRst: boolean | null; // Retrospective Restatement
  // Share Information
  ShOutFY: number | null; // Shares Outstanding at FY End (incl. Treasury)
  TrShFY: number | null; // Treasury Stock at FY End
  AvgSh: number | null; // Average Number of Shares
  // Non-Consolidated Data
  NCSales: number | null;
  NCOP: number | null;
  NCOdP: number | null;
  NCNP: number | null;
  NCEPS: number | null;
  NCTA: number | null;
  NCEq: number | null;
  NCEqAR: number | null;
  NCBPS: number | null;
  // Non-Consolidated Forecasts - 2Q
  FNCSales2Q: number | null;
  FNCOP2Q: number | null;
  FNCOdP2Q: number | null;
  FNCNP2Q: number | null;
  FNCEPS2Q: number | null;
  // Non-Consolidated Next Year Forecasts - 2Q
  NxFNCSales2Q: number | null;
  NxFNCOP2Q: number | null;
  NxFNCOdP2Q: number | null;
  NxFNCNP2Q: number | null;
  NxFNCEPS2Q: number | null;
  // Non-Consolidated Forecasts - FY
  FNCSales: number | null;
  FNCOP: number | null;
  FNCOdP: number | null;
  FNCNP: number | null;
  FNCEPS: number | null;
  // Non-Consolidated Next Year Forecasts - FY
  NxFNCSales: number | null;
  NxFNCOP: number | null;
  NxFNCOdP: number | null;
  NxFNCNP: number | null;
  NxFNCEPS: number | null;
}

export interface JQuantsStatementsResponse {
  data: JQuantsStatement[];
  pagination_key?: string;
}

export interface JQuantsStatementsParams {
  code?: string; // 4-5 digit stock code. Either code or date is required
  date?: string; // YYYYMMDD or YYYY-MM-DD. Either code or date is required
  pagination_key?: string;
}

// API Error Response
export interface JQuantsErrorResponse {
  message: string;
  status: number;
}
