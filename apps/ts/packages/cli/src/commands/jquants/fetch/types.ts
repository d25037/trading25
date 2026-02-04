export interface FetchOptions {
  date?: string;
  from?: string;
  to?: string;
  csv?: boolean;
  json?: boolean;
  output: string;
  code?: string;
}

export interface TestDataOptions {
  days: string;
}
