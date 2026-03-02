import { BacktestClient } from '@trading25/api-clients/backtest';

export function createBacktestClient(btUrl?: string): BacktestClient {
  return new BacktestClient({
    baseUrl: btUrl ?? process.env.BT_API_URL ?? 'http://localhost:3002',
  });
}
