import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import type { ScreeningResultItem } from '@trading25/shared/types/api-response-types';
import { formatResults } from './output-formatter';

const sampleResults: ScreeningResultItem[] = [
  {
    stockCode: '7203',
    companyName: 'Toyota Motor',
    scaleCategory: 'TOPIX Large70',
    sector33Name: '輸送用機器',
    matchedDate: '2026-01-07',
    bestStrategyName: 'range_break_v15',
    bestStrategyScore: 1.12,
    matchStrategyCount: 2,
    matchedStrategies: [
      {
        strategyName: 'range_break_v15',
        matchedDate: '2026-01-07',
        strategyScore: 1.12,
      },
      {
        strategyName: 'forward_eps_driven',
        matchedDate: '2026-01-06',
        strategyScore: null,
      },
    ],
  },
];

describe('screening output formatter', () => {
  let logSpy: ReturnType<typeof spyOn> | null = null;

  afterEach(() => {
    if (logSpy) {
      logSpy.mockRestore();
      logSpy = null;
    }
  });

  it('prints strategy-centric JSON output', async () => {
    logSpy = spyOn(console, 'log').mockImplementation(() => undefined);

    await formatResults(sampleResults, {
      format: 'json',
      verbose: false,
      debug: false,
    });

    const payloads = logSpy.mock.calls.map((call: unknown[]) => String(call[0] ?? ''));
    const jsonPayload = payloads.find((payload: string) => payload.includes('"bestStrategyName"'));

    expect(jsonPayload).toBeDefined();
    expect(jsonPayload as string).toContain('"bestStrategyName": "range_break_v15"');
    expect(jsonPayload as string).toContain('"matchStrategyCount": 2');
    expect(jsonPayload as string).not.toContain('rangeBreakFast');
    expect(jsonPayload as string).not.toContain('rangeBreakSlow');
  });
});
