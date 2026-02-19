import { afterEach, describe, expect, it, mock, spyOn } from 'bun:test';
import type { ScreeningResultItem } from '@trading25/shared/types/api-response-types';

mock.module('chalk', () => {
  const identity = (text: string) => text;
  const bold = Object.assign((text: string) => text, { white: identity });

  return {
    default: {
      gray: identity,
      cyan: identity,
      white: identity,
      magenta: identity,
      yellow: identity,
      green: identity,
      bold,
    },
  };
});

import { formatResults } from './output-formatter.js';

const sampleResults: ScreeningResultItem[] = [
  {
    stockCode: '7203',
    companyName: 'Toyota "Motor", Inc.',
    scaleCategory: 'TOPIX Large70',
    sector33Name: '輸送用機器',
    matchedDate: '2026-01-07T09:00:00Z',
    bestStrategyName: 'range_break_v15',
    bestStrategyScore: 1.125,
    matchStrategyCount: 2,
    matchedStrategies: [
      {
        strategyName: 'range_break_v15',
        matchedDate: '2026-01-07',
        strategyScore: 1.125,
      },
      {
        strategyName: 'forward_eps_driven',
        matchedDate: '2026-01-06',
        strategyScore: null,
      },
    ],
  },
  {
    stockCode: '6758',
    companyName: 'Sony Group Corporation',
    scaleCategory: 'TOPIX Core30',
    sector33Name: '',
    matchedDate: '2026-01-08',
    bestStrategyName: 'forward_eps_driven',
    bestStrategyScore: null,
    matchStrategyCount: 1,
    matchedStrategies: [
      {
        strategyName: 'forward_eps_driven',
        matchedDate: '2026-01-08',
        strategyScore: null,
      },
    ],
  },
];

function getLogLines(logSpy: ReturnType<typeof spyOn>): string[] {
  return logSpy.mock.calls.map((call: unknown[]) => String(call[0] ?? ''));
}

describe('screening output formatter', () => {
  afterEach(() => {
    mock.restore();
  });

  it('prints strategy-centric JSON output with summary fields', async () => {
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);
    logSpy.mockClear();

    await formatResults(sampleResults, {
      format: 'json',
      verbose: false,
      debug: false,
    });

    const lines = getLogLines(logSpy);
    const jsonPayload = lines.find((line) => line.includes('"results"'));
    expect(jsonPayload).toBeDefined();
    expect(jsonPayload as string).toContain('"total": 2');
    expect(jsonPayload as string).toContain('"range_break_v15": 1');
    expect(jsonPayload as string).toContain('"noScoreCount": 1');
    expect(jsonPayload as string).toContain('"bestStrategyName": "range_break_v15"');
  });

  it('prints warning when table output has no result rows', async () => {
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);
    logSpy.mockClear();

    await formatResults([], {
      format: 'table',
      verbose: false,
      debug: false,
    });

    const lines = getLogLines(logSpy);
    expect(lines.at(-1)).toBe('No results to display');
  });

  it('prints table output with verbose matched strategies and summary', async () => {
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);
    logSpy.mockClear();

    await formatResults(sampleResults, {
      format: 'table',
      verbose: true,
      debug: false,
    });

    const lines = getLogLines(logSpy);
    expect(lines.some((line) => line.includes('Code'))).toBe(true);
    expect(lines.some((line) => line.includes('range_break_v15'))).toBe(true);
    expect(lines.some((line) => line.includes('matched: range_break_v15:1.125'))).toBe(true);
    expect(lines.some((line) => line.includes('Total: 2 stocks'))).toBe(true);
    expect(lines.some((line) => line.includes('Top:'))).toBe(true);
  });

  it('prints CSV output with escaped fields and verbose strategy details', async () => {
    const logSpy = spyOn(console, 'log').mockImplementation(() => undefined);
    logSpy.mockClear();

    await formatResults(sampleResults, {
      format: 'csv',
      verbose: true,
      debug: false,
    });

    const lines = getLogLines(logSpy);
    expect(lines[0]).toContain('MatchedStrategies');
    expect(lines[1]).toContain('"Toyota ""Motor"", Inc."');
    expect(lines[1]).toContain('"range_break_v15:1.125|forward_eps_driven:N/A"');
    expect(lines[2]).toContain('"forward_eps_driven",,1,"forward_eps_driven:N/A"');
  });

  it('throws for unsupported output format', async () => {
    await expect(
      formatResults(sampleResults, {
        format: 'xml' as 'table',
        verbose: false,
        debug: false,
      })
    ).rejects.toThrow('Unsupported format: xml');
  });
});
