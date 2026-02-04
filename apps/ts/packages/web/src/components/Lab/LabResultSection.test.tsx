import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { LabResultData } from '@/types/backtest';
import { LabResultSection } from './LabResultSection';

// Mock child result components
vi.mock('./LabGenerateResults', () => ({
  LabGenerateResults: ({ result }: { result: { total_generated: number } }) => (
    <div data-testid="generate-results">Generate: {result.total_generated}</div>
  ),
}));

vi.mock('./LabEvolveResults', () => ({
  LabEvolveResults: ({ result }: { result: { best_score: number } }) => (
    <div data-testid="evolve-results">Evolve: {result.best_score}</div>
  ),
}));

vi.mock('./LabOptimizeResults', () => ({
  LabOptimizeResults: ({ result }: { result: { total_trials: number } }) => (
    <div data-testid="optimize-results">Optimize: {result.total_trials}</div>
  ),
}));

vi.mock('./LabImproveResults', () => ({
  LabImproveResults: ({ result }: { result: { strategy_name: string } }) => (
    <div data-testid="improve-results">Improve: {result.strategy_name}</div>
  ),
}));

const validGenerateResult: LabResultData = {
  lab_type: 'generate',
  results: [
    {
      strategy_id: 'strat_001',
      score: 85.5,
      sharpe_ratio: 1.2,
      calmar_ratio: 2.1,
      total_return: 0.35,
      max_drawdown: -0.12,
      win_rate: 0.6,
      trade_count: 42,
      entry_signals: ['sma_cross'],
      exit_signals: ['rsi_overbought'],
    },
  ],
  total_generated: 100,
};

const validEvolveResult: LabResultData = {
  lab_type: 'evolve',
  best_strategy_id: 'evolved_001',
  best_score: 92.3,
  history: [{ generation: 1, best_score: 80.0, avg_score: 60.0, worst_score: 30.0 }],
};

const validOptimizeResult: LabResultData = {
  lab_type: 'optimize',
  best_score: 95.0,
  best_params: { sma_period: 20 },
  total_trials: 50,
  history: [{ trial: 1, score: 70.0, params: { sma_period: 10 } }],
};

const validImproveResult: LabResultData = {
  lab_type: 'improve',
  strategy_name: 'my_strategy',
  max_drawdown: -0.15,
  max_drawdown_duration_days: 30,
  suggested_improvements: [],
  improvements: [],
};

describe('LabResultSection', () => {
  const originalError = console.error;
  beforeEach(() => {
    console.error = vi.fn();
  });

  afterEach(() => {
    console.error = originalError;
  });

  it('renders generate results for valid generate data', () => {
    render(<LabResultSection resultData={validGenerateResult} />);
    expect(screen.getByTestId('generate-results')).toBeInTheDocument();
    expect(screen.getByText('Generate: 100')).toBeInTheDocument();
  });

  it('renders evolve results for valid evolve data', () => {
    render(<LabResultSection resultData={validEvolveResult} />);
    expect(screen.getByTestId('evolve-results')).toBeInTheDocument();
  });

  it('renders optimize results for valid optimize data', () => {
    render(<LabResultSection resultData={validOptimizeResult} />);
    expect(screen.getByTestId('optimize-results')).toBeInTheDocument();
  });

  it('renders improve results for valid improve data', () => {
    render(<LabResultSection resultData={validImproveResult} />);
    expect(screen.getByTestId('improve-results')).toBeInTheDocument();
  });

  it('shows validation error for invalid data', () => {
    const invalidData = { lab_type: 'generate', results: 'not_an_array' } as unknown as LabResultData;
    render(<LabResultSection resultData={invalidData} />);
    expect(screen.getByText('結果データの形式が不正です')).toBeInTheDocument();
  });

  it('shows validation error for data with missing required fields', () => {
    const invalidData = { lab_type: 'generate' } as unknown as LabResultData;
    render(<LabResultSection resultData={invalidData} />);
    expect(screen.getByText('結果データの形式が不正です')).toBeInTheDocument();
  });

  it('shows ErrorBoundary fallback when child component throws', async () => {
    const generateMod = await import('./LabGenerateResults');
    vi.spyOn(generateMod, 'LabGenerateResults').mockImplementation(() => {
      throw new Error('Render crash');
    });

    render(<LabResultSection resultData={validGenerateResult} />);
    expect(screen.getByText('結果の表示に失敗しました')).toBeInTheDocument();
    expect(screen.getByText('再試行')).toBeInTheDocument();

    vi.restoreAllMocks();
  });

  it('switches result component when resultData prop changes', () => {
    const { rerender } = render(<LabResultSection resultData={validGenerateResult} />);
    expect(screen.getByTestId('generate-results')).toBeInTheDocument();

    rerender(<LabResultSection resultData={validEvolveResult} />);
    expect(screen.getByTestId('evolve-results')).toBeInTheDocument();
    expect(screen.queryByTestId('generate-results')).not.toBeInTheDocument();
  });

  it('resets ErrorBoundary on retry button click', async () => {
    const user = userEvent.setup();
    let shouldThrow = true;

    const generateMod = await import('./LabGenerateResults');
    vi.spyOn(generateMod, 'LabGenerateResults').mockImplementation(() => {
      if (shouldThrow) {
        throw new Error('Render crash');
      }
      return <div data-testid="generate-results">Recovered</div>;
    });

    render(<LabResultSection resultData={validGenerateResult} />);
    expect(screen.getByText('結果の表示に失敗しました')).toBeInTheDocument();

    shouldThrow = false;
    const retryButton = screen.getByText('再試行');
    await user.click(retryButton);

    expect(screen.getByText('Recovered')).toBeInTheDocument();

    vi.restoreAllMocks();
  });
});
