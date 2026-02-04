import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { BacktestPage } from './BacktestPage';

const mockBacktestState = {
  activeSubTab: 'runner' as string,
  setActiveSubTab: vi.fn(),
};

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => mockBacktestState,
}));

vi.mock('@/components/Backtest', () => ({
  BacktestRunner: () => <div>Runner Panel</div>,
  BacktestResults: () => <div>Results Panel</div>,
  BacktestStrategies: () => <div>Strategies Panel</div>,
  BacktestStatus: () => <div>Status Panel</div>,
  DatasetManager: () => <div>Dataset Panel</div>,
}));

describe('BacktestPage', () => {
  it('shows the active sub-tab content', () => {
    mockBacktestState.activeSubTab = 'runner';
    const { rerender } = render(<BacktestPage />);

    expect(screen.getByText('Runner Panel')).toBeInTheDocument();

    mockBacktestState.activeSubTab = 'results';
    rerender(<BacktestPage />);
    expect(screen.getByText('Results Panel')).toBeInTheDocument();
  });

  it('calls store action when sub-tab is clicked', async () => {
    const user = userEvent.setup();
    mockBacktestState.activeSubTab = 'runner';

    render(<BacktestPage />);

    await user.click(screen.getByRole('button', { name: /Results/i }));
    expect(mockBacktestState.setActiveSubTab).toHaveBeenCalledWith('results');
  });
});
