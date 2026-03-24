import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BacktestPage } from './BacktestPage';

const mockBacktestState = {
  activeSubTab: 'runner' as string,
  setActiveSubTab: vi.fn(),
  selectedStrategy: null as string | null,
  setSelectedStrategy: vi.fn(),
  setSelectedResultJobId: vi.fn(),
  activeLabType: null as 'generate' | 'evolve' | 'optimize' | 'improve' | null,
  setActiveLabType: vi.fn(),
};

vi.mock('@/hooks/usePageRouteState', () => ({
  useBacktestRouteState: () => mockBacktestState,
  useMigrateBacktestRouteState: () => {},
}));

vi.mock('@/components/Backtest', () => ({
  BacktestRunner: () => <div>Runner Panel</div>,
  BacktestResults: () => <div>Results Panel</div>,
  BacktestAttribution: () => <div>Attribution Panel</div>,
  BacktestStrategies: () => <div>Strategies Panel</div>,
  BacktestStatus: ({ onViewJob }: { onViewJob: (jobId: string) => void }) => (
    <button type="button" onClick={() => onViewJob('job-123')}>
      Status Panel
    </button>
  ),
  DatasetManager: () => <div>Dataset Panel</div>,
}));

vi.mock('@/components/Lab', () => ({
  LabPanel: () => <div>Lab Panel</div>,
}));

describe('BacktestPage', () => {
  beforeEach(() => {
    mockBacktestState.setActiveSubTab.mockClear();
    mockBacktestState.setSelectedStrategy.mockClear();
    mockBacktestState.setSelectedResultJobId.mockClear();
    mockBacktestState.setActiveLabType.mockClear();
  });

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

  it('renders attribution tab content', () => {
    mockBacktestState.activeSubTab = 'attribution';
    render(<BacktestPage />);
    expect(screen.getByText('Attribution Panel')).toBeInTheDocument();
  });

  it('routes from status to results when a job is selected', async () => {
    const user = userEvent.setup();
    mockBacktestState.activeSubTab = 'status';

    render(<BacktestPage />);

    await user.click(screen.getByRole('button', { name: 'Status Panel' }));

    expect(mockBacktestState.setSelectedResultJobId).toHaveBeenCalledWith('job-123');
    expect(mockBacktestState.setActiveSubTab).toHaveBeenCalledWith('results');
  });

  it('renders remaining tab panels', () => {
    const matrix = [
      { tab: 'strategies', text: 'Strategies Panel' },
      { tab: 'dataset', text: 'Dataset Panel' },
      { tab: 'lab', text: 'Lab Panel' },
    ] as const;

    const { rerender } = render(<BacktestPage />);

    for (const item of matrix) {
      mockBacktestState.activeSubTab = item.tab;
      rerender(<BacktestPage />);
      expect(screen.getByText(item.text)).toBeInTheDocument();
    }
  });
});
