import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { StrategyDetailResponse, StrategyMetadata } from '@/types/backtest';
import { BacktestStrategies } from './BacktestStrategies';

const mockUseStrategies = vi.fn();
const mockUseStrategy = vi.fn();

vi.mock('@/hooks/useBacktest', () => ({
  useStrategies: () => mockUseStrategies(),
  useStrategy: (name: string | null) => mockUseStrategy(name),
}));

vi.mock('./StrategyEditor', () => ({
  StrategyEditor: () => null,
}));

vi.mock('./DeleteConfirmDialog', () => ({
  DeleteConfirmDialog: () => null,
}));

vi.mock('./DuplicateDialog', () => ({
  DuplicateDialog: () => null,
}));

vi.mock('./MoveGroupDialog', () => ({
  MoveGroupDialog: () => null,
}));

vi.mock('./RenameDialog', () => ({
  RenameDialog: () => null,
}));

vi.mock('./OptimizationGridEditor', () => ({
  OptimizationGridEditor: () => <div>Optimization Grid</div>,
}));

const strategies: StrategyMetadata[] = [
  {
    name: 'production/range_break_v16',
    category: 'production',
    description: null,
    display_name: null,
    last_modified: '2026-02-17T00:00:00Z',
  },
  {
    name: 'experimental/demo_strategy',
    category: 'experimental',
    description: null,
    display_name: null,
    last_modified: '2026-02-17T00:00:00Z',
  },
  {
    name: 'reference/template_strategy',
    category: 'reference',
    description: null,
    display_name: null,
    last_modified: '2026-02-17T00:00:00Z',
  },
];

const details: Record<string, StrategyDetailResponse> = {
  'production/range_break_v16': {
    name: 'production/range_break_v16',
    category: 'production',
    description: null,
    display_name: null,
    config: { entry_filter_params: {} },
    execution_info: {},
  },
  'experimental/demo_strategy': {
    name: 'experimental/demo_strategy',
    category: 'experimental',
    description: 'demo description',
    display_name: null,
    config: { entry_filter_params: {} },
    execution_info: { status: 'ok' },
  },
  'reference/template_strategy': {
    name: 'reference/template_strategy',
    category: 'reference',
    description: null,
    display_name: null,
    config: { entry_filter_params: {} },
    execution_info: {},
  },
};

const mockHookState = {
  strategiesData: { strategies, total: strategies.length },
  strategiesLoading: false,
  strategyDetails: details as Record<string, StrategyDetailResponse | null>,
  detailLoading: false,
};

describe('BacktestStrategies', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHookState.strategiesData = { strategies, total: strategies.length };
    mockHookState.strategiesLoading = false;
    mockHookState.strategyDetails = details;
    mockHookState.detailLoading = false;

    mockUseStrategies.mockImplementation(() => ({
      data: mockHookState.strategiesData,
      isLoading: mockHookState.strategiesLoading,
    }));
    mockUseStrategy.mockImplementation((name: string | null) => ({
      data: name ? mockHookState.strategyDetails[name] : null,
      isLoading: mockHookState.detailLoading,
    }));
  });

  it('renders loading state while fetching strategies', () => {
    mockHookState.strategiesLoading = true;

    render(<BacktestStrategies />);

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('renders empty state when no strategies are available', () => {
    mockHookState.strategiesData = { strategies: [], total: 0 };

    render(<BacktestStrategies />);

    expect(screen.getByText('No strategies available')).toBeInTheDocument();
  });

  it('renders select hint when no strategy is selected', () => {
    render(<BacktestStrategies />);

    expect(screen.getByText('Select a strategy to view details')).toBeInTheDocument();
  });

  it('allows YAML editing for production strategy but keeps rename/delete disabled', async () => {
    const user = userEvent.setup();
    render(<BacktestStrategies />);

    await user.click(screen.getByText('production/range_break_v16'));

    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Rename' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete' })).not.toBeInTheDocument();
  });

  it('keeps full edit actions for experimental strategy', async () => {
    const user = userEvent.setup();
    render(<BacktestStrategies />);

    await user.click(screen.getByText('experimental/demo_strategy'));

    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Rename' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Delete' })).toBeInTheDocument();
  });

  it('hides edit actions for non-managed category strategy', async () => {
    const user = userEvent.setup();
    render(<BacktestStrategies />);

    await user.click(screen.getByText('reference/template_strategy'));

    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Rename' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete' })).not.toBeInTheDocument();
  });

  it('renders detail sections and optimize tab content', async () => {
    const user = userEvent.setup();
    render(<BacktestStrategies />);

    await user.click(screen.getByText('experimental/demo_strategy'));

    expect(screen.getByText('Description')).toBeInTheDocument();
    expect(screen.getByText('Configuration')).toBeInTheDocument();
    expect(screen.getByText('Execution Info')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Optimize' }));
    expect(screen.getByText('Optimization Grid')).toBeInTheDocument();
  });

  it('renders detail loading state when selected strategy is loading', async () => {
    const user = userEvent.setup();
    mockHookState.detailLoading = true;

    render(<BacktestStrategies />);
    await user.click(screen.getByText('experimental/demo_strategy'));

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('handles missing detail response without crashing', async () => {
    const user = userEvent.setup();
    mockHookState.strategyDetails = {
      ...details,
      'experimental/demo_strategy': null,
    };

    render(<BacktestStrategies />);
    await user.click(screen.getByText('experimental/demo_strategy'));

    expect(screen.queryByText('Configuration')).not.toBeInTheDocument();
  });
});
