import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { StrategyOptimizationStateResponse } from '@/types/backtest';
import { OptimizationSpecEditor } from './OptimizationGridEditor';

const mockSaveMutate = vi.fn();
const mockDeleteMutate = vi.fn();
const mockGenerateMutate = vi.fn();

const DEFAULT_SPEC_CONTENT = `description: saved
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [10, 20]
`;

function createSavedState(): StrategyOptimizationStateResponse {
  return {
    strategy_name: 'production/demo',
    persisted: true,
    source: 'saved',
    optimization: {
      description: 'saved',
      parameter_ranges: {
        entry_filter_params: {
          period_extrema_break: { period: [10, 20] },
        },
      },
    },
    yaml_content: DEFAULT_SPEC_CONTENT,
    valid: true,
    ready_to_run: true,
    param_count: 1,
    combinations: 2,
    errors: [],
    warnings: [],
    drift: [],
  };
}

const mockHookState = {
  strategyOptimization: createSavedState() as ReturnType<typeof createSavedState> | null,
  isLoading: false,
  savePending: false,
  saveError: null as Error | null,
  deletePending: false,
  deleteError: null as Error | null,
  generatePending: false,
  generateError: null as Error | null,
};

vi.mock('@/hooks/useOptimization', () => ({
  useStrategyOptimization: () => ({
    data: mockHookState.strategyOptimization,
    isLoading: mockHookState.isLoading,
  }),
  useSaveStrategyOptimization: () => ({
    mutate: mockSaveMutate,
    isPending: mockHookState.savePending,
    isError: !!mockHookState.saveError,
    error: mockHookState.saveError,
  }),
  useDeleteStrategyOptimization: () => ({
    mutate: mockDeleteMutate,
    isPending: mockHookState.deletePending,
    isError: !!mockHookState.deleteError,
    error: mockHookState.deleteError,
  }),
  useGenerateStrategyOptimizationDraft: () => ({
    mutate: mockGenerateMutate,
    isPending: mockHookState.generatePending,
    isError: !!mockHookState.generateError,
    error: mockHookState.generateError,
  }),
}));

vi.mock('@/components/Editor/MonacoYamlEditor', () => ({
  MonacoYamlEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea aria-label="Optimization YAML Editor" value={value} onChange={(event) => onChange(event.target.value)} />
  ),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
}));

vi.mock('./SignalReferencePanel', () => ({
  SignalReferencePanel: ({ onCopySnippet }: { onCopySnippet: (snippet: string) => void }) => (
    <div>
      <div>Signal Reference</div>
      <button type="button" onClick={() => onCopySnippet('entry_filter_params:\n  mock_signal:\n    period: [5, 10]')}>
        Insert Mock Signal
      </button>
    </div>
  ),
}));

describe('OptimizationSpecEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHookState.strategyOptimization = createSavedState();
    mockHookState.isLoading = false;
    mockHookState.savePending = false;
    mockHookState.saveError = null;
    mockHookState.deletePending = false;
    mockHookState.deleteError = null;
    mockHookState.generatePending = false;
    mockHookState.generateError = null;
  });

  async function openEditorDialog(strategyName = 'production/demo') {
    const user = userEvent.setup();
    render(<OptimizationSpecEditor strategyName={strategyName} />);
    await user.click(screen.getByRole('button', { name: /Open Editor/i }));
    await screen.findByText(`Optimization Spec Editor: ${strategyName}`);
    return user;
  }

  it('renders loading state while optimization state is fetched', () => {
    mockHookState.isLoading = true;
    mockHookState.strategyOptimization = null;

    render(<OptimizationSpecEditor strategyName="production/demo" />);

    expect(screen.queryByRole('button', { name: /Open Editor/i })).not.toBeInTheDocument();
  });

  it('renders saved summary and opens popup editor', async () => {
    const user = await openEditorDialog();

    expect(screen.getByText('Optimization Spec')).toBeInTheDocument();
    expect(screen.getAllByText('Saved').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Ready to Run/).length).toBeGreaterThan(0);
    expect(screen.getAllByText('1 params / 2 combos').length).toBeGreaterThan(0);
    expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
      'parameter_ranges'
    );
    expect(screen.getByText('Signal Reference')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Close' }));
    expect(screen.queryByText('Optimization Spec Editor: production/demo')).not.toBeInTheDocument();
  });

  it('shows YAML parse error and disables save', async () => {
    await openEditorDialog();

    fireEvent.change(screen.getByLabelText('Optimization YAML Editor'), {
      target: { value: 'parameter_ranges: [invalid' },
    });

    await waitFor(() => {
      expect(screen.getAllByText(/YAML parse error:/).length).toBeGreaterThan(0);
    });
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('generates draft using the full strategy name', async () => {
    mockGenerateMutate.mockImplementation((_strategy: string, options?: { onSuccess?: (value: ReturnType<typeof createSavedState>) => void }) => {
      options?.onSuccess?.({
        ...createSavedState(),
        persisted: false,
        source: 'draft',
        ready_to_run: false,
        yaml_content: 'description: draft\nparameter_ranges:\n  exit_trigger_params:\n    atr_stop:\n      atr_multiplier: [1.5, 2.0]\n',
      });
    });

    render(<OptimizationSpecEditor strategyName="production/demo" />);

    await userEvent.click(screen.getByRole('button', { name: /Generate Draft from Strategy/i }));

    expect(mockGenerateMutate).toHaveBeenCalledWith('production/demo', expect.any(Object));
    expect(screen.getByText('Generated draft (unsaved)')).toBeInTheDocument();
  });

  it('saves edited YAML using strategy-scoped payload', async () => {
    mockSaveMutate.mockImplementation(
      (
        _payload: unknown,
        options?: { onSuccess?: (value: ReturnType<typeof createSavedState>) => void }
      ) => {
        options?.onSuccess?.({
          ...createSavedState(),
          combinations: 4,
          yaml_content: 'description: updated\nparameter_ranges:\n  entry_filter_params:\n    period_extrema_break:\n      period: [5, 10, 15, 20]\n',
        });
      }
    );

    const user = await openEditorDialog();

    fireEvent.change(screen.getByLabelText('Optimization YAML Editor'), {
      target: {
        value: `description: updated
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [5, 10, 15, 20]
`,
      },
    });

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockSaveMutate).toHaveBeenCalledWith(
      {
        strategy: 'production/demo',
        request: {
          yaml_content: expect.stringContaining('period: [5, 10, 15, 20]'),
        },
      },
      expect.any(Object)
    );
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('deletes persisted optimization and resets to empty spec', async () => {
    mockDeleteMutate.mockImplementation((_strategy: string, options?: { onSuccess?: () => void }) => {
      options?.onSuccess?.();
    });

    const user = await openEditorDialog();

    await user.click(screen.getByRole('button', { name: 'Delete' }));

    expect(mockDeleteMutate).toHaveBeenCalledWith('production/demo', expect.any(Object));
    await waitFor(() => {
      expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
        'parameter_ranges: {}'
      );
    });
  });

  it('inserts signal reference snippet and keeps unsaved state', async () => {
    const user = await openEditorDialog();

    await user.click(screen.getByRole('button', { name: 'Insert Mock Signal' }));

    await waitFor(() => {
      expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain('mock_signal');
    });

    expect(screen.getAllByText('Unsaved changes').length).toBeGreaterThan(0);
  });

  it('shows warning details and mutation error banners inside the editor', async () => {
    mockHookState.strategyOptimization = {
      ...createSavedState(),
      ready_to_run: false,
      warnings: [{ path: 'optimization.parameter_ranges.entry_filter_params', message: 'warning detail' }],
      drift: [{ path: 'optimization.parameter_ranges.entry_filter_params', message: 'drift detail' }],
    };
    mockHookState.generateError = new Error('draft failed');
    mockHookState.saveError = new Error('save failed');
    mockHookState.deleteError = new Error('delete failed');

    await openEditorDialog();

    expect(screen.getAllByText(/warning detail/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/drift detail/).length).toBeGreaterThan(0);
    expect(screen.getByText('draft failed')).toBeInTheDocument();
    expect(screen.getByText('save failed')).toBeInTheDocument();
    expect(screen.getByText('delete failed')).toBeInTheDocument();
  });

  it('resets unsaved edits back to the saved baseline', async () => {
    const user = await openEditorDialog();

    fireEvent.change(screen.getByLabelText('Optimization YAML Editor'), {
      target: {
        value: `description: changed
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [99]
`,
      },
    });

    expect(screen.getAllByText('Unsaved changes').length).toBeGreaterThan(0);

    await user.click(screen.getByRole('button', { name: 'Reset' }));

    await waitFor(() => {
      expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
        'period: [10, 20]'
      );
    });
    expect(screen.queryByText('Unsaved changes')).not.toBeInTheDocument();
  });
});
