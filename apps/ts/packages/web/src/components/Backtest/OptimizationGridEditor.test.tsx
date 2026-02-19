import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { OptimizationGridEditor } from './OptimizationGridEditor';

const mockSaveMutate = vi.fn();
const mockDeleteMutate = vi.fn();

const DEFAULT_GRID_CONTENT = `parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 20]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0, 2.5]
`;

function createDefaultGridConfig() {
  return {
    strategy_name: 'demo',
    content: DEFAULT_GRID_CONTENT,
    param_count: 2,
    combinations: 6,
  };
}

const mockHookState = {
  gridConfig: createDefaultGridConfig() as {
    strategy_name: string;
    content: string;
    param_count: number;
    combinations: number;
  } | null,
  isLoading: false,
  isError: false,
  savePending: false,
  saveError: null as Error | null,
  saveData: null as { strategy_name: string; param_count: number; combinations: number } | null,
  deletePending: false,
  deleteError: null as Error | null,
};

vi.mock('@/hooks/useOptimization', () => ({
  useOptimizationGridConfig: () => ({
    data: mockHookState.gridConfig,
    isLoading: mockHookState.isLoading,
    isError: mockHookState.isError,
  }),
  useSaveOptimizationGrid: () => ({
    mutate: mockSaveMutate,
    isPending: mockHookState.savePending,
    isError: !!mockHookState.saveError,
    error: mockHookState.saveError,
    data: mockHookState.saveData,
  }),
  useDeleteOptimizationGrid: () => ({
    mutate: mockDeleteMutate,
    isPending: mockHookState.deletePending,
    isError: !!mockHookState.deleteError,
    error: mockHookState.deleteError,
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
      <button type="button" onClick={() => onCopySnippet('mock_signal:\n  period: 14')}>
        Insert Mock Signal
      </button>
    </div>
  ),
}));

describe('OptimizationGridEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHookState.gridConfig = createDefaultGridConfig();
    mockHookState.isLoading = false;
    mockHookState.isError = false;
    mockHookState.savePending = false;
    mockHookState.saveError = null;
    mockHookState.saveData = null;
    mockHookState.deletePending = false;
    mockHookState.deleteError = null;
  });

  async function openEditorDialog(basename = 'demo') {
    const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: /Open Editor/i }));
    await screen.findByText(`Optimization Grid Editor: ${basename}`);
    return user;
  }

  it('renders loading state while grid config is being fetched', () => {
    mockHookState.isLoading = true;

    render(<OptimizationGridEditor strategyName="production/demo" />);

    expect(screen.queryByRole('button', { name: /Open Editor/i })).not.toBeInTheDocument();
  });

  it('renders Current/Saved/State and opens popup editor with signal list', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    expect(screen.getByText('Current')).toBeInTheDocument();
    expect(screen.getByText('Saved')).toBeInTheDocument();
    expect(screen.getByText('State')).toBeInTheDocument();
    expect(screen.getByText('2 params / 6 combos')).toBeInTheDocument();
    expect(screen.getByText('Synced')).toBeInTheDocument();

    await openEditorDialog();

    expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
      'parameter_ranges'
    );
    expect(screen.getByText('Signal Reference')).toBeInTheDocument();
  });

  it('shows YAML parse error and disables save', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    await openEditorDialog();

    const editor = screen.getByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, { target: { value: 'parameter_ranges: [invalid' } });

    expect(await screen.findByText(/YAML parse error:/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('shows warning when parameter_ranges is missing but allows save', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    await openEditorDialog();

    const editor = screen.getByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, { target: { value: 'foo: 1' } });

    expect(
      screen.getByText('Missing "parameter_ranges" key. Saving is allowed, but optimization combinations will be 0.')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();
  });

  it('shows warning when parameter arrays are not detected', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    await openEditorDialog();

    const editor = screen.getByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, {
      target: {
        value: `parameter_ranges:
  entry_filter_params:
    rsi:
      period:
        min: 10
`,
      },
    });

    expect(
      screen.getByText('No parameter arrays found under "parameter_ranges". Add list values like period: [10, 20, 30].')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();
  });

  it('saves with basename strategy key and edited content', async () => {
    mockSaveMutate.mockImplementation((_payload: unknown, options?: { onSuccess?: () => void }) => {
      options?.onSuccess?.();
    });

    render(<OptimizationGridEditor strategyName="production/demo" />);

    const user = await openEditorDialog();

    const editor = screen.getByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, {
      target: {
        value: `parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [5, 10]
`,
      },
    });

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockSaveMutate).toHaveBeenCalledWith(
      {
        strategy: 'demo',
        request: {
          content: expect.stringContaining('period: [5, 10]'),
        },
      },
      expect.any(Object)
    );
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('shows fallback template when no persisted grid config exists', async () => {
    mockHookState.gridConfig = null;
    mockHookState.isError = true;

    render(<OptimizationGridEditor strategyName="production/no-grid" />);

    expect(screen.getByText('Not saved')).toBeInTheDocument();
    expect(screen.getByText(/No saved grid config exists yet\./)).toBeInTheDocument();

    await openEditorDialog('no-grid');

    const editor = screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement;
    expect(editor.value).toContain('period: [10, 15, 20, 25, 30]');
    expect(screen.queryByRole('button', { name: 'Delete' })).not.toBeInTheDocument();
  });

  it('resets to baseline and deletes persisted config from dialog actions', async () => {
    mockDeleteMutate.mockImplementation((_strategy: string, options?: { onSuccess?: () => void }) => {
      options?.onSuccess?.();
    });

    render(<OptimizationGridEditor strategyName="production/demo" />);
    const user = await openEditorDialog();

    fireEvent.change(screen.getByLabelText('Optimization YAML Editor'), {
      target: { value: 'parameter_ranges:\n  test:\n    period: [1, 2]\n' },
    });
    expect(screen.getAllByText('Unsaved changes').length).toBeGreaterThan(0);

    await user.click(screen.getByRole('button', { name: 'Reset' }));
    expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
      'period_breakout'
    );

    await user.click(screen.getByRole('button', { name: 'Delete' }));
    expect(mockDeleteMutate).toHaveBeenCalledWith('demo', expect.any(Object));
    expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
      'period: [10, 15, 20, 25, 30]'
    );
  });

  it('prefers latest save summary when save response matches current strategy', () => {
    mockHookState.saveData = {
      strategy_name: 'demo',
      param_count: 9,
      combinations: 81,
    };

    render(<OptimizationGridEditor strategyName="production/demo" />);

    expect(screen.getByText('9 params, 81 combinations')).toBeInTheDocument();
  });

  it('shows mutation error banners inside popup editor', async () => {
    mockHookState.saveError = new Error('save failed');
    mockHookState.deleteError = new Error('delete failed');

    render(<OptimizationGridEditor strategyName="production/demo" />);
    await openEditorDialog();

    expect(screen.getByText('save failed')).toBeInTheDocument();
    expect(screen.getByText('delete failed')).toBeInTheDocument();
  });

  it('inserts signal snippet from popup signal reference and keeps unsaved state', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    const user = await openEditorDialog();
    await user.click(screen.getByRole('button', { name: 'Insert Mock Signal' }));

    await waitFor(() => {
      expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain('mock_signal');
    });

    expect(screen.getAllByText('Unsaved changes').length).toBeGreaterThan(0);
  });
});
