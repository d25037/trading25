import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { OptimizationGridEditor } from './OptimizationGridEditor';

const mockSaveMutate = vi.fn();
const mockDeleteMutate = vi.fn();

const mockHookState = {
  gridConfig: {
    strategy_name: 'demo',
    content: `parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 20]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0, 2.5]
`,
    param_count: 2,
    combinations: 6,
  },
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
  MonacoYamlEditor: ({
    value,
    onChange,
  }: {
    value: string;
    onChange: (value: string) => void;
  }) => (
    <textarea
      aria-label="Optimization YAML Editor"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  ),
}));

describe('OptimizationGridEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHookState.gridConfig = {
      strategy_name: 'demo',
      content: `parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [10, 20]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0, 2.5]
`,
      param_count: 2,
      combinations: 6,
    };
    mockHookState.isLoading = false;
    mockHookState.isError = false;
    mockHookState.savePending = false;
    mockHookState.saveError = null;
    mockHookState.saveData = null;
    mockHookState.deletePending = false;
    mockHookState.deleteError = null;
  });

  it('renders rich status and detected parameter previews', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    await waitFor(() => {
      expect((screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement).value).toContain(
        'parameter_ranges'
      );
    });

    expect(screen.getByText('Optimization Grid Editor')).toBeInTheDocument();
    expect(screen.getByText('2 params, 6 combinations')).toBeInTheDocument();
    expect(screen.getByText('entry_filter_params.period_breakout.period')).toBeInTheDocument();
    expect(screen.getByText('exit_trigger_params.atr_stop.atr_multiplier')).toBeInTheDocument();
    expect(screen.getByText('Ready: 2 parameters, 6 combinations detected.')).toBeInTheDocument();
  });

  it('shows YAML parse error and disables save', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    const editor = await screen.findByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, { target: { value: 'parameter_ranges: [invalid' } });

    expect(await screen.findByText(/YAML parse error:/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('shows warning when parameter_ranges is missing but allows save', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    const editor = await screen.findByLabelText('Optimization YAML Editor');
    fireEvent.change(editor, { target: { value: 'foo: 1' } });

    expect(
      screen.getByText('Missing "parameter_ranges" key. Saving is allowed, but optimization combinations will be 0.')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();
  });

  it('shows warning when parameter arrays are not detected', async () => {
    render(<OptimizationGridEditor strategyName="production/demo" />);

    const editor = await screen.findByLabelText('Optimization YAML Editor');
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
    const user = userEvent.setup();
    mockSaveMutate.mockImplementation((_payload: unknown, options?: { onSuccess?: () => void }) => {
      options?.onSuccess?.();
    });

    render(<OptimizationGridEditor strategyName="production/demo" />);

    const editor = await screen.findByLabelText('Optimization YAML Editor');
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

  it('applies preset template content from helper panel', async () => {
    const user = userEvent.setup();
    render(<OptimizationGridEditor strategyName="production/demo" />);

    await user.click(screen.getByRole('button', { name: /Breakout Focus/i }));

    const editor = screen.getByLabelText('Optimization YAML Editor') as HTMLTextAreaElement;
    expect(editor.value).toContain('volume_breakout');
    expect(editor.value).toContain('atr_multiplier');
  });
});
