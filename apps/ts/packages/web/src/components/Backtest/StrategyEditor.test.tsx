import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { StrategyEditor } from './StrategyEditor';

const mockValidateMutateAsync = vi.fn();
const mockValidateReset = vi.fn();
const mockUpdateMutate = vi.fn();
const mockUpdateReset = vi.fn();

const mockBacktestHooks = {
  strategyData: {
    config: {
      entry_filter_params: {
        volume: {
          enabled: true,
        },
      },
    },
  } as { config: Record<string, unknown> } | null,
  isLoadingStrategy: false,
  validatePending: false,
  updatePending: false,
  updateError: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useStrategy: () => ({
    data: mockBacktestHooks.strategyData,
    isLoading: mockBacktestHooks.isLoadingStrategy,
  }),
  useValidateStrategy: () => ({
    mutateAsync: mockValidateMutateAsync,
    isPending: mockBacktestHooks.validatePending,
    reset: mockValidateReset,
  }),
  useUpdateStrategy: () => ({
    mutate: mockUpdateMutate,
    isPending: mockBacktestHooks.updatePending,
    isError: !!mockBacktestHooks.updateError,
    error: mockBacktestHooks.updateError,
    reset: mockUpdateReset,
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
      aria-label="YAML Editor"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  ),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({
    open,
    children,
  }: {
    open: boolean;
    children: ReactNode;
  }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
}));

vi.mock('@/components/ui/button', () => ({
  Button: ({
    children,
    onClick,
    disabled,
    variant,
  }: {
    children: ReactNode;
    onClick?: () => void;
    disabled?: boolean;
    variant?: string;
  }) => (
    <button type="button" onClick={onClick} disabled={disabled} data-variant={variant}>
      {children}
    </button>
  ),
}));

vi.mock('./SignalReferencePanel', () => ({
  SignalReferencePanel: () => <div>Signal Reference</div>,
}));

describe('StrategyEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockBacktestHooks.strategyData = {
      config: {
        entry_filter_params: {
          volume: {
            enabled: true,
          },
        },
      },
    };
    mockBacktestHooks.isLoadingStrategy = false;
    mockBacktestHooks.validatePending = false;
    mockBacktestHooks.updatePending = false;
    mockBacktestHooks.updateError = null;
    mockValidateMutateAsync.mockResolvedValue({
      valid: true,
      errors: [],
      warnings: [],
    });
  });

  it('blocks save when backend validation reports errors', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockResolvedValueOnce({
      valid: false,
      errors: ['entry_filter_params.fundamental.foward_eps_growth is not a valid parameter name'],
      warnings: [],
    });

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await waitFor(() => {
      expect((screen.getByLabelText('YAML Editor') as HTMLTextAreaElement).value).toContain('entry_filter_params');
    });

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockValidateMutateAsync).toHaveBeenCalledWith({
      name: 'experimental/sample',
      request: expect.objectContaining({
        config: expect.any(Object),
      }),
    });
    expect(mockUpdateMutate).not.toHaveBeenCalled();
    expect(
      await screen.findByText('entry_filter_params.fundamental.foward_eps_growth is not a valid parameter name')
    ).toBeInTheDocument();
  });

  it('blocks save when backend validation request fails', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockRejectedValueOnce(new Error('network down'));

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await waitFor(() => {
      expect((screen.getByLabelText('YAML Editor') as HTMLTextAreaElement).value).toContain('entry_filter_params');
    });

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).not.toHaveBeenCalled();
    expect(await screen.findByText('Validation request failed: network down')).toBeInTheDocument();
  });

  it('shows parse error and skips backend validation when YAML is invalid', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await waitFor(() => {
      expect((screen.getByLabelText('YAML Editor') as HTMLTextAreaElement).value).toContain('entry_filter_params');
    });

    await user.clear(screen.getByLabelText('YAML Editor'));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: { value: '{invalid' },
    });
    await user.click(screen.getByRole('button', { name: 'Validate' }));

    expect(mockValidateMutateAsync).not.toHaveBeenCalled();
    expect(await screen.findByText(/YAML parse error:/)).toBeInTheDocument();
  });

  it('shows validation passed with warnings from backend response', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockResolvedValueOnce({
      valid: true,
      errors: [],
      warnings: ['signal reference stale'],
    });

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Validate' }));

    expect(await screen.findByText('Validation passed with warnings')).toBeInTheDocument();
    expect(screen.getByText('signal reference stale')).toBeInTheDocument();
  });

  it('saves when backend validation passes', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSuccess = vi.fn();
    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(
      <StrategyEditor
        open
        onOpenChange={onOpenChange}
        strategyName="experimental/sample"
        onSuccess={onSuccess}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockValidateMutateAsync).toHaveBeenCalled();
    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: expect.objectContaining({
          config: expect.any(Object),
        }),
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
    expect(onSuccess).toHaveBeenCalled();
  });

  it('calls reset handlers on cancel', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(mockValidateReset).toHaveBeenCalled();
    expect(mockUpdateReset).toHaveBeenCalled();
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('renders loading state', () => {
    mockBacktestHooks.isLoadingStrategy = true;

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(screen.queryByLabelText('YAML Editor')).not.toBeInTheDocument();
  });

  it('renders update error banner', () => {
    mockBacktestHooks.updateError = new Error('update failed');

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(screen.getByText('Error: update failed')).toBeInTheDocument();
  });
});
