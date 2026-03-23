import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AuthoringFieldSchema, SignalDefinition } from '@/types/backtest';
import { StrategyEditor } from './StrategyEditor';

const mockValidateMutateAsync = vi.fn();
const mockValidateReset = vi.fn();
const mockUpdateMutate = vi.fn();
const mockUpdateReset = vi.fn();

const compiledStrategy = {
  schema_version: 1,
  strategy_name: 'experimental/sample',
  execution_semantics: 'standard',
  timeframe: 'daily',
  signal_ids: ['entry.volume_ratio_above'],
  required_data_domains: ['market'],
  required_fundamental_fields: [],
  signals: [
    {
      signal_id: 'entry.volume_ratio_above',
      scope: 'entry',
      param_key: 'volume_ratio_above',
      signal_name: 'Volume Ratio Above',
      category: 'volume',
      description: 'volume ratio filter',
      data_requirements: ['market.volume'],
      availability: {
        observation_time: 'prior_session_close',
        available_at: 'prior_session_close',
        decision_cutoff: 'prior_session_close',
        execution_session: 'current_session',
      },
    },
  ],
};

interface MockStrategyContext {
  strategy_name: string;
  category: string;
  raw_config: Record<string, unknown>;
  default_shared_config: Record<string, unknown>;
  default_execution: Record<string, unknown>;
  effective_shared_config: Record<string, unknown>;
  effective_execution: Record<string, unknown>;
  shared_config_provenance: unknown[];
  execution_provenance: unknown[];
  unknown_top_level_keys: string[];
}

interface MockStrategyEditorReference {
  basics: AuthoringFieldSchema[];
  shared_config_fields: AuthoringFieldSchema[];
  execution_fields: AuthoringFieldSchema[];
  shared_config_groups: Array<{ key: string; label: string; description: string | null }>;
  execution_groups: Array<{ key: string; label: string; description: string | null }>;
  signal_categories: Array<{ key: string; label: string }>;
  capabilities: {
    visual_editor: boolean;
    yaml_fallback: boolean;
    preview: boolean;
    preserves_unknown_fields: boolean;
    structured_default_edit: boolean;
  };
}

interface MockSignalReference {
  categories: Array<{ key: string; label: string }>;
  total: number;
  signals: SignalDefinition[];
}

interface MockState {
  strategyContext: MockStrategyContext;
  reference: MockStrategyEditorReference;
  signalReference: MockSignalReference;
  datasets: Array<{ name: string }>;
  indices: { indices: Array<{ code: string; name: string }> };
  datasetInfo: { name: string; storage: { backend: string } } | null;
  contextLoading: boolean;
  referenceLoading: boolean;
  signalLoading: boolean;
  validatePending: boolean;
  updatePending: boolean;
  updateError: Error | null;
}

const mockState: MockState = {
  strategyContext: {
    strategy_name: 'experimental/sample',
    category: 'experimental',
    raw_config: {
      shared_config: {
        dataset: 'custom-dataset',
        execution_policy: { mode: 'standard' },
      },
      execution: {
        template_notebook: 'custom.py',
      },
      entry_filter_params: {
        volume_ratio_above: {
          enabled: true,
          ratio_threshold: 1.5,
        },
      },
      exit_trigger_params: {
        rsi_threshold: {
          enabled: true,
          threshold: 70,
        },
      },
    },
    default_shared_config: {
      dataset: 'default-dataset',
      benchmark_table: 'topix',
      stock_codes: ['all'],
      execution_policy: { mode: 'standard' },
    },
    default_execution: {
      template_notebook: 'default.py',
    },
    effective_shared_config: {
      dataset: 'custom-dataset',
      benchmark_table: 'topix',
      stock_codes: ['all'],
      execution_policy: { mode: 'standard' },
    },
    effective_execution: {
      template_notebook: 'custom.py',
    },
    shared_config_provenance: [],
    execution_provenance: [],
    unknown_top_level_keys: [],
  },
  reference: {
    basics: [
      {
        path: 'display_name',
        section: 'strategy',
        group: 'basics',
        label: 'Display Name',
        type: 'string',
        widget: 'text',
        description: 'Display name',
        summary: 'Human label',
        default: null,
        options: null,
        constraints: undefined,
        placeholder: 'Forward EPS Driven',
        unit: null,
        examples: ['Forward EPS Driven'],
        required: false,
        advanced_only: false,
      },
      {
        path: 'description',
        section: 'strategy',
        group: 'basics',
        label: 'Description',
        type: 'string',
        widget: 'textarea',
        description: 'Description',
        summary: 'Strategy summary',
        default: null,
        options: null,
        constraints: undefined,
        placeholder: 'Describe strategy',
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
    ],
    shared_config_fields: [
      {
        path: 'dataset',
        section: 'shared_config',
        group: 'data',
        label: 'Dataset',
        type: 'string',
        widget: 'combobox',
        description: 'Dataset name',
        summary: 'Dataset snapshot',
        default: 'default-dataset',
        options: null,
        constraints: undefined,
        placeholder: 'prime_20260316',
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
      {
        path: 'benchmark_table',
        section: 'shared_config',
        group: 'execution',
        label: 'Benchmark',
        type: 'string',
        widget: 'combobox',
        description: 'Benchmark',
        summary: 'Benchmark table',
        default: 'topix',
        options: null,
        constraints: undefined,
        placeholder: 'topix',
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
      {
        path: 'execution_policy.mode',
        section: 'shared_config',
        group: 'execution',
        label: 'Execution Policy',
        type: 'select',
        widget: 'select',
        description: 'Execution policy',
        summary: 'Execution policy mode',
        default: 'standard',
        options: ['standard', 'next_session_round_trip'],
        constraints: undefined,
        placeholder: null,
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
      {
        path: 'stock_codes',
        section: 'shared_config',
        group: 'data',
        label: 'Stock Codes',
        type: 'string_list',
        widget: 'string_list',
        description: 'Stock codes',
        summary: 'Use all or provide a custom list',
        default: ['all'],
        options: null,
        constraints: undefined,
        placeholder: null,
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
    ],
    execution_fields: [],
    shared_config_groups: [
      { key: 'data', label: 'Data', description: 'Data settings' },
      { key: 'execution', label: 'Execution', description: 'Execution settings' },
    ],
    execution_groups: [],
    signal_categories: [
      { key: 'volume', label: 'Volume' },
      { key: 'oscillator', label: 'Oscillator' },
    ],
    capabilities: {
      visual_editor: true,
      yaml_fallback: true,
      preview: true,
      preserves_unknown_fields: true,
      structured_default_edit: true,
    },
  },
  signalReference: {
    categories: [
      { key: 'volume', label: 'Volume' },
      { key: 'oscillator', label: 'Oscillator' },
    ],
    total: 2,
    signals: [
      {
        key: 'volume_ratio_above',
        signal_type: 'volume_ratio_above',
        name: 'Volume Ratio Above',
        category: 'volume',
        description: 'Volume filter',
        summary: 'Volume filter',
        when_to_use: ['Use when volume expansion matters.'],
        pitfalls: ['Needs volume coverage.'],
        examples: ['volume_ratio_above'],
        usage_hint: 'Entry hint',
        yaml_snippet: 'entry_filter_params:\n  volume_ratio_above:\n    enabled: true',
        exit_disabled: false,
        data_requirements: ['market.volume'],
        availability_profiles: [],
        chart: {
          supported: true,
          supported_modes: ['entry', 'exit'],
          supports_relative_mode: true,
          requires_benchmark: false,
          requires_sector_data: false,
          requires_margin_data: false,
          requires_statements_data: false,
        },
        fields: [
          {
            name: 'enabled',
            label: 'Enabled',
            type: 'boolean',
            description: 'Enabled',
            default: true,
          },
          {
            name: 'ratio_threshold',
            label: 'Ratio Threshold',
            type: 'number',
            description: 'Threshold',
            default: 1.5,
            placeholder: '1.5',
          },
        ],
      },
      {
        key: 'rsi_threshold',
        signal_type: 'rsi_threshold',
        name: 'RSI Threshold',
        category: 'oscillator',
        description: 'RSI exit filter',
        summary: 'RSI exit filter',
        when_to_use: ['Use for overbought exits.'],
        pitfalls: ['Needs price data.'],
        examples: ['rsi_threshold'],
        usage_hint: 'Exit hint',
        yaml_snippet: 'exit_trigger_params:\n  rsi_threshold:\n    enabled: true',
        exit_disabled: false,
        data_requirements: ['market.close'],
        availability_profiles: [],
        chart: {
          supported: true,
          supported_modes: ['entry', 'exit'],
          supports_relative_mode: true,
          requires_benchmark: false,
          requires_sector_data: false,
          requires_margin_data: false,
          requires_statements_data: false,
        },
        fields: [
          {
            name: 'enabled',
            label: 'Enabled',
            type: 'boolean',
            description: 'Enabled',
            default: true,
          },
          {
            name: 'threshold',
            label: 'Threshold',
            type: 'number',
            description: 'Threshold',
            default: 70,
          },
        ],
      },
    ],
  },
  datasets: [{ name: 'custom-dataset' }, { name: 'default-dataset' }],
  indices: {
    indices: [
      { code: 'topix', name: 'TOPIX' },
      { code: 'N225_UNDERPX', name: 'Nikkei 225 UnderPx' },
    ],
  },
  datasetInfo: null as { name: string; storage: { backend: string } } | null,
  contextLoading: false,
  referenceLoading: false,
  signalLoading: false,
  validatePending: false,
  updatePending: false,
  updateError: null as Error | null,
};

const defaultSignalReference = structuredClone(mockState.signalReference);

function createDeferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
}

vi.mock('@/hooks/useBacktest', () => ({
  useStrategyEditorContext: () => ({
    data: mockState.strategyContext,
    isLoading: mockState.contextLoading,
  }),
  useStrategyEditorReference: () => ({
    data: mockState.reference,
    isLoading: mockState.referenceLoading,
  }),
  useSignalReference: () => ({
    data: mockState.signalReference,
    isLoading: mockState.signalLoading,
  }),
  useValidateStrategy: () => ({
    mutateAsync: mockValidateMutateAsync,
    isPending: mockState.validatePending,
    reset: mockValidateReset,
  }),
  useUpdateStrategy: () => ({
    mutate: mockUpdateMutate,
    isPending: mockState.updatePending,
    isError: !!mockState.updateError,
    error: mockState.updateError,
    reset: mockUpdateReset,
  }),
}));

vi.mock('@/hooks/useDataset', () => ({
  useDatasets: () => ({
    data: mockState.datasets,
  }),
  useDatasetInfo: () => ({
    data: mockState.datasetInfo,
  }),
}));

vi.mock('@/hooks/useIndices', () => ({
  useIndicesList: () => ({
    data: mockState.indices,
  }),
}));

vi.mock('@/components/Editor/MonacoYamlEditor', () => ({
  MonacoYamlEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea aria-label="YAML Editor" value={value} onChange={(event) => onChange(event.target.value)} />
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

vi.mock('@/components/ui/select', () => ({
  Select: ({
    children,
    value,
    onValueChange,
    disabled,
  }: {
    children: ReactNode;
    value: string;
    onValueChange: (value: string) => void;
    disabled?: boolean;
  }) => (
    <select
      aria-label="mock-select"
      value={value}
      onChange={(event) => onValueChange(event.target.value)}
      disabled={disabled}
    >
      {children}
    </select>
  ),
  SelectTrigger: ({ children, 'aria-label': ariaLabel }: { children: ReactNode; 'aria-label'?: string }) => (
    <option value="">{ariaLabel ?? children}</option>
  ),
  SelectValue: ({ placeholder }: { placeholder?: string }) => <option value="">{placeholder ?? 'Select value'}</option>,
  SelectContent: ({ children }: { children: ReactNode }) => <>{children}</>,
  SelectItem: ({ children, value }: { children: ReactNode; value: string }) => (
    <option value={value}>{children}</option>
  ),
}));

vi.mock('@/components/ui/switch', () => ({
  Switch: ({
    id,
    checked,
    disabled,
    onCheckedChange,
  }: {
    id?: string;
    checked: boolean;
    disabled?: boolean;
    onCheckedChange: (value: boolean) => void;
  }) => (
    <input
      id={id}
      type="checkbox"
      role="switch"
      aria-checked={checked}
      checked={checked}
      disabled={disabled}
      onChange={(event) => onCheckedChange(event.target.checked)}
    />
  ),
}));

vi.mock('./SignalReferencePanel', () => ({
  SignalReferencePanel: ({ onCopySnippet }: { onCopySnippet: (snippet: string) => void }) => (
    <div>
      <div>Signal Reference</div>
      <button type="button" onClick={() => onCopySnippet('entry_filter_params:\n  snippet: true')}>
        Insert Snippet
      </button>
    </div>
  ),
}));

describe('StrategyEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.contextLoading = false;
    mockState.referenceLoading = false;
    mockState.signalLoading = false;
    mockState.validatePending = false;
    mockState.updatePending = false;
    mockState.updateError = null;
    mockState.datasetInfo = null;
    mockState.signalReference = structuredClone(defaultSignalReference);
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
        },
        execution: {
          template_notebook: 'custom.py',
        },
        entry_filter_params: {
          volume_ratio_above: {
            enabled: true,
            ratio_threshold: 1.5,
          },
        },
        exit_trigger_params: {
          rsi_threshold: {
            enabled: true,
            threshold: 70,
          },
        },
      },
      default_shared_config: {
        dataset: 'default-dataset',
        benchmark_table: 'topix',
        stock_codes: ['all'],
        execution_policy: { mode: 'standard' },
      },
      effective_execution: {
        template_notebook: 'custom.py',
      },
    };
    mockValidateMutateAsync.mockResolvedValue({
      valid: true,
      errors: [],
      warnings: [],
      compiled_strategy: compiledStrategy,
    });
  });

  it('renders visual editor with strategy context and advanced-only execution notice', async () => {
    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(await screen.findByText('Strategy Editor')).toBeInTheDocument();
    expect(screen.getByText('Shared Config')).toBeInTheDocument();
    expect(screen.getByDisplayValue('custom-dataset')).toBeInTheDocument();
    expect(screen.getByText('Advanced-only Content')).toBeInTheDocument();
    expect(screen.getByText('execution')).toBeInTheDocument();
  });

  it('renders section sidebar and updates the active section state', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    const basicsButton = await screen.findByRole('button', { name: 'Basics Display name and strategy summary.' });
    const entryButton = screen.getByRole('button', { name: 'Entry Filters Signals that gate entries.' });

    expect(basicsButton).toHaveAttribute('aria-current', 'page');

    await user.click(entryButton);

    expect(entryButton).toHaveAttribute('aria-current', 'page');
    expect(basicsButton).not.toHaveAttribute('aria-current');
  });

  it('renders loading state while editor queries are pending', () => {
    mockState.contextLoading = true;

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('omits the advanced-only notice when the config is fully visualizable', async () => {
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
        },
        entry_filter_params: {},
        exit_trigger_params: {},
      },
    };

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(await screen.findByText('Shared Config')).toBeInTheDocument();
    expect(screen.queryByText('Advanced-only Content')).not.toBeInTheDocument();
  });

  it('switches to preview and renders validation warnings', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockResolvedValueOnce({
      valid: true,
      errors: [],
      warnings: ['signal reference stale'],
      compiled_strategy: compiledStrategy,
    });

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Validate' }));

    expect(await screen.findByText('Validation passed with warnings')).toBeInTheDocument();
    expect(screen.getByText('signal reference stale')).toBeInTheDocument();
    expect(screen.getByText('Availability Timing')).toBeInTheDocument();
  });

  it('refreshes preview from the tab and shows compiled fallback values', async () => {
    const user = userEvent.setup();
    const previewResult = {
      valid: true,
      errors: [],
      warnings: [],
      compiled_strategy: {
        ...compiledStrategy,
        execution_semantics: 'intraday_custom',
        dataset_name: undefined,
        signal_ids: [],
        required_data_domains: [],
        required_fundamental_fields: [],
        signals: [
          {
            ...compiledStrategy.signals[0],
            data_requirements: [],
            availability: {
              observation_time: 'intraday_custom',
              available_at: 'intraday_custom',
              decision_cutoff: 'intraday_custom',
              execution_session: 'intraday_custom',
            },
          },
        ],
      },
    };
    mockValidateMutateAsync.mockResolvedValueOnce(previewResult).mockResolvedValueOnce(previewResult);

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Preview' }));

    expect(await screen.findByText('Validation passed')).toBeInTheDocument();
    expect(screen.getByText('intraday_custom')).toBeInTheDocument();
    expect(screen.getByText('inherited/default')).toBeInTheDocument();
    expect(screen.getByText('No fundamental fields required.')).toBeInTheDocument();
    expect(screen.getByText(/^market$/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Re-run Preview' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Re-run Preview' }));

    await waitFor(() => {
      expect(mockValidateMutateAsync).toHaveBeenCalledTimes(2);
    });
  });

  it('blocks save when backend validation reports errors', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockResolvedValueOnce({
      valid: false,
      errors: ['entry_filter_params.fundamental.forward_eps_growth is invalid'],
      warnings: [],
      compiled_strategy: null,
    });

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).not.toHaveBeenCalled();
  });

  it('saves strategy after successful validation and closes dialog', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockValidateMutateAsync).toHaveBeenCalledWith({
      name: 'experimental/sample',
      request: { config: expect.any(Object) },
    });
    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: { config: expect.any(Object) },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('surfaces validation request failures in preview', async () => {
    const user = userEvent.setup();
    mockValidateMutateAsync.mockRejectedValueOnce(new Error('network down'));

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Validate' }));

    expect(await screen.findByText('Validation failed')).toBeInTheDocument();
    expect(screen.getByText(/Validation request failed: network down/)).toBeInTheDocument();
  });

  it('shows stale preview state and update errors before rerunning validation', async () => {
    const user = userEvent.setup();
    const deferred = createDeferred<{
      valid: boolean;
      errors: string[];
      warnings: string[];
      compiled_strategy: typeof compiledStrategy;
    }>();
    mockState.updateError = new Error('save failed');
    mockValidateMutateAsync.mockResolvedValueOnce({
      valid: true,
      errors: [],
      warnings: [],
      compiled_strategy: compiledStrategy,
    });
    mockValidateMutateAsync.mockImplementationOnce(() => deferred.promise);

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Validate' }));
    expect(await screen.findByText('Validation passed')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Visual' }));
    fireEvent.change(screen.getByLabelText('Description'), {
      target: { value: 'Changed description' },
    });
    await user.click(screen.getByRole('tab', { name: 'Preview' }));

    expect(await screen.findByText('(stale)')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh Preview' })).toBeInTheDocument();
    expect(screen.getByText('Error: save failed')).toBeInTheDocument();

    deferred.resolve({
      valid: true,
      errors: [],
      warnings: [],
      compiled_strategy: compiledStrategy,
    });

    await waitFor(() => {
      expect(mockValidateMutateAsync).toHaveBeenCalledTimes(2);
    });
  });

  it('shows parse error and skips validation for invalid advanced yaml', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: { value: '{invalid' },
    });
    await user.click(screen.getByRole('button', { name: 'Validate' }));

    expect(mockValidateMutateAsync).not.toHaveBeenCalled();
    expect(await screen.findByText(/YAML parse error:/)).toBeInTheDocument();
  });

  it('returns to visual mode when advanced yaml is compatible', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: {
        value: `display_name: Hybrid\nshared_config:\n  dataset: switched-dataset\nentry_filter_params: {}\nexit_trigger_params: {}`,
      },
    });
    await user.click(screen.getByRole('tab', { name: 'Visual' }));

    expect(await screen.findByDisplayValue('switched-dataset')).toBeInTheDocument();
  });

  it('shows parse errors inside preview when advanced yaml cannot be parsed', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: { value: '{invalid' },
    });
    await user.click(screen.getByRole('tab', { name: 'Preview' }));

    expect(await screen.findByText(/YAML parse error:/)).toBeInTheDocument();
    expect(mockValidateMutateAsync).not.toHaveBeenCalled();
  });

  it('blocks returning to visual mode for incompatible yaml', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: { value: 'shared_config: []' },
    });
    await user.click(screen.getByRole('tab', { name: 'Visual' }));

    expect(await screen.findByText('shared_config must be an object to edit it in Visual mode.')).toBeInTheDocument();
  });

  it('shows dataset info and saves custom stock codes', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        ...mockState.strategyContext.raw_config,
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
          stock_codes: ['7203'],
        },
      },
    };
    mockState.datasetInfo = { name: 'custom-dataset', storage: { backend: 'duckdb-parquet' } };
    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    expect(await screen.findByText(/loaded from duckdb-parquet/i)).toBeInTheDocument();
    const stockCodesInput = Array.from(document.querySelectorAll('textarea')).find(
      (element) => element.value === '7203'
    ) as HTMLTextAreaElement | undefined;
    expect(stockCodesInput).toBeTruthy();
    fireEvent.change(stockCodesInput as HTMLTextAreaElement, { target: { value: '7203\n6758' } });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: expect.objectContaining({
          config: expect.objectContaining({
            shared_config: expect.objectContaining({
              stock_codes: ['7203', '6758'],
            }),
          }),
        }),
      },
      expect.any(Object)
    );
  });

  it('selects dataset and benchmark values from the available option lists', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    const datasetSelect = (await screen.findByLabelText('Dataset')) as HTMLSelectElement;
    expect(datasetSelect.tagName).toBe('SELECT');
    expect(screen.getByRole('option', { name: 'custom-dataset' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'default-dataset' })).toBeInTheDocument();

    fireEvent.change(datasetSelect, { target: { value: 'default-dataset' } });
    const benchmarkSelect = screen.getByLabelText('Benchmark') as HTMLSelectElement;
    expect(benchmarkSelect.tagName).toBe('SELECT');
    expect(screen.getByRole('option', { name: 'topix' })).toBeInTheDocument();
    fireEvent.change(benchmarkSelect, { target: { value: 'N225_UNDERPX' } });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: expect.objectContaining({
          config: expect.objectContaining({
            shared_config: expect.objectContaining({
              dataset: 'default-dataset',
              benchmark_table: 'N225_UNDERPX',
            }),
          }),
        }),
      },
      expect.any(Object)
    );
  });

  it('switches stock code modes and preserves reset behavior', async () => {
    const user = userEvent.setup();
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        ...mockState.strategyContext.raw_config,
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
          stock_codes: ['7203'],
        },
      },
    };

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    expect(await screen.findByDisplayValue('7203')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Custom' }));
    expect(screen.getByDisplayValue('7203')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'All' }));
    expect(screen.getByText('Entire dataset universe is selected.')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Custom' }));
    const stockCodesTextarea = Array.from(document.querySelectorAll('textarea')).find((element) =>
      (element as HTMLTextAreaElement).placeholder.includes('7203')
    ) as HTMLTextAreaElement | undefined;
    expect(stockCodesTextarea).toBeTruthy();
    expect(stockCodesTextarea).toHaveValue('');
  });

  it('allows resetting basic fields from visual mode', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    const descriptionInput = await screen.findByLabelText('Description');
    fireEvent.change(descriptionInput, { target: { value: 'Needs cleanup' } });
    expect(screen.getByDisplayValue('Needs cleanup')).toBeInTheDocument();

    await user.click(screen.getAllByRole('button', { name: 'Reset' })[1] as HTMLElement);
    expect(screen.getByLabelText('Description')).toHaveValue('');
  });

  it('adds and removes regular signals from the visual editor', async () => {
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
        },
        entry_filter_params: {},
        exit_trigger_params: {},
      },
    };
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    fireEvent.change(await screen.findByLabelText('Add Entry Signals'), {
      target: { value: 'volume_ratio_above' },
    });
    expect(screen.getByRole('heading', { name: 'Volume Ratio Above' })).toBeInTheDocument();

    await user.click(screen.getAllByRole('button', { name: 'Remove' })[0] as HTMLElement);
    expect(screen.queryByRole('heading', { name: 'Volume Ratio Above' })).not.toBeInTheDocument();
  });

  it('updates regular signal fields before saving', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    await user.click(await screen.findByLabelText('Volume Ratio Above enabled'));
    fireEvent.change(screen.getByLabelText('Ratio Threshold'), {
      target: { value: '2.3' },
    });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: expect.objectContaining({
          config: expect.objectContaining({
            entry_filter_params: expect.objectContaining({
              volume_ratio_above: expect.objectContaining({
                enabled: false,
                ratio_threshold: 2.3,
              }),
            }),
          }),
        }),
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('adds a fundamental signal and shows shared parent controls', async () => {
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
        },
        entry_filter_params: {},
        exit_trigger_params: {},
      },
    };
    mockState.signalReference = {
      ...mockState.signalReference,
      categories: [...mockState.signalReference.categories, { key: 'fundamental', label: 'Fundamental' }],
      signals: [
        ...mockState.signalReference.signals,
        {
          key: 'fundamental_forward_eps_growth',
          signal_type: 'forward_eps_growth',
          name: 'Forward EPS Growth',
          category: 'fundamental',
          description: 'Forecast growth filter',
          summary: 'Forecast growth filter',
          when_to_use: ['Use when forecasts improve.'],
          pitfalls: ['Needs statements data.'],
          examples: ['forward_eps_growth'],
          usage_hint: 'Entry hint',
          yaml_snippet: `entry_filter_params:
  fundamental:
    enabled: true
    period_type: FY
    use_adjusted: true
    forward_eps_growth:
      enabled: true
      threshold: 0.2`,
          exit_disabled: false,
          data_requirements: ['statements'],
          availability_profiles: [],
          chart: {
            supported: false,
            supported_modes: [],
            supports_relative_mode: false,
            requires_benchmark: false,
            requires_sector_data: false,
            requires_margin_data: false,
            requires_statements_data: true,
          },
          fields: [
            { name: 'enabled', label: 'Enabled', type: 'boolean', description: 'Enabled', default: true },
            {
              name: 'period_type',
              label: 'Period Type',
              type: 'select',
              description: 'Period type',
              default: 'FY',
              options: ['FY', 'Q'],
            },
            {
              name: 'use_adjusted',
              label: 'Use Adjusted Values',
              type: 'boolean',
              description: 'Use adjusted values',
              default: true,
            },
            {
              name: 'threshold',
              label: 'Threshold',
              type: 'number',
              description: 'Threshold',
              default: 0.2,
            },
          ],
        },
      ],
    };

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    fireEvent.change(await screen.findByLabelText('Add Entry Signals fundamental signal'), {
      target: { value: 'forward_eps_growth' },
    });

    expect(screen.getByRole('heading', { name: 'Forward EPS Growth' })).toBeInTheDocument();
    expect(screen.getAllByText('Period Type').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Use Adjusted Values').length).toBeGreaterThan(0);
  });

  it('updates and removes a fundamental child signal from the visual editor', async () => {
    const user = userEvent.setup();
    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'standard' },
        },
        entry_filter_params: {},
        exit_trigger_params: {},
      },
    };
    mockState.signalReference = {
      ...mockState.signalReference,
      categories: [...mockState.signalReference.categories, { key: 'fundamental', label: 'Fundamental' }],
      signals: [
        ...mockState.signalReference.signals,
        {
          key: 'fundamental_forward_eps_growth',
          signal_type: 'forward_eps_growth',
          name: 'Forward EPS Growth',
          category: 'fundamental',
          description: 'Forecast growth filter',
          summary: 'Forecast growth filter',
          when_to_use: [],
          pitfalls: [],
          examples: ['forward_eps_growth'],
          usage_hint: 'Entry hint',
          yaml_snippet: 'entry_filter_params:\n  fundamental:\n    forward_eps_growth:\n      enabled: true',
          exit_disabled: false,
          data_requirements: ['statements'],
          availability_profiles: [],
          chart: {
            supported: false,
            supported_modes: [],
            supports_relative_mode: false,
            requires_benchmark: false,
            requires_sector_data: false,
            requires_margin_data: false,
            requires_statements_data: true,
          },
          fields: [
            { name: 'enabled', label: 'Enabled', type: 'boolean', description: 'Enabled', default: true },
            {
              name: 'period_type',
              label: 'Period Type',
              type: 'select',
              description: 'Period type',
              default: 'FY',
              options: ['FY', 'Q'],
            },
            {
              name: 'use_adjusted',
              label: 'Use Adjusted Values',
              type: 'boolean',
              description: 'Use adjusted values',
              default: true,
            },
            {
              name: 'threshold',
              label: 'Threshold',
              type: 'number',
              description: 'Threshold',
              default: 0.2,
            },
          ],
        },
      ],
    };

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    fireEvent.change(await screen.findByLabelText('Add Entry Signals fundamental signal'), {
      target: { value: 'forward_eps_growth' },
    });

    const periodTypeSelect = Array.from(document.querySelectorAll('select[aria-label="mock-select"]')).find((element) =>
      element.textContent?.includes('Q')
    ) as HTMLSelectElement | undefined;
    expect(periodTypeSelect).toBeTruthy();

    fireEvent.change(periodTypeSelect as HTMLSelectElement, {
      target: { value: 'Q' },
    });
    await user.click(screen.getAllByRole('switch', { name: 'Use Adjusted Values' })[0] as HTMLElement);
    fireEvent.change(screen.getAllByLabelText('Threshold').at(-1) as HTMLElement, {
      target: { value: '0.4' },
    });
    await user.click(screen.getByRole('button', { name: 'Remove' }));

    expect(screen.queryByRole('heading', { name: 'Forward EPS Growth' })).not.toBeInTheDocument();
    expect(screen.getAllByText('No fundamental filters configured in this section.').length).toBeGreaterThan(0);
  });

  it('disables exit section for round-trip execution mode and saves cleared exits', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockState.strategyContext = {
      ...mockState.strategyContext,
      raw_config: {
        ...mockState.strategyContext.raw_config,
        shared_config: {
          dataset: 'custom-dataset',
          execution_policy: { mode: 'next_session_round_trip' },
        },
        exit_trigger_params: {
          rsi_threshold: {
            enabled: true,
            threshold: 70,
          },
        },
      },
      default_shared_config: {
        ...mockState.strategyContext.default_shared_config,
        execution_policy: { mode: 'standard' },
      },
    };

    mockUpdateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    expect(await screen.findByText(/disables exit triggers/i)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Clear Exit Config' }));
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockUpdateMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/sample',
        request: expect.objectContaining({
          config: expect.objectContaining({
            exit_trigger_params: {},
          }),
        }),
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('appends copied snippet in advanced yaml mode', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    await user.click(screen.getByRole('button', { name: 'Insert Snippet' }));

    await waitFor(() => {
      expect((screen.getByLabelText('YAML Editor') as HTMLTextAreaElement).value).toContain('snippet: true');
    });
  });

  it('appends copied snippet into an empty advanced yaml document', async () => {
    const user = userEvent.setup();

    render(<StrategyEditor open onOpenChange={vi.fn()} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('YAML Editor'), {
      target: { value: '' },
    });
    await user.click(screen.getByRole('button', { name: 'Insert Snippet' }));

    expect(screen.getByLabelText('YAML Editor')).toHaveValue('entry_filter_params:\n  snippet: true');
  });

  it('resets validation state when cancel is clicked', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<StrategyEditor open onOpenChange={onOpenChange} strategyName="experimental/sample" />);

    await user.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(mockUpdateReset).toHaveBeenCalled();
    expect(mockValidateReset).toHaveBeenCalled();
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });
});
