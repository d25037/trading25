import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DefaultConfigEditor } from './DefaultConfigEditor';

const mockRawMutate = vi.fn();
const mockRawReset = vi.fn();
const mockStructuredMutate = vi.fn();
const mockStructuredReset = vi.fn();

const mockState = {
  context: {
    raw_yaml: `default:
  execution:
    template_notebook: notebooks/templates/strategy_analysis.py
  parameters:
    shared_config:
      dataset: prime_20260316
      benchmark_table: topix
`,
    raw_document: {
      default: {
        extra_note: 'keep me',
        execution: {
          template_notebook: 'notebooks/templates/strategy_analysis.py',
        },
        parameters: {
          shared_config: {
            dataset: 'prime_20260316',
            benchmark_table: 'topix',
          },
        },
      },
    },
    raw_execution: {
      template_notebook: 'notebooks/templates/strategy_analysis.py',
    },
    raw_shared_config: {
      dataset: 'prime_20260316',
      benchmark_table: 'topix',
    },
    effective_execution: {
      template_notebook: 'notebooks/templates/strategy_analysis.py',
    },
    effective_shared_config: {
      dataset: 'prime_20260316',
      benchmark_table: 'topix',
    },
    advanced_only_paths: ['default.extra_note'],
  },
  reference: {
    basics: [],
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
        constraints: null,
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
        summary: 'Benchmark code',
        default: 'topix',
        options: null,
        constraints: null,
        placeholder: 'topix',
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
    ],
    execution_fields: [
      {
        path: 'template_notebook',
        section: 'execution',
        group: 'execution',
        label: 'Template Notebook',
        type: 'string',
        widget: 'text',
        description: 'Template path',
        summary: 'Notebook template',
        default: 'notebooks/templates/strategy_analysis.py',
        options: null,
        constraints: null,
        placeholder: null,
        unit: null,
        examples: [],
        required: false,
        advanced_only: false,
      },
    ],
    shared_config_groups: [
      { key: 'data', label: 'Data', description: 'Data settings' },
      { key: 'execution', label: 'Execution', description: 'Execution settings' },
    ],
    execution_groups: [{ key: 'execution', label: 'Execution', description: 'Execution defaults' }],
    signal_categories: [],
    capabilities: {
      visual_editor: true,
      yaml_fallback: true,
      preview: true,
      preserves_unknown_fields: true,
      structured_default_edit: true,
    },
  },
  datasets: [{ name: 'prime_20260316' }, { name: 'default-dataset' }],
  indices: {
    indices: [
      { code: 'topix', name: 'TOPIX' },
      { code: 'N225_UNDERPX', name: 'Nikkei 225 UnderPx' },
    ],
  },
  contextLoading: false,
  referenceLoading: false,
  rawPending: false,
  structuredPending: false,
  rawError: null as Error | null,
  structuredError: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useDefaultConfigEditorContext: () => ({
    data: mockState.context,
    isLoading: mockState.contextLoading,
  }),
  useStrategyEditorReference: () => ({
    data: mockState.reference,
    isLoading: mockState.referenceLoading,
  }),
  useUpdateDefaultConfig: () => ({
    mutate: mockRawMutate,
    reset: mockRawReset,
    isPending: mockState.rawPending,
    isError: !!mockState.rawError,
    error: mockState.rawError,
  }),
  useUpdateDefaultConfigStructured: () => ({
    mutate: mockStructuredMutate,
    reset: mockStructuredReset,
    isPending: mockState.structuredPending,
    isError: !!mockState.structuredError,
    error: mockState.structuredError,
  }),
}));

vi.mock('@/hooks/useDataset', () => ({
  useDatasets: () => ({
    data: mockState.datasets,
  }),
}));

vi.mock('@/hooks/useIndices', () => ({
  useIndicesList: () => ({
    data: mockState.indices,
  }),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock('@/components/ui/button', () => ({
  Button: ({ children, onClick, disabled }: { children: ReactNode; onClick?: () => void; disabled?: boolean }) => (
    <button type="button" onClick={onClick} disabled={disabled}>
      {children}
    </button>
  ),
}));

vi.mock('@/components/Editor/MonacoYamlEditor', () => ({
  MonacoYamlEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea
      aria-label="yaml-editor"
      value={value}
      onChange={(event: ChangeEvent<HTMLTextAreaElement>) => onChange(event.target.value)}
    />
  ),
}));

describe('DefaultConfigEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.contextLoading = false;
    mockState.referenceLoading = false;
    mockState.rawPending = false;
    mockState.structuredPending = false;
    mockState.rawError = null;
    mockState.structuredError = null;
  });

  it('renders loading state', () => {
    mockState.contextLoading = true;

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('saves structured visual draft and closes dialog on success', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockStructuredMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<DefaultConfigEditor open={true} onOpenChange={onOpenChange} />);

    expect(screen.getByText('Execution Defaults')).toBeInTheDocument();
    expect(screen.getByText('Advanced-only Content')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockStructuredMutate).toHaveBeenCalledWith(
      {
        execution: {
          template_notebook: 'notebooks/templates/strategy_analysis.py',
        },
        shared_config: {
          dataset: 'prime_20260316',
          benchmark_table: 'topix',
        },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('uses available dataset and benchmark options for default shared config selection', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockStructuredMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<DefaultConfigEditor open={true} onOpenChange={onOpenChange} />);

    const datasetSelect = (await screen.findByLabelText('Dataset')) as HTMLSelectElement;
    expect(datasetSelect.tagName).toBe('SELECT');
    expect(screen.getByRole('option', { name: 'prime_20260316' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'default-dataset' })).toBeInTheDocument();

    fireEvent.change(datasetSelect, { target: { value: 'default-dataset' } });
    const benchmarkSelect = screen.getByLabelText('Benchmark') as HTMLSelectElement;
    expect(benchmarkSelect.tagName).toBe('SELECT');
    expect(screen.getByRole('option', { name: 'topix' })).toBeInTheDocument();
    fireEvent.change(benchmarkSelect, { target: { value: 'N225_UNDERPX' } });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockStructuredMutate).toHaveBeenCalledWith(
      {
        execution: {
          template_notebook: 'notebooks/templates/strategy_analysis.py',
        },
        shared_config: {
          dataset: 'default-dataset',
          benchmark_table: 'N225_UNDERPX',
        },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('blocks advanced save for invalid yaml', async () => {
    const user = userEvent.setup();

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('yaml-editor'), { target: { value: 'default: [' } });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockRawMutate).not.toHaveBeenCalled();
    expect(screen.getByText(/YAML parse error:/)).toBeInTheDocument();
  });

  it('returns to visual mode when advanced yaml remains compatible', async () => {
    const user = userEvent.setup();

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('yaml-editor'), {
      target: {
        value: `default:
  execution:
    template_notebook: custom_template.py
  parameters:
    shared_config:
      dataset: switched_dataset`,
      },
    });
    await user.click(screen.getByRole('tab', { name: 'Visual' }));

    expect(await screen.findByDisplayValue('switched_dataset')).toBeInTheDocument();
  });

  it('blocks returning to visual mode for incompatible yaml', async () => {
    const user = userEvent.setup();

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    fireEvent.change(screen.getByLabelText('yaml-editor'), {
      target: { value: 'default: []' },
    });
    await user.click(screen.getByRole('tab', { name: 'Visual' }));

    expect(screen.getByText("default.yaml must contain a 'default' object to use Visual mode.")).toBeInTheDocument();
  });

  it('uses raw yaml save in advanced mode', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockRawMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<DefaultConfigEditor open={true} onOpenChange={onOpenChange} />);

    await user.click(screen.getByRole('tab', { name: 'Advanced YAML' }));
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockRawMutate).toHaveBeenCalledWith(
      {
        content: expect.stringContaining('template_notebook'),
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('shows mutation error banner', () => {
    mockState.structuredError = new Error('update failed');

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    expect(screen.getByText('Error: update failed')).toBeInTheDocument();
  });
});
