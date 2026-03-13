import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { DatasetListItem } from '@/types/dataset';
import { DatasetList } from './DatasetList';

const mockUseDatasets = vi.fn();
const mockMutateResume = vi.fn();
const mockSetActiveDatasetJobId = vi.fn();
const mockResumeState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useDataset', () => ({
  useDatasets: () => mockUseDatasets(),
  useResumeDataset: () => ({
    mutate: mockMutateResume,
    isPending: mockResumeState.isPending,
    isError: mockResumeState.isError,
    error: mockResumeState.error,
  }),
}));

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => ({
    setActiveDatasetJobId: mockSetActiveDatasetJobId,
  }),
}));

vi.mock('./DatasetInfoDialog', () => ({
  DatasetInfoDialog: ({
    open,
    datasetName,
  }: {
    open: boolean;
    datasetName: string | null;
    onOpenChange: (open: boolean) => void;
  }) => (open ? <div>info:{datasetName}</div> : null),
}));

vi.mock('./DatasetDeleteDialog', () => ({
  DatasetDeleteDialog: ({
    open,
    datasetName,
  }: {
    open: boolean;
    datasetName: string;
    onOpenChange: (open: boolean) => void;
  }) => (open ? <div>delete:{datasetName}</div> : null),
}));

function createDatasets(): DatasetListItem[] {
  return [
    {
      name: 'beta',
      preset: null,
      fileSize: 3000,
      lastModified: '2026-01-01T00:00:00.000Z',
      createdAt: '2026-01-01T00:00:00.000Z',
      backend: 'sqlite-legacy',
      hasCompatibilityArtifact: false,
    },
    {
      name: 'alpha',
      preset: 'primeMarket',
      fileSize: 1000,
      lastModified: '2026-01-02T00:00:00.000Z',
      createdAt: '2026-01-02T00:00:00.000Z',
      backend: 'duckdb-parquet',
      hasCompatibilityArtifact: true,
    },
  ];
}

function setDatasetsQueryState({
  data = [],
  isLoading = false,
  isError = false,
  error = null,
}: {
  data?: DatasetListItem[] | undefined;
  isLoading?: boolean;
  isError?: boolean;
  error?: Error | null;
}): void {
  mockUseDatasets.mockReturnValue({
    data,
    isLoading,
    isError,
    error,
    refetch: vi.fn(),
  });
}

function getFirstDataRow(): HTMLElement {
  const rows = screen.getAllByRole('row').slice(1);
  expect(rows.length).toBeGreaterThan(0);
  const firstRow = rows[0];
  if (!firstRow) throw new Error('Expected at least one data row');
  return firstRow;
}

function getFirstByTitle(title: string): HTMLElement {
  const elements = screen.getAllByTitle(title);
  expect(elements.length).toBeGreaterThan(0);
  const firstElement = elements[0];
  if (!firstElement) throw new Error(`Expected at least one element with title: ${title}`);
  return firstElement;
}

describe('DatasetList', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResumeState.isPending = false;
    mockResumeState.isError = false;
    mockResumeState.error = null;
    setDatasetsQueryState({});
  });

  it('renders empty state for an empty dataset list', () => {
    render(<DatasetList />);
    expect(screen.getByText('データセットがありません')).toBeInTheDocument();
  });

  it('renders loading and error states', () => {
    setDatasetsQueryState({
      data: undefined,
      isLoading: true,
      isError: true,
      error: new Error('fetch failed'),
    });

    render(<DatasetList />);
    expect(screen.getByText('読み込み中...')).toBeInTheDocument();
    expect(screen.getByText('Error: fetch failed')).toBeInTheDocument();
  });

  it('sorts rows and toggles sorting across columns', async () => {
    const user = userEvent.setup();
    setDatasetsQueryState({ data: createDatasets() });

    render(<DatasetList />);

    expect(within(getFirstDataRow()).getByText('alpha')).toBeInTheDocument();

    await user.click(screen.getByText('Name'));
    expect(within(getFirstDataRow()).getByText('alpha')).toBeInTheDocument();

    await user.click(screen.getByText('Name'));
    expect(within(getFirstDataRow()).getByText('beta')).toBeInTheDocument();

    await user.click(screen.getByText('Storage'));
    expect(within(getFirstDataRow()).getByText('DuckDB + compat')).toBeInTheDocument();

    await user.click(screen.getByText('Preset'));
    expect(within(getFirstDataRow()).getByText('beta')).toBeInTheDocument();

    await user.click(screen.getByText('Size'));
    expect(within(getFirstDataRow()).getByText('beta')).toBeInTheDocument();

    await user.click(screen.getByText('Modified'));
    expect(within(getFirstDataRow()).getByText('alpha')).toBeInTheDocument();
  });

  it('resumes a resumable dataset and stores returned job id', async () => {
    const user = userEvent.setup();
    setDatasetsQueryState({ data: createDatasets() });
    mockMutateResume.mockImplementation((_input, options) => {
      options.onSuccess({ jobId: 'job-123' });
    });

    render(<DatasetList />);

    expect(screen.getAllByTitle('レジューム')).toHaveLength(1);
    await user.click(screen.getByTitle('レジューム'));

    expect(mockMutateResume).toHaveBeenCalledWith(
      {
        name: 'alpha',
        preset: 'primeMarket',
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
    expect(mockSetActiveDatasetJobId).toHaveBeenCalledWith('job-123');
  });

  it('renders resume error and opens info/delete dialogs', async () => {
    const user = userEvent.setup();
    mockResumeState.isError = true;
    mockResumeState.error = new Error('resume failed');
    setDatasetsQueryState({ data: createDatasets() });

    render(<DatasetList />);

    expect(screen.getByText('Resume Error: resume failed')).toBeInTheDocument();
    expect(screen.getByText('Legacy SQLite')).toBeInTheDocument();
    expect(screen.getByText('DuckDB + compat')).toBeInTheDocument();
    await user.click(getFirstByTitle('詳細'));
    expect(screen.getByText('info:alpha')).toBeInTheDocument();

    await user.click(getFirstByTitle('削除'));
    expect(screen.getByText('delete:alpha')).toBeInTheDocument();
  });
});
