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
      name: 'beta.db',
      preset: null,
      fileSize: 3000,
      lastModified: '2026-01-01T00:00:00.000Z',
      createdAt: '2026-01-01T00:00:00.000Z',
    },
    {
      name: 'alpha.db',
      preset: 'primeMarket',
      fileSize: 1000,
      lastModified: '2026-01-02T00:00:00.000Z',
      createdAt: '2026-01-02T00:00:00.000Z',
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
  return rows[0]!;
}

function getFirstByTitle(title: string): HTMLElement {
  const elements = screen.getAllByTitle(title);
  expect(elements.length).toBeGreaterThan(0);
  return elements[0]!;
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

    expect(within(getFirstDataRow()).getByText('alpha.db')).toBeInTheDocument();

    await user.click(screen.getByText('Name'));
    expect(within(getFirstDataRow()).getByText('alpha.db')).toBeInTheDocument();

    await user.click(screen.getByText('Name'));
    expect(within(getFirstDataRow()).getByText('beta.db')).toBeInTheDocument();

    await user.click(screen.getByText('Preset'));
    expect(within(getFirstDataRow()).getByText('beta.db')).toBeInTheDocument();

    await user.click(screen.getByText('Size'));
    expect(within(getFirstDataRow()).getByText('beta.db')).toBeInTheDocument();

    await user.click(screen.getByText('Modified'));
    expect(within(getFirstDataRow()).getByText('alpha.db')).toBeInTheDocument();
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
        name: 'alpha.db',
        preset: 'primeMarket',
        timeoutMinutes: 30,
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
    await user.click(getFirstByTitle('詳細'));
    expect(screen.getByText('info:alpha.db')).toBeInTheDocument();

    await user.click(getFirstByTitle('削除'));
    expect(screen.getByText('delete:alpha.db')).toBeInTheDocument();
  });
});
