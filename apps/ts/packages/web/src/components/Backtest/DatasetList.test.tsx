import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { DatasetListItem } from '@/types/dataset';
import { DatasetList } from './DatasetList';

const mockUseDatasets = vi.fn();

vi.mock('@/hooks/useDataset', () => ({
  useDatasets: () => mockUseDatasets(),
}));

vi.mock('./DatasetInfoDialog', () => ({
  DatasetInfoDialog: ({
    open,
    datasetName,
    onOpenChange,
  }: {
    open: boolean;
    datasetName: string | null;
    onOpenChange: (open: boolean) => void;
  }) =>
    open ? (
      <div>
        <div>info:{datasetName}</div>
        <button type="button" onClick={() => onOpenChange(false)}>
          close-info
        </button>
      </div>
    ) : null,
}));

vi.mock('./DatasetDeleteDialog', () => ({
  DatasetDeleteDialog: ({
    open,
    datasetName,
    onOpenChange,
  }: {
    open: boolean;
    datasetName: string;
    onOpenChange: (open: boolean) => void;
  }) =>
    open ? (
      <div>
        <div>delete:{datasetName}</div>
        <button type="button" onClick={() => onOpenChange(false)}>
          close-delete
        </button>
      </div>
    ) : null,
}));

function createDatasets(): DatasetListItem[] {
  return [
    {
      name: 'beta',
      path: '/tmp/beta',
      preset: null,
      fileSize: 3000,
      lastModified: '2026-01-01T00:00:00.000Z',
      createdAt: '2026-01-01T00:00:00.000Z',
      backend: 'duckdb-parquet',
    },
    {
      name: 'alpha',
      path: '/tmp/alpha',
      preset: 'primeMarket',
      fileSize: 1000,
      lastModified: '2026-01-02T00:00:00.000Z',
      createdAt: '2026-01-02T00:00:00.000Z',
      backend: 'duckdb-parquet',
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
    refetch: vi.fn().mockResolvedValue(undefined),
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
    expect(within(getFirstDataRow()).getByText('DuckDB snapshot')).toBeInTheDocument();

    await user.click(screen.getByText('Preset'));
    expect(within(getFirstDataRow()).getByText('beta')).toBeInTheDocument();

    await user.click(screen.getByText('Size'));
    expect(within(getFirstDataRow()).getByText('beta')).toBeInTheDocument();

    await user.click(screen.getByText('Modified'));
    expect(within(getFirstDataRow()).getByText('alpha')).toBeInTheDocument();
  });

  it('opens info/delete dialogs and renders storage labels', async () => {
    const user = userEvent.setup();
    setDatasetsQueryState({ data: createDatasets() });

    render(<DatasetList />);

    expect(screen.getAllByText('DuckDB snapshot').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('dataset.duckdb + parquet/ + manifest.v2.json').length).toBeGreaterThanOrEqual(1);
    await user.click(getFirstByTitle('詳細'));
    expect(screen.getByText('info:alpha')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'close-info' }));
    expect(screen.queryByText('info:alpha')).not.toBeInTheDocument();

    await user.click(getFirstByTitle('削除'));
    expect(screen.getByText('delete:alpha')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'close-delete' }));
    expect(screen.queryByText('delete:alpha')).not.toBeInTheDocument();
  });

  it('calls refetch when refresh button is clicked', async () => {
    const user = userEvent.setup();
    const refetch = vi.fn().mockResolvedValue(undefined);
    mockUseDatasets.mockReturnValue({
      data: createDatasets(),
      isLoading: false,
      isError: false,
      error: null,
      refetch,
    });

    render(<DatasetList />);

    await user.click(screen.getByRole('button', { name: '' }));
    expect(refetch).toHaveBeenCalledTimes(1);
  });

});
