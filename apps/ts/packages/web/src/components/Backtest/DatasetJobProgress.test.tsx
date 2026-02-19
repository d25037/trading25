import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DatasetJobResponse } from '@/types/dataset';
import { DatasetJobProgress } from './DatasetJobProgress';

const mockInvalidateQueries = vi.fn();
const mockCancelMutate = vi.fn();
const mockSetActiveDatasetJobId = vi.fn();

const mockStoreState = {
  activeDatasetJobId: null as string | null,
};

const mockHookState = {
  job: null as DatasetJobResponse | null,
  cancelIsPending: false,
};

vi.mock('@tanstack/react-query', async () => {
  const actual = await vi.importActual<typeof import('@tanstack/react-query')>('@tanstack/react-query');
  return {
    ...actual,
    useQueryClient: () => ({
      invalidateQueries: (...args: unknown[]) => mockInvalidateQueries(...args),
    }),
  };
});

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: Object.assign(
    () => ({
      activeDatasetJobId: mockStoreState.activeDatasetJobId,
      setActiveDatasetJobId: (...args: unknown[]) => mockSetActiveDatasetJobId(...args),
    }),
    {
      getState: () => ({
        activeDatasetJobId: mockStoreState.activeDatasetJobId,
      }),
    }
  ),
}));

vi.mock('@/hooks/useDataset', () => ({
  datasetKeys: {
    list: () => ['dataset', 'list'],
  },
  useDatasetJobStatus: () => ({
    data: mockHookState.job,
  }),
  useCancelDatasetJob: () => ({
    mutate: (...args: unknown[]) => mockCancelMutate(...args),
    isPending: mockHookState.cancelIsPending,
  }),
}));

describe('DatasetJobProgress', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
    mockStoreState.activeDatasetJobId = null;
    mockHookState.job = null;
    mockHookState.cancelIsPending = false;
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders nothing when no active job is selected', () => {
    const { container } = render(<DatasetJobProgress />);
    expect(container.firstChild).toBeNull();
  });

  it('renders running state, elapsed time, and allows cancellation', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-10T00:01:05Z'));

    mockStoreState.activeDatasetJobId = 'job-running';
    mockHookState.job = {
      jobId: 'job-running',
      status: 'running',
      startedAt: '2026-02-10T00:00:00Z',
      progress: null,
    } as DatasetJobResponse;

    render(<DatasetJobProgress />);

    expect(screen.getByText('running')).toBeInTheDocument();
    expect(screen.getByText('1:05')).toBeInTheDocument();
    expect(document.querySelector('.animate-progress-indeterminate')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'キャンセル' }));
    expect(mockCancelMutate).toHaveBeenCalledWith('job-running', expect.any(Object));

    const [, options] = mockCancelMutate.mock.calls[0] as [
      string,
      { onSuccess?: () => void } | undefined,
    ];
    options?.onSuccess?.();
    expect(mockSetActiveDatasetJobId).toHaveBeenCalledWith(null);
  });

  it('renders progress content and caps progress bar width at 100%', () => {
    mockStoreState.activeDatasetJobId = 'job-progress';
    mockHookState.job = {
      jobId: 'job-progress',
      status: 'running',
      startedAt: '2026-02-10T00:00:00Z',
      progress: {
        stage: 'sync',
        percentage: 132.5,
        message: 'processing...',
      },
    } as DatasetJobResponse;

    const { container } = render(<DatasetJobProgress />);

    expect(screen.getByText('sync')).toBeInTheDocument();
    expect(screen.getByText('132.5%')).toBeInTheDocument();
    expect(screen.getByText('processing...')).toBeInTheDocument();
    expect(container.querySelector('[style="width: 100%;"]')).toBeInTheDocument();
  });

  it('invalidates dataset list and auto-clears active id after completion', () => {
    vi.useFakeTimers();
    mockStoreState.activeDatasetJobId = 'job-completed';
    mockHookState.job = {
      jobId: 'job-completed',
      status: 'completed',
      startedAt: '2026-02-10T00:00:00Z',
      result: {
        processedStocks: 8,
        totalStocks: 10,
        warnings: ['warn-1'],
      },
    } as DatasetJobResponse;

    render(<DatasetJobProgress />);

    expect(mockInvalidateQueries).toHaveBeenCalledWith({ queryKey: ['dataset', 'list'] });
    expect(screen.getByText('8/10 銘柄処理完了')).toBeInTheDocument();
    expect(screen.getByText('1 warnings')).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(5000);
    });

    expect(mockSetActiveDatasetJobId).toHaveBeenCalledWith(null);
  });

  it('does not clear active id when a different job became active before timeout', () => {
    vi.useFakeTimers();
    mockStoreState.activeDatasetJobId = 'job-a';
    mockHookState.job = {
      jobId: 'job-a',
      status: 'cancelled',
      startedAt: '2026-02-10T00:00:00Z',
    } as DatasetJobResponse;

    render(<DatasetJobProgress />);

    mockSetActiveDatasetJobId.mockClear();
    mockStoreState.activeDatasetJobId = 'job-b';
    act(() => {
      vi.advanceTimersByTime(5000);
    });

    expect(mockSetActiveDatasetJobId).not.toHaveBeenCalled();
  });

  it('renders failed state error message', () => {
    mockStoreState.activeDatasetJobId = 'job-failed';
    mockHookState.job = {
      jobId: 'job-failed',
      status: 'failed',
      startedAt: '2026-02-10T00:00:00Z',
      error: 'dataset creation failed',
    } as DatasetJobResponse;

    render(<DatasetJobProgress />);

    expect(screen.getByText('failed')).toBeInTheDocument();
    expect(screen.getByText('dataset creation failed')).toBeInTheDocument();
  });
});
