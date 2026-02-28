import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { SettingsPage } from './SettingsPage';

type SyncJobStatus = {
  jobId: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  mode: string;
  startedAt?: string;
};

const mockStartSyncState = {
  mutate: vi.fn(),
  isPending: false,
  error: null as Error | null,
};

const mockCancelSyncState = {
  mutate: vi.fn(),
  isPending: false,
};

const mockUseSyncJobStatus = vi.fn();

vi.mock('@/hooks/useDbSync', () => ({
  useStartSync: () => mockStartSyncState,
  useCancelSync: () => mockCancelSyncState,
  useSyncJobStatus: (jobId: string | null) => mockUseSyncJobStatus(jobId),
}));

beforeEach(() => {
  vi.clearAllMocks();
  mockStartSyncState.isPending = false;
  mockStartSyncState.error = null;
  mockCancelSyncState.isPending = false;
  mockUseSyncJobStatus.mockImplementation((jobId: string | null) => {
    if (!jobId) {
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    }

    return {
      data: {
        jobId,
        status: 'running',
        mode: 'auto',
        startedAt: '2026-02-13T00:00:00Z',
      } as SyncJobStatus,
      isLoading: false,
      error: null,
    };
  });
  mockStartSyncState.mutate.mockImplementation((_, options) => {
    options?.onSuccess?.({ jobId: 'job-1', status: 'running', mode: 'auto' });
  });
});

describe('SettingsPage', () => {
  it('starts sync and renders running status', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      {
        mode: 'auto',
        dataPlane: {
          backend: 'duckdb-parquet',
          sqliteMirror: false,
        },
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
    expect(await screen.findByRole('button', { name: /Cancel/i })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(mockCancelSyncState.mutate).toHaveBeenCalledWith('job-1');
  });

  it('sends duckdb data plane override by default and allows sqlite mirror override', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('switch', { name: 'SQLite Mirror' }));
    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      {
        mode: 'auto',
        dataPlane: {
          backend: 'duckdb-parquet',
          sqliteMirror: true,
        },
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('shows starting state while request is in progress', () => {
    mockStartSyncState.isPending = true;

    render(<SettingsPage />);

    expect(screen.getByRole('button', { name: /Starting.../i })).toBeInTheDocument();
  });

  it('sends selected sync mode', async () => {
    const user = userEvent.setup();

    render(<SettingsPage />);

    await user.click(screen.getByRole('combobox', { name: 'Sync Mode' }));
    await user.click(screen.getByRole('option', { name: /Incremental Backfill/i }));
    await user.click(screen.getByRole('button', { name: /Start Sync/i }));

    expect(mockStartSyncState.mutate).toHaveBeenCalledWith(
      {
        mode: 'incremental',
        dataPlane: {
          backend: 'duckdb-parquet',
          sqliteMirror: false,
        },
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
  });

  it('shows sync error message when start sync fails', () => {
    mockStartSyncState.error = new Error('Sync failed');

    render(<SettingsPage />);

    expect(screen.getByText('Sync failed')).toBeInTheDocument();
  });
});
