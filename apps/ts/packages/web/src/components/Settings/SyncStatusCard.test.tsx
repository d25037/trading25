import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { SyncJobResponse } from '@/types/sync';
import { SyncStatusCard } from './SyncStatusCard';

function createJob(overrides: Partial<SyncJobResponse> = {}): SyncJobResponse {
  return {
    jobId: 'job-1',
    status: 'running',
    mode: 'incremental',
    startedAt: '2026-03-04T00:00:00Z',
    progress: {
      stage: 'stock_data',
      current: 2,
      total: 5,
      percentage: 40,
      message: 'Fetch strategy: /equities/bars/daily -> BULK (REST est=120, BULK est=6, targets=42 dates)',
    },
    ...overrides,
  };
}

describe('SyncStatusCard', () => {
  it('renders nothing when job is null', () => {
    const { container } = render(
      <SyncStatusCard job={null} isLoading={false} onCancel={vi.fn()} isCancelling={false} />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('shows running progress with BULK fetch info and triggers cancel', async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    render(
      <SyncStatusCard
        job={createJob()}
        isLoading={false}
        onCancel={onCancel}
        isCancelling={false}
      />
    );

    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(screen.getByText('stock_data')).toBeInTheDocument();
    expect(screen.getByText('40.0%')).toBeInTheDocument();
    expect(screen.getByText('Fetch')).toBeInTheDocument();
    expect(screen.getByText('BULK')).toBeInTheDocument();
    expect(screen.getByText('/equities/bars/daily')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('shows REST fetch info when progress message indicates REST execution', () => {
    render(
      <SyncStatusCard
        job={createJob({
          progress: {
            stage: 'indices',
            current: 3,
            total: 5,
            percentage: 60,
            message: 'Fetching /indices/bars/daily via REST (bulk fallback), targets=30 codes...',
          },
        })}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.getByText('REST')).toBeInTheDocument();
    expect(screen.getByText('/indices/bars/daily')).toBeInTheDocument();
  });

  it('hides fetch info when message does not contain method or endpoint', () => {
    render(
      <SyncStatusCard
        job={createJob({
          progress: {
            stage: 'stock_data',
            current: 1,
            total: 5,
            percentage: 20,
            message: 'Preparing fetch plan...',
          },
        })}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.queryByText('Fetch')).not.toBeInTheDocument();
  });

  it('renders completed result summary including failed dates count', () => {
    render(
      <SyncStatusCard
        job={createJob({
          status: 'completed',
          progress: undefined,
          result: {
            success: true,
            totalApiCalls: 55,
            stocksUpdated: 120,
            datesProcessed: 10,
            fundamentalsUpdated: 80,
            fundamentalsDatesProcessed: 3,
            failedDates: ['2026-03-01'],
            errors: [],
          },
        })}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(screen.getByText('API Calls:')).toBeInTheDocument();
    expect(screen.getByText('55')).toBeInTheDocument();
    expect(screen.getByText('Failed Dates:')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();
  });

  it('renders failed status with error text', () => {
    render(
      <SyncStatusCard
        job={createJob({
          status: 'failed',
          progress: undefined,
          error: 'sync failed',
        })}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.getByText('sync failed')).toBeInTheDocument();
  });

  it('renders cancelled status message', () => {
    render(
      <SyncStatusCard
        job={createJob({
          status: 'cancelled',
          progress: undefined,
        })}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.getByText('Cancelled')).toBeInTheDocument();
    expect(screen.getByText('Sync was cancelled by user.')).toBeInTheDocument();
  });

  it('handles unexpected status value without crashing', () => {
    const unknownStatusJob = { ...createJob({ progress: undefined }), status: 'unknown' } as unknown as SyncJobResponse;
    render(
      <SyncStatusCard
        job={unknownStatusJob}
        isLoading={false}
        onCancel={vi.fn()}
        isCancelling={false}
      />
    );

    expect(screen.getByText('Mode: incremental')).toBeInTheDocument();
  });
});
