import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ScreeningJobResponse } from '@/types/screening';
import { ScreeningJobProgress, ScreeningJobStatusInline } from './ScreeningJobProgress';

function createJob(overrides: Partial<ScreeningJobResponse> = {}): ScreeningJobResponse {
  return {
    job_id: 'job-1',
    status: 'pending',
    created_at: '2026-02-18T12:00:00Z',
    markets: 'prime',
    recentDays: 10,
    sortBy: 'matchedDate',
    order: 'desc',
    ...overrides,
  };
}

describe('ScreeningJobProgress', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-18T12:01:00Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders nothing when there is no job', () => {
    const { container } = render(<ScreeningJobProgress job={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders pending progress with elapsed time and cancel action', () => {
    const onCancel = vi.fn();

    render(<ScreeningJobProgress job={createJob()} onCancel={onCancel} />);

    act(() => {
      vi.advanceTimersByTime(0);
    });

    expect(screen.getByText('Screening Job: pending')).toBeInTheDocument();
    expect(screen.getByText('1:00')).toBeInTheDocument();
    expect(screen.getByText('Running...')).toBeInTheDocument();
    expect(screen.queryByText('%')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('renders determinate running progress using started_at and message', () => {
    const { container } = render(
      <ScreeningJobProgress
        job={createJob({
          status: 'running',
          started_at: '2026-02-18T12:00:30Z',
          progress: 0.42,
          message: 'Fetching results',
        })}
      />
    );

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(screen.getByText('Screening Job: running')).toBeInTheDocument();
    expect(screen.getByText('0:31')).toBeInTheDocument();
    expect(screen.getByText('Fetching results')).toBeInTheDocument();
    expect(screen.getByText('42%')).toBeInTheDocument();
    expect(container.querySelector('[style*="width: 42%"]')).not.toBeNull();
  });

  it('disables cancel button while cancellation is pending', () => {
    render(<ScreeningJobProgress job={createJob()} onCancel={vi.fn()} isCancelling />);
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled();
  });

  it('renders failure state with error message', () => {
    render(<ScreeningJobProgress job={createJob({ status: 'failed', error: 'backend failed' })} />);
    expect(screen.getByText('Screening Job: failed')).toBeInTheDocument();
    expect(screen.getByText('backend failed')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel' })).not.toBeInTheDocument();
  });

  it('renders cancelled state message', () => {
    render(<ScreeningJobProgress job={createJob({ status: 'cancelled' })} />);
    expect(screen.getByText('Screening Job: cancelled')).toBeInTheDocument();
    expect(screen.getByText('Screening was cancelled.')).toBeInTheDocument();
  });
});

describe('ScreeningJobStatusInline', () => {
  it('renders completed status inline', () => {
    render(<ScreeningJobStatusInline job={createJob({ status: 'completed' })} />);
    expect(screen.getByText('Screening Job: completed')).toBeInTheDocument();
  });

  it('renders unknown status text without crashing', () => {
    render(
      <ScreeningJobStatusInline
        job={createJob({
          status: 'mystery' as unknown as ScreeningJobResponse['status'],
        })}
      />
    );

    expect(screen.getByText('Screening Job: mystery')).toBeInTheDocument();
  });
});
