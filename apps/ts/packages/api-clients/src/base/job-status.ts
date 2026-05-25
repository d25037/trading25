export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

const ACTIVE_JOB_STATUSES = ['pending', 'running'] as const;
const TERMINAL_JOB_STATUSES = ['completed', 'failed', 'cancelled'] as const;

export function isActiveJobStatus(status: JobStatus): boolean {
  return ACTIVE_JOB_STATUSES.includes(status as (typeof ACTIVE_JOB_STATUSES)[number]);
}

export function isTerminalJobStatus(status: JobStatus): boolean {
  return TERMINAL_JOB_STATUSES.includes(status as (typeof TERMINAL_JOB_STATUSES)[number]);
}
