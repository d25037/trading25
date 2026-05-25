export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

const ACTIVE_JOB_STATUSES = ['pending', 'running'] as const;
const TERMINAL_JOB_STATUSES = ['completed', 'failed', 'cancelled'] as const;

export type ActiveJobStatus = (typeof ACTIVE_JOB_STATUSES)[number];
export type TerminalJobStatus = (typeof TERMINAL_JOB_STATUSES)[number];

export function isActiveJobStatus(status: string | null | undefined): status is ActiveJobStatus {
  return ACTIVE_JOB_STATUSES.includes(status as ActiveJobStatus);
}

export function isTerminalJobStatus(status: string | null | undefined): status is TerminalJobStatus {
  return TERMINAL_JOB_STATUSES.includes(status as TerminalJobStatus);
}
