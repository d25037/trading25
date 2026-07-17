import type { JobStatus } from '@trading25/contracts';

export type { JobStatus };

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

export function resolveActiveJobRefetchInterval(
  status: string | null | undefined,
  intervalMs = 2000
): false | number {
  return isActiveJobStatus(status) ? intervalMs : false;
}
