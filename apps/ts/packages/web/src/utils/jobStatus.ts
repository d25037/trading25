const ACTIVE_JOB_STATUSES = ['pending', 'running'] as const;
const TERMINAL_JOB_STATUSES = ['completed', 'failed', 'cancelled'] as const;

export function isActiveJobStatus(status: string | null | undefined): boolean {
  return ACTIVE_JOB_STATUSES.includes(status as (typeof ACTIVE_JOB_STATUSES)[number]);
}

export function isTerminalJobStatus(status: string | null | undefined): boolean {
  return TERMINAL_JOB_STATUSES.includes(status as (typeof TERMINAL_JOB_STATUSES)[number]);
}

export function resolveActiveJobRefetchInterval(status: string | null | undefined, intervalMs = 2000): false | number {
  return isActiveJobStatus(status) ? intervalMs : false;
}
