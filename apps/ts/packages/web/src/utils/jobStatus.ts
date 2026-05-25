const ACTIVE_JOB_STATUSES = ['pending', 'running'] as const;

export function isActiveJobStatus(status: string | null | undefined): boolean {
  return ACTIVE_JOB_STATUSES.includes(status as (typeof ACTIVE_JOB_STATUSES)[number]);
}

export function resolveActiveJobRefetchInterval(status: string | null | undefined, intervalMs = 2000): false | number {
  return isActiveJobStatus(status) ? intervalMs : false;
}
