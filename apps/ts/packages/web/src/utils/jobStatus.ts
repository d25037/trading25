import { isActiveJobStatus, isTerminalJobStatus } from '@trading25/api-clients/base/job-status';

export { isActiveJobStatus, isTerminalJobStatus };

export function resolveActiveJobRefetchInterval(status: string | null | undefined, intervalMs = 2000): false | number {
  return isActiveJobStatus(status) ? intervalMs : false;
}
