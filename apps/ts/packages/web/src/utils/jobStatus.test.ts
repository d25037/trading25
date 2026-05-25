import { describe, expect, it } from 'vitest';
import { isActiveJobStatus, isTerminalJobStatus, resolveActiveJobRefetchInterval } from './jobStatus';

describe('isActiveJobStatus', () => {
  it('matches pending and running statuses', () => {
    expect(isActiveJobStatus('pending')).toBe(true);
    expect(isActiveJobStatus('running')).toBe(true);
  });

  it('does not match terminal or missing statuses', () => {
    expect(isActiveJobStatus('completed')).toBe(false);
    expect(isActiveJobStatus('failed')).toBe(false);
    expect(isActiveJobStatus('cancelled')).toBe(false);
    expect(isActiveJobStatus(null)).toBe(false);
    expect(isActiveJobStatus(undefined)).toBe(false);
  });
});

describe('isTerminalJobStatus', () => {
  it('matches completed, failed, and cancelled statuses', () => {
    expect(isTerminalJobStatus('completed')).toBe(true);
    expect(isTerminalJobStatus('failed')).toBe(true);
    expect(isTerminalJobStatus('cancelled')).toBe(true);
  });

  it('does not match active or missing statuses', () => {
    expect(isTerminalJobStatus('pending')).toBe(false);
    expect(isTerminalJobStatus('running')).toBe(false);
    expect(isTerminalJobStatus(null)).toBe(false);
    expect(isTerminalJobStatus(undefined)).toBe(false);
  });
});

describe('resolveActiveJobRefetchInterval', () => {
  it('returns the polling interval only for active statuses', () => {
    expect(resolveActiveJobRefetchInterval('pending')).toBe(2000);
    expect(resolveActiveJobRefetchInterval('running', 1000)).toBe(1000);
    expect(resolveActiveJobRefetchInterval('completed')).toBe(false);
  });
});
