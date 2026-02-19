import { afterEach, describe, expect, it, mock, spyOn } from 'bun:test';
import {
  CLIAPIError,
  CLICancelError,
  CLIError,
  CLINotFoundError,
  CLIValidationError,
  DATASET_TIPS,
  DB_TIPS,
  displayTroubleshootingTips,
  handleCommandError,
} from './error-handling.js';

type Spinner = {
  fail: ReturnType<typeof mock>;
  stop: ReturnType<typeof mock>;
};

function createSpinner(): Spinner {
  return {
    fail: mock(() => undefined),
    stop: mock(() => undefined),
  };
}

describe('error-handling utilities', () => {
  afterEach(() => {
    mock.restore();
  });

  it('provides structured CLI error classes', () => {
    const base = new CLIError('base');
    expect(base.name).toBe('CLIError');
    expect(base.exitCode).toBe(1);
    expect(base.silent).toBe(false);

    const validation = new CLIValidationError('invalid input');
    expect(validation.name).toBe('CLIValidationError');
    expect(validation.exitCode).toBe(1);
    expect(validation.silent).toBe(false);

    const cancelled = new CLICancelError();
    expect(cancelled.name).toBe('CLICancelError');
    expect(cancelled.exitCode).toBe(0);
    expect(cancelled.silent).toBe(true);

    const notFound = new CLINotFoundError('missing');
    expect(notFound.name).toBe('CLINotFoundError');
    expect(notFound.exitCode).toBe(1);

    const apiError = new CLIAPIError('api down');
    expect(apiError.name).toBe('CLIAPIError');
    expect(apiError.exitCode).toBe(1);
  });

  it('prints troubleshooting tips in consistent format', () => {
    const errorSpy = spyOn(console, 'error').mockImplementation(() => undefined);
    displayTroubleshootingTips(['Tip A', 'Tip B']);

    const lines = errorSpy.mock.calls.map((call) => String(call[0] ?? ''));
    expect(lines[0]).toContain('Troubleshooting tips');
    expect(lines).toContain('   • Tip A');
    expect(lines).toContain('   • Tip B');
  });

  it('rethrows existing CLIError without wrapping', () => {
    const spinner = createSpinner();
    const original = new CLIValidationError('invalid');

    let thrown: unknown;
    try {
      handleCommandError(original, spinner as never, {
        failMessage: 'should not be used',
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBe(original);
    expect(spinner.stop).toHaveBeenCalledTimes(1);
    expect(spinner.fail).toHaveBeenCalledTimes(0);
  });

  it('wraps generic errors, logs details, and includes custom tips', () => {
    const spinner = createSpinner();
    const errorSpy = spyOn(console, 'error').mockImplementation(() => undefined);
    const originalError = new Error('request failed');
    originalError.stack = 'stack trace for debug';

    let thrown: unknown;
    try {
      handleCommandError(originalError, spinner as never, {
        failMessage: 'Operation failed',
        debug: true,
        tips: ['custom tip'],
      });
    } catch (error) {
      thrown = error;
    }

    expect(spinner.fail).toHaveBeenCalledWith('Operation failed');
    expect(thrown).toBeInstanceOf(CLIError);
    expect(thrown).toBeInstanceOf(Error);

    if (thrown instanceof CLIError) {
      expect(thrown.message).toBe('request failed');
      expect(thrown.silent).toBe(true);
    } else {
      throw new Error('Expected CLIError to be thrown');
    }

    const lines = errorSpy.mock.calls.map((call) => String(call[0] ?? ''));
    expect(lines.some((line) => line.includes('Error: request failed'))).toBe(true);
    expect(lines.some((line) => line.includes('[DEBUG] Stack trace'))).toBe(true);
    expect(lines.some((line) => line.includes('custom tip'))).toBe(true);
  });

  it('uses default tips for non-Error values', () => {
    const spinner = createSpinner();
    const errorSpy = spyOn(console, 'error').mockImplementation(() => undefined);

    let thrown: unknown;
    try {
      handleCommandError('plain failure', spinner as never, {
        failMessage: 'Failed',
      });
    } catch (error) {
      thrown = error;
    }

    expect(spinner.fail).toHaveBeenCalledWith('Failed');
    expect(thrown).toBeInstanceOf(CLIError);
    const lines = errorSpy.mock.calls.map((call) => String(call[0] ?? ''));
    expect(lines.some((line) => line.includes('Ensure the API server is running'))).toBe(true);
  });

  it('exports DB and dataset troubleshooting tips', () => {
    expect(DB_TIPS.refresh.length).toBeGreaterThan(0);
    expect(DB_TIPS.sync.length).toBeGreaterThan(0);

    expect(DATASET_TIPS.info.length).toBeGreaterThan(0);
    expect(DATASET_TIPS.create.length).toBeGreaterThan(0);
    expect(DATASET_TIPS.createTimeout.length).toBeGreaterThan(0);
    expect(DATASET_TIPS.search.length).toBeGreaterThan(0);
    expect(DATASET_TIPS.sample.length).toBeGreaterThan(0);
  });
});
