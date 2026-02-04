import { describe, expect, spyOn, test } from 'bun:test';
import { getElementOrFail } from '../test-utils/array-helpers';
import {
  ConsoleProgressFormatter,
  createConsoleProgressCallback,
  createSilentProgressCallback,
  MultiStageProgressTracker,
  ProgressTracker,
} from './progress';
import type { ProgressInfo } from './types';

describe('ProgressTracker', () => {
  test('initializes with default values', () => {
    const tracker = new ProgressTracker();
    const progress = tracker.getProgress();
    expect(progress.stage).toBe('stocks');
    expect(progress.processed).toBe(0);
    expect(progress.total).toBe(0);
    expect(progress.currentItem).toBe('');
    expect(progress.errors).toEqual([]);
  });

  test('startStage sets stage and total', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('quotes', 100);
    const progress = tracker.getProgress();
    expect(progress.stage).toBe('quotes');
    expect(progress.total).toBe(100);
    expect(progress.processed).toBe(0);
  });

  test('updateProgress increments processed count', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('stocks', 10);
    tracker.updateProgress('item1');
    expect(tracker.getProgress().processed).toBe(1);
    expect(tracker.getProgress().currentItem).toBe('item1');
  });

  test('updateProgress without currentItem still increments', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('stocks', 10);
    tracker.updateProgress();
    expect(tracker.getProgress().processed).toBe(1);
  });

  test('setCurrentItem updates item without incrementing', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('stocks', 10);
    tracker.setCurrentItem('loading...');
    expect(tracker.getProgress().currentItem).toBe('loading...');
    expect(tracker.getProgress().processed).toBe(0);
  });

  test('setProgress sets processed and total directly', () => {
    const tracker = new ProgressTracker();
    tracker.setProgress(50, 200, 'halfway');
    const progress = tracker.getProgress();
    expect(progress.processed).toBe(50);
    expect(progress.total).toBe(200);
    expect(progress.currentItem).toBe('halfway');
  });

  test('setProgress without currentItem keeps existing', () => {
    const tracker = new ProgressTracker();
    tracker.setCurrentItem('existing');
    tracker.setProgress(10, 20);
    expect(tracker.getProgress().currentItem).toBe('existing');
  });

  test('addError appends error', () => {
    const tracker = new ProgressTracker();
    tracker.addError('something failed');
    tracker.addError('another failure');
    expect(tracker.getProgress().errors).toEqual(['something failed', 'another failure']);
  });

  test('getProgress returns a copy of errors', () => {
    const tracker = new ProgressTracker();
    tracker.addError('err1');
    const progress = tracker.getProgress();
    progress.errors.push('mutated');
    expect(tracker.getProgress().errors).toEqual(['err1']);
  });

  test('isStageComplete returns true when processed >= total', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('stocks', 2);
    expect(tracker.isStageComplete()).toBe(false);
    tracker.updateProgress();
    tracker.updateProgress();
    expect(tracker.isStageComplete()).toBe(true);
  });

  test('getPercentage returns 0 when total is 0', () => {
    const tracker = new ProgressTracker();
    expect(tracker.getPercentage()).toBe(0);
  });

  test('getPercentage returns rounded percentage', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('quotes', 3);
    tracker.updateProgress();
    expect(tracker.getPercentage()).toBe(33);
  });

  test('clearErrors removes all errors', () => {
    const tracker = new ProgressTracker();
    tracker.addError('err1');
    tracker.addError('err2');
    tracker.clearErrors();
    expect(tracker.getProgress().errors).toEqual([]);
  });

  test('reset clears all state', () => {
    const tracker = new ProgressTracker();
    tracker.startStage('margin', 50);
    tracker.updateProgress('item');
    tracker.addError('err');
    tracker.reset();

    const progress = tracker.getProgress();
    expect(progress.stage).toBe('stocks');
    expect(progress.processed).toBe(0);
    expect(progress.total).toBe(0);
    expect(progress.currentItem).toBe('');
    expect(progress.errors).toEqual([]);
  });

  test('invokes callback on every state change', () => {
    const calls: ProgressInfo[] = [];
    const tracker = new ProgressTracker((p) => calls.push({ ...p }));

    tracker.startStage('quotes', 5);
    tracker.updateProgress('a');
    tracker.setCurrentItem('b');
    tracker.addError('err');
    tracker.clearErrors();
    tracker.reset();

    expect(calls.length).toBe(6);
    expect(calls[0]?.stage).toBe('quotes');
    expect(calls[1]?.processed).toBe(1);
    expect(calls[2]?.currentItem).toBe('b');
    expect(calls[3]?.errors).toEqual(['err']);
    expect(calls[4]?.errors).toEqual([]);
    expect(calls[5]?.stage).toBe('stocks');
  });
});

describe('ConsoleProgressFormatter', () => {
  test('format produces correct output', () => {
    const formatter = new ConsoleProgressFormatter();
    const result = formatter.format({
      stage: 'stocks',
      processed: 5,
      total: 10,
      currentItem: 'Toyota',
      errors: [],
    });
    expect(result).toBe('[50%] Stocks: 5/10 - Toyota');
  });

  test('format handles zero total', () => {
    const formatter = new ConsoleProgressFormatter();
    const result = formatter.format({
      stage: 'quotes',
      processed: 0,
      total: 0,
      errors: [],
    });
    expect(result).toBe('[0%] Quotes: 0/0');
  });

  test('format shows error count', () => {
    const formatter = new ConsoleProgressFormatter();
    const result = formatter.format({
      stage: 'margin',
      processed: 3,
      total: 10,
      errors: ['err1', 'err2'],
    });
    expect(result).toContain('(2 errors)');
  });

  test('format maps all stage names', () => {
    const formatter = new ConsoleProgressFormatter();
    const stages: ProgressInfo['stage'][] = ['stocks', 'quotes', 'margin', 'topix', 'sectors', 'statements', 'saving'];
    const expected = ['Stocks', 'Quotes', 'Margin', 'TOPIX', 'Sectors', 'Statements', 'Saving'];

    for (let i = 0; i < stages.length; i++) {
      const result = formatter.format({
        stage: getElementOrFail(stages, i),
        processed: 0,
        total: 0,
        errors: [],
      });
      expect(result).toContain(getElementOrFail(expected, i));
    }
  });

  test('print writes to stdout and skips duplicate output', () => {
    const formatter = new ConsoleProgressFormatter();
    const writeSpy = spyOn(process.stdout, 'write').mockReturnValue(true);

    const progress: ProgressInfo = {
      stage: 'stocks',
      processed: 1,
      total: 10,
      errors: [],
    };

    formatter.print(progress);
    expect(writeSpy).toHaveBeenCalledTimes(1);

    // Same output should be skipped
    formatter.print(progress);
    expect(writeSpy).toHaveBeenCalledTimes(1);

    // Different output should print (clears previous + writes new)
    formatter.print({ ...progress, processed: 2 });
    expect(writeSpy).toHaveBeenCalledTimes(3); // clear + write

    writeSpy.mockRestore();
  });

  test('finish writes newline and resets', () => {
    const formatter = new ConsoleProgressFormatter();
    const writeSpy = spyOn(process.stdout, 'write').mockReturnValue(true);

    formatter.print({
      stage: 'stocks',
      processed: 10,
      total: 10,
      errors: [],
    });

    formatter.finish();
    expect(writeSpy).toHaveBeenLastCalledWith('\n');

    // Calling finish again should be a no-op
    const callCount = writeSpy.mock.calls.length;
    formatter.finish();
    expect(writeSpy.mock.calls.length).toBe(callCount);

    writeSpy.mockRestore();
  });
});

describe('createConsoleProgressCallback', () => {
  test('returns a function that logs progress', () => {
    const writeSpy = spyOn(process.stdout, 'write').mockReturnValue(true);
    const errorSpy = spyOn(console, 'error').mockImplementation(() => {});

    const callback = createConsoleProgressCallback();

    callback({
      stage: 'stocks',
      processed: 5,
      total: 10,
      errors: [],
    });

    expect(writeSpy).toHaveBeenCalled();

    // With errors
    callback({
      stage: 'stocks',
      processed: 6,
      total: 10,
      errors: ['something failed'],
    });

    expect(errorSpy).toHaveBeenCalled();

    // Stage complete triggers finish
    callback({
      stage: 'stocks',
      processed: 10,
      total: 10,
      errors: [],
    });

    writeSpy.mockRestore();
    errorSpy.mockRestore();
  });
});

describe('createSilentProgressCallback', () => {
  test('returns a no-op function', () => {
    const callback = createSilentProgressCallback();
    // Should not throw
    callback({
      stage: 'stocks',
      processed: 0,
      total: 0,
      errors: [],
    });
  });
});

describe('MultiStageProgressTracker', () => {
  test('initializes with empty stages', () => {
    const tracker = new MultiStageProgressTracker();
    expect(tracker.getOverallProgress()).toBe(0);
    expect(tracker.isComplete()).toBe(true); // No stages = trivially complete
  });

  test('defineStages sets up stages', () => {
    const tracker = new MultiStageProgressTracker();
    tracker.defineStages([
      { name: 'stocks', weight: 1 },
      { name: 'quotes', weight: 2 },
    ]);
    expect(tracker.getOverallProgress()).toBe(0);
    expect(tracker.isComplete()).toBe(false);
  });

  test('updateStageProgress updates current stage', () => {
    const calls: ProgressInfo[] = [];
    const tracker = new MultiStageProgressTracker((p) => calls.push({ ...p }));
    tracker.defineStages([
      { name: 'stocks', weight: 1 },
      { name: 'quotes', weight: 1 },
    ]);

    tracker.updateStageProgress(50, 'halfway');
    expect(calls.length).toBe(1);
    expect(calls[0]?.stage).toBe('stocks');
    expect(calls[0]?.processed).toBe(50);
  });

  test('updateStageProgress clamps to 0-100', () => {
    const tracker = new MultiStageProgressTracker();
    tracker.defineStages([{ name: 'stocks', weight: 1 }]);
    tracker.updateStageProgress(150);
    expect(tracker.getOverallProgress()).toBe(100);

    tracker.updateStageProgress(-10);
    expect(tracker.getOverallProgress()).toBe(0);
  });

  test('updateStageProgress is no-op when no stages defined', () => {
    const tracker = new MultiStageProgressTracker();
    // Should not throw
    tracker.updateStageProgress(50);
  });

  test('completeCurrentStage advances to next stage', () => {
    const tracker = new MultiStageProgressTracker();
    tracker.defineStages([
      { name: 'stocks', weight: 1 },
      { name: 'quotes', weight: 1 },
    ]);

    tracker.completeCurrentStage();
    expect(tracker.getOverallProgress()).toBe(50);
    expect(tracker.isComplete()).toBe(false);

    tracker.completeCurrentStage();
    expect(tracker.getOverallProgress()).toBe(100);
    expect(tracker.isComplete()).toBe(true);
  });

  test('completeCurrentStage is no-op when no stages', () => {
    const tracker = new MultiStageProgressTracker();
    // Should not throw
    tracker.completeCurrentStage();
  });

  test('getOverallProgress considers weights', () => {
    const tracker = new MultiStageProgressTracker();
    tracker.defineStages([
      { name: 'stocks', weight: 1 },
      { name: 'quotes', weight: 3 },
    ]);

    tracker.completeCurrentStage(); // stocks 100% (weight 1)
    // Overall: 1/4 = 25%
    expect(tracker.getOverallProgress()).toBe(25);
  });

  test('reset clears all progress', () => {
    const tracker = new MultiStageProgressTracker();
    tracker.defineStages([
      { name: 'stocks', weight: 1 },
      { name: 'quotes', weight: 1 },
    ]);

    tracker.completeCurrentStage();
    tracker.reset();
    expect(tracker.getOverallProgress()).toBe(0);
    expect(tracker.isComplete()).toBe(false);
  });
});
