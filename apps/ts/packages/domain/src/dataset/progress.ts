/**
 * Dataset V2 - Progress Reporting
 * Simplified progress tracking and reporting
 */

import type { ProgressCallback, ProgressInfo } from './types';

/**
 * Progress tracker for dataset operations
 */
export class ProgressTracker {
  private currentStage: ProgressInfo['stage'] = 'stocks';
  private processed = 0;
  private total = 0;
  private currentItem = '';
  private errors: string[] = [];

  constructor(private callback?: ProgressCallback) {}

  /**
   * Start a new stage with total items
   */
  startStage(stage: ProgressInfo['stage'], total: number): void {
    this.currentStage = stage;
    this.processed = 0;
    this.total = total;
    this.currentItem = '';
    this.reportProgress();
  }

  /**
   * Update progress for current item
   */
  updateProgress(currentItem?: string): void {
    this.processed++;
    if (currentItem) {
      this.currentItem = currentItem;
    }
    this.reportProgress();
  }

  /**
   * Set current item without incrementing progress
   */
  setCurrentItem(item: string): void {
    this.currentItem = item;
    this.reportProgress();
  }

  /**
   * Set progress directly from external source (e.g., fetcher callbacks)
   * Use this when a sub-component reports its own progress
   */
  setProgress(processed: number, total: number, currentItem?: string): void {
    this.processed = processed;
    this.total = total;
    if (currentItem !== undefined) {
      this.currentItem = currentItem;
    }
    this.reportProgress();
  }

  /**
   * Add an error
   */
  addError(error: string): void {
    this.errors.push(error);
    this.reportProgress();
  }

  /**
   * Get current progress info
   */
  getProgress(): ProgressInfo {
    return {
      stage: this.currentStage,
      processed: this.processed,
      total: this.total,
      currentItem: this.currentItem,
      errors: [...this.errors], // Copy to prevent mutations
    };
  }

  /**
   * Check if all items in current stage are processed
   */
  isStageComplete(): boolean {
    return this.processed >= this.total;
  }

  /**
   * Get completion percentage (0-100)
   */
  getPercentage(): number {
    if (this.total === 0) return 0;
    return Math.round((this.processed / this.total) * 100);
  }

  /**
   * Report progress to callback
   */
  private reportProgress(): void {
    if (this.callback) {
      this.callback(this.getProgress());
    }
  }

  /**
   * Clear all errors
   */
  clearErrors(): void {
    this.errors = [];
    this.reportProgress();
  }

  /**
   * Reset tracker
   */
  reset(): void {
    this.processed = 0;
    this.total = 0;
    this.currentItem = '';
    this.errors = [];
    this.currentStage = 'stocks';
    this.reportProgress();
  }
}

/**
 * Simple progress formatter for console output
 */
export class ConsoleProgressFormatter {
  private lastOutput = '';

  /**
   * Format progress info for console display
   */
  format(progress: ProgressInfo): string {
    const percentage = progress.total > 0 ? Math.round((progress.processed / progress.total) * 100) : 0;

    const stage = this.formatStageName(progress.stage);
    const current = progress.currentItem ? ` - ${progress.currentItem}` : '';
    const errors = progress.errors.length > 0 ? ` (${progress.errors.length} errors)` : '';

    return `[${percentage}%] ${stage}: ${progress.processed}/${progress.total}${current}${errors}`;
  }

  /**
   * Print progress to console (overwrites previous line)
   */
  print(progress: ProgressInfo): void {
    const output = this.format(progress);

    // Only print if output changed to avoid spam
    if (output !== this.lastOutput) {
      if (this.lastOutput) {
        // Clear previous line
        process.stdout.write(`\r${' '.repeat(this.lastOutput.length)}\r`);
      }
      process.stdout.write(output);
      this.lastOutput = output;
    }
  }

  /**
   * Print final newline (call when done)
   */
  finish(): void {
    if (this.lastOutput) {
      process.stdout.write('\n');
      this.lastOutput = '';
    }
  }

  private formatStageName(stage: ProgressInfo['stage']): string {
    switch (stage) {
      case 'stocks':
        return 'Stocks';
      case 'quotes':
        return 'Quotes';
      case 'margin':
        return 'Margin';
      case 'topix':
        return 'TOPIX';
      case 'sectors':
        return 'Sectors';
      case 'statements':
        return 'Statements';
      case 'saving':
        return 'Saving';
      default:
        return stage;
    }
  }
}

/**
 * Create a progress callback that logs to console
 */
export function createConsoleProgressCallback(): ProgressCallback {
  const formatter = new ConsoleProgressFormatter();

  return (progress: ProgressInfo) => {
    formatter.print(progress);

    // Print errors immediately
    if (progress.errors.length > 0) {
      const newErrors = progress.errors.slice(-1); // Only show newest error
      for (const error of newErrors) {
        console.error(`\n❌ ${error}`);
      }
    }

    // Finish line if stage complete
    if (progress.processed >= progress.total) {
      formatter.finish();
    }
  };
}

/**
 * Create a silent progress callback (no output)
 */
export function createSilentProgressCallback(): ProgressCallback {
  return () => {
    // Do nothing
  };
}

/**
 * Multi-stage progress tracker for complex operations
 */
export class MultiStageProgressTracker {
  private stages: Array<{
    name: ProgressInfo['stage'];
    weight: number;
    completed: boolean;
    progress: number;
  }> = [];

  private currentStageIndex = 0;

  constructor(private callback?: ProgressCallback) {}

  /**
   * Define the stages and their relative weights
   */
  defineStages(stages: Array<{ name: ProgressInfo['stage']; weight: number }>): void {
    this.stages = stages.map((stage) => ({
      ...stage,
      completed: false,
      progress: 0,
    }));
    this.currentStageIndex = 0;
  }

  /**
   * Update progress for current stage
   */
  updateStageProgress(progress: number, currentItem?: string): void {
    if (this.stages.length === 0) return;

    const currentStage = this.stages[this.currentStageIndex];
    if (currentStage) {
      currentStage.progress = Math.min(100, Math.max(0, progress));

      this.callback?.({
        stage: currentStage.name,
        processed: Math.round(currentStage.progress),
        total: 100,
        currentItem: currentItem || '',
        errors: [],
      });
    }
  }

  /**
   * Complete current stage and move to next
   */
  completeCurrentStage(): void {
    if (this.stages.length === 0) return;

    const currentStage = this.stages[this.currentStageIndex];
    if (currentStage) {
      currentStage.completed = true;
      currentStage.progress = 100;
    }

    this.currentStageIndex++;
  }

  /**
   * Get overall progress percentage
   */
  getOverallProgress(): number {
    if (this.stages.length === 0) return 0;

    const totalWeight = this.stages.reduce((sum, stage) => sum + stage.weight, 0);
    const completedWeight = this.stages.reduce((sum, stage) => {
      return sum + stage.weight * (stage.progress / 100);
    }, 0);

    return Math.round((completedWeight / totalWeight) * 100);
  }

  /**
   * Check if all stages are complete
   */
  isComplete(): boolean {
    return this.stages.every((stage) => stage.completed);
  }

  /**
   * Reset all stages
   */
  reset(): void {
    this.stages.forEach((stage) => {
      stage.completed = false;
      stage.progress = 0;
    });
    this.currentStageIndex = 0;
  }
}
