import { beforeEach, describe, expect, it } from 'vitest';
import { useBacktestStore } from './backtestStore';

const resetBacktestStore = () => {
  useBacktestStore.setState({
    activeJobId: null,
    activeAttributionJobId: null,
    activeOptimizationJobId: null,
    activeDatasetJobId: null,
    activeLabJobId: null,
  });
};

describe('backtestStore', () => {
  beforeEach(() => {
    useBacktestStore.persist?.clearStorage?.();
    resetBacktestStore();
  });

  it('updates backtest state via actions', () => {
    const {
      setActiveJobId,
      setActiveAttributionJobId,
      setActiveOptimizationJobId,
      setActiveDatasetJobId,
      setActiveLabJobId,
    } = useBacktestStore.getState();

    setActiveJobId('job-1');
    setActiveAttributionJobId('attr-1');
    setActiveOptimizationJobId('opt-1');
    setActiveDatasetJobId('ds-1');
    setActiveLabJobId('lab-1');

    const state = useBacktestStore.getState();
    expect(state.activeJobId).toBe('job-1');
    expect(state.activeAttributionJobId).toBe('attr-1');
    expect(state.activeOptimizationJobId).toBe('opt-1');
    expect(state.activeDatasetJobId).toBe('ds-1');
    expect(state.activeLabJobId).toBe('lab-1');
  });

  it('clears active lab job state', () => {
    const { setActiveLabJobId } = useBacktestStore.getState();

    setActiveLabJobId('lab-job-1');
    setActiveLabJobId(null);

    const state = useBacktestStore.getState();
    expect(state.activeLabJobId).toBeNull();
  });
});
