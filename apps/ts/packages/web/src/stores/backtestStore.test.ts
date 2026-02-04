import { beforeEach, describe, expect, it } from 'vitest';
import { useBacktestStore } from './backtestStore';

const resetBacktestStore = () => {
  useBacktestStore.setState({
    activeSubTab: 'runner',
    selectedStrategy: null,
    activeJobId: null,
    selectedResultJobId: null,
    activeOptimizationJobId: null,
    activeDatasetJobId: null,
    selectedDatasetName: null,
    activeLabJobId: null,
    activeLabType: null,
  });
};

describe('backtestStore', () => {
  beforeEach(() => {
    useBacktestStore.persist?.clearStorage?.();
    resetBacktestStore();
  });

  it('updates backtest state via actions', () => {
    const {
      setActiveSubTab,
      setSelectedStrategy,
      setActiveJobId,
      setSelectedResultJobId,
      setActiveOptimizationJobId,
      setActiveDatasetJobId,
      setSelectedDatasetName,
    } = useBacktestStore.getState();

    setActiveSubTab('results');
    setSelectedStrategy('strategy.yml');
    setActiveJobId('job-1');
    setSelectedResultJobId('job-2');
    setActiveOptimizationJobId('opt-1');
    setActiveDatasetJobId('ds-1');
    setSelectedDatasetName('prime.db');

    const state = useBacktestStore.getState();
    expect(state.activeSubTab).toBe('results');
    expect(state.selectedStrategy).toBe('strategy.yml');
    expect(state.activeJobId).toBe('job-1');
    expect(state.selectedResultJobId).toBe('job-2');
    expect(state.activeOptimizationJobId).toBe('opt-1');
    expect(state.activeDatasetJobId).toBe('ds-1');
    expect(state.selectedDatasetName).toBe('prime.db');
  });

  it('updates lab state via actions', () => {
    const { setActiveLabJobId, setActiveLabType } = useBacktestStore.getState();

    setActiveLabJobId('lab-job-1');
    setActiveLabType('generate');

    const state = useBacktestStore.getState();
    expect(state.activeLabJobId).toBe('lab-job-1');
    expect(state.activeLabType).toBe('generate');
  });

  it('sets lab tab as active sub tab', () => {
    const { setActiveSubTab } = useBacktestStore.getState();

    setActiveSubTab('lab');

    const state = useBacktestStore.getState();
    expect(state.activeSubTab).toBe('lab');
  });

  it('clears lab state', () => {
    const { setActiveLabJobId, setActiveLabType } = useBacktestStore.getState();

    setActiveLabJobId('lab-job-1');
    setActiveLabType('evolve');

    setActiveLabJobId(null);
    setActiveLabType(null);

    const state = useBacktestStore.getState();
    expect(state.activeLabJobId).toBeNull();
    expect(state.activeLabType).toBeNull();
  });
});
