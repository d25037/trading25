import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { LabOptimizeForm } from './LabOptimizeForm';

describe('LabOptimizeForm', () => {
  it('submits default payload', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'tpe',
      structure_mode: 'params_only',
      target_scope: 'both',
    });
  });

  it('submits fundamental-only constraints', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    const comboboxes = screen.getAllByRole('combobox');
    const targetScopeCombobox = comboboxes[1];
    const categoryCombobox = comboboxes[2];
    expect(targetScopeCombobox).toBeDefined();
    expect(categoryCombobox).toBeDefined();
    if (!targetScopeCombobox || !categoryCombobox) return;
    await user.click(targetScopeCombobox);
    await user.click(screen.getByText('entry filter only'));
    await user.click(categoryCombobox);
    await user.click(screen.getByText('fundamental only'));
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'tpe',
      structure_mode: 'params_only',
      target_scope: 'entry_filter_only',
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('submits random_add payload with signal counts and seed', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    const comboboxes = screen.getAllByRole('combobox');
    const samplerCombobox = comboboxes[0];
    const structureCombobox = comboboxes[3];
    expect(samplerCombobox).toBeDefined();
    expect(structureCombobox).toBeDefined();
    if (!samplerCombobox || !structureCombobox) return;

    await user.click(samplerCombobox);
    await user.click(screen.getByText('Random'));
    await user.click(structureCombobox);
    await user.click(screen.getByText('Fix Existing Signals + Add New Signals'));

    await user.clear(screen.getByLabelText('Add Entry Signals'));
    await user.type(screen.getByLabelText('Add Entry Signals'), '2');
    await user.clear(screen.getByLabelText('Add Exit Signals'));
    await user.type(screen.getByLabelText('Add Exit Signals'), '3');
    await user.type(screen.getByLabelText('Seed (optional)'), '42');
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'random',
      structure_mode: 'random_add',
      target_scope: 'both',
      random_add_entry_signals: 2,
      random_add_exit_signals: 3,
      seed: 42,
    });
  });

  it('submits exit-only random_add payload with entry add count fixed to zero', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    const comboboxes = screen.getAllByRole('combobox');
    const targetScopeCombobox = comboboxes[1];
    const structureCombobox = comboboxes[3];
    expect(targetScopeCombobox).toBeDefined();
    expect(structureCombobox).toBeDefined();
    if (!targetScopeCombobox || !structureCombobox) return;

    await user.click(targetScopeCombobox);
    await user.click(screen.getByText('exit trigger only'));
    await user.click(structureCombobox);
    await user.click(screen.getByText('Fix Existing Signals + Add New Signals'));
    await user.clear(screen.getByLabelText('Add Exit Signals'));
    await user.type(screen.getByLabelText('Add Exit Signals'), '4');
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'tpe',
      structure_mode: 'random_add',
      target_scope: 'exit_trigger_only',
      random_add_entry_signals: 0,
      random_add_exit_signals: 4,
    });
  });

  it('falls back to default trials when input is invalid', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.clear(screen.getByLabelText('Trials'));
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'tpe',
      structure_mode: 'params_only',
      target_scope: 'both',
    });
  });

  it('disables submit when strategy is not selected', () => {
    const onSubmit = vi.fn();
    render(<LabOptimizeForm strategyName={null} onSubmit={onSubmit} />);

    expect(screen.getByRole('button', { name: 'Start Optimization' })).toBeDisabled();
  });
});
