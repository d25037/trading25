import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { LabEvolveForm } from './LabEvolveForm';

describe('LabEvolveForm', () => {
  it('submits default payload', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabEvolveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);
    await user.click(screen.getByRole('button', { name: 'Start Evolution' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      generations: 10,
      population: 20,
      structure_mode: 'params_only',
    });
  });

  it('submits fundamental-only constraints', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabEvolveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));
    const comboboxes = screen.getAllByRole('combobox');
    const categoryCombobox = comboboxes[0];
    expect(categoryCombobox).toBeDefined();
    if (!categoryCombobox) return;
    await user.click(categoryCombobox);
    await user.click(screen.getByText('Fundamental Only'));
    await user.click(screen.getByRole('button', { name: 'Start Evolution' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      generations: 10,
      population: 20,
      structure_mode: 'params_only',
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('submits random_add payload with signal counts and seed', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabEvolveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    const comboboxes = screen.getAllByRole('combobox');
    const structureCombobox = comboboxes[1];
    expect(structureCombobox).toBeDefined();
    if (!structureCombobox) return;
    await user.click(structureCombobox);
    await user.click(screen.getByText('Random Add Signals'));

    await user.clear(screen.getByLabelText('Add Entry Signals'));
    await user.type(screen.getByLabelText('Add Entry Signals'), '2');
    await user.clear(screen.getByLabelText('Add Exit Signals'));
    await user.type(screen.getByLabelText('Add Exit Signals'), '3');
    await user.type(screen.getByLabelText('Seed (optional)'), '42');
    await user.click(screen.getByRole('button', { name: 'Start Evolution' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      generations: 10,
      population: 20,
      structure_mode: 'random_add',
      random_add_entry_signals: 2,
      random_add_exit_signals: 3,
      seed: 42,
    });
  });

  it('falls back to defaults when generations/population are invalid', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabEvolveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.clear(screen.getByLabelText('Generations'));
    await user.clear(screen.getByLabelText('Population'));
    await user.click(screen.getByRole('button', { name: 'Start Evolution' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      generations: 10,
      population: 20,
      structure_mode: 'params_only',
    });
  });

  it('disables submit when strategy is not selected', () => {
    const onSubmit = vi.fn();
    render(<LabEvolveForm strategyName={null} onSubmit={onSubmit} />);

    expect(screen.getByRole('button', { name: 'Start Evolution' })).toBeDisabled();
  });
});
