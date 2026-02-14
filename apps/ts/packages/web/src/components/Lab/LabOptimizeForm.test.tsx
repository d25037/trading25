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
    });
  });

  it('submits fundamental-only constraints', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabOptimizeForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));
    const comboboxes = screen.getAllByRole('combobox');
    const categoryCombobox = comboboxes.at(1);
    expect(categoryCombobox).toBeDefined();
    if (!categoryCombobox) {
      return;
    }
    await user.click(categoryCombobox);
    await user.click(screen.getByText('Fundamental Only'));
    await user.click(screen.getByRole('button', { name: 'Start Optimization' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      trials: 50,
      sampler: 'tpe',
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('disables submit when strategy is not selected', () => {
    const onSubmit = vi.fn();
    render(<LabOptimizeForm strategyName={null} onSubmit={onSubmit} />);

    expect(screen.getByRole('button', { name: 'Start Optimization' })).toBeDisabled();
  });
});
