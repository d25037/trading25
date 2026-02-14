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
    });
  });

  it('submits fundamental-only constraints', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabEvolveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));
    await user.click(screen.getByRole('combobox'));
    await user.click(screen.getByText('Fundamental Only'));
    await user.click(screen.getByRole('button', { name: 'Start Evolution' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      generations: 10,
      population: 20,
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('disables submit when strategy is not selected', () => {
    const onSubmit = vi.fn();
    render(<LabEvolveForm strategyName={null} onSubmit={onSubmit} />);

    expect(screen.getByRole('button', { name: 'Start Evolution' })).toBeDisabled();
  });
});
