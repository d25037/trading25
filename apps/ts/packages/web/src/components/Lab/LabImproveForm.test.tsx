import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { LabImproveForm } from './LabImproveForm';

describe('LabImproveForm', () => {
  it('submits default payload', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabImproveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);
    await user.click(screen.getByRole('button', { name: 'Analyze & Improve' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      auto_apply: false,
    });
  });

  it('submits fundamental-only constraints', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabImproveForm strategyName="experimental/base_strategy_01" onSubmit={onSubmit} />);

    await user.click(screen.getByRole('switch', { name: 'Auto Apply' }));
    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));

    await user.click(screen.getByRole('combobox'));
    await user.click(screen.getByText('Fundamental Only'));
    await user.click(screen.getByRole('button', { name: 'Analyze & Improve' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      strategy_name: 'experimental/base_strategy_01',
      auto_apply: true,
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('disables submit when strategy is not selected', () => {
    const onSubmit = vi.fn();
    render(<LabImproveForm strategyName={null} onSubmit={onSubmit} />);

    expect(screen.getByRole('button', { name: 'Analyze & Improve' })).toBeDisabled();
  });
});
