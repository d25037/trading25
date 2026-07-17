import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { LabGenerateForm } from './LabGenerateForm';

describe('LabGenerateForm', () => {
  it('submits default payload', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabGenerateForm onSubmit={onSubmit} />);
    await user.click(screen.getByRole('button', { name: 'Generate Strategies' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      count: 10,
      top: 5,
      direction: 'longonly',
      timeframe: 'daily',
      entry_filter_only: false,
      save: true,
      engine_policy: {
        mode: 'fast_only',
      },
    });
  });

  it('falls back to defaults when count/top are invalid', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabGenerateForm onSubmit={onSubmit} />);

    await user.clear(screen.getByLabelText('Count'));
    await user.clear(screen.getByLabelText('Top'));
    await user.click(screen.getByRole('button', { name: 'Generate Strategies' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      count: 10,
      top: 5,
      direction: 'longonly',
      timeframe: 'daily',
      entry_filter_only: false,
      save: true,
      engine_policy: {
        mode: 'fast_only',
      },
    });
  });

  it('submits fundamental-only constraints and universe preset', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabGenerateForm onSubmit={onSubmit} />);

    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));

    const comboboxes = screen.getAllByRole('combobox');
    const universeCombobox = comboboxes.at(2);
    const categoryCombobox = comboboxes.at(3);
    expect(universeCombobox).toBeDefined();
    expect(categoryCombobox).toBeDefined();
    if (!universeCombobox || !categoryCombobox) {
      return;
    }
    await user.click(universeCombobox);
    await user.click(screen.getByText('Prime ex TOPIX500'));
    await user.click(categoryCombobox);
    await user.click(screen.getByText('Fundamental Only'));

    await user.click(screen.getByRole('button', { name: 'Generate Strategies' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      count: 10,
      top: 5,
      direction: 'longonly',
      timeframe: 'daily',
      universe_preset: 'primeExTopix500',
      entry_filter_only: true,
      save: true,
      allowed_categories: ['fundamental'],
      engine_policy: {
        mode: 'fast_only',
      },
    });
  });

  it('respects disabled state', () => {
    const onSubmit = vi.fn();
    render(<LabGenerateForm onSubmit={onSubmit} disabled />);

    expect(screen.getByRole('button', { name: 'Generate Strategies' })).toBeDisabled();
  });
});
