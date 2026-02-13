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
    });
  });

  it('submits fundamental-only constraints and trimmed dataset', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(<LabGenerateForm onSubmit={onSubmit} />);

    await user.clear(screen.getByLabelText('Dataset (optional)'));
    await user.type(screen.getByLabelText('Dataset (optional)'), '  custom_dataset  ');
    await user.click(screen.getByRole('switch', { name: 'Entry Filter Only' }));

    const comboboxes = screen.getAllByRole('combobox');
    const categoryCombobox = comboboxes.at(2);
    expect(categoryCombobox).toBeDefined();
    if (!categoryCombobox) {
      return;
    }
    await user.click(categoryCombobox);
    await user.click(screen.getByText('Fundamental Only'));

    await user.click(screen.getByRole('button', { name: 'Generate Strategies' }));

    expect(onSubmit).toHaveBeenCalledTimes(1);
    expect(onSubmit).toHaveBeenCalledWith({
      count: 10,
      top: 5,
      direction: 'longonly',
      timeframe: 'daily',
      dataset: 'custom_dataset',
      entry_filter_only: true,
      allowed_categories: ['fundamental'],
    });
  });

  it('respects disabled state', () => {
    const onSubmit = vi.fn();
    render(<LabGenerateForm onSubmit={onSubmit} disabled />);

    expect(screen.getByRole('button', { name: 'Generate Strategies' })).toBeDisabled();
  });
});
