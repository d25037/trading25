import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { NumberSelect } from './NumberSelect';

describe('NumberSelect', () => {
  it('calls onChange with selected numeric value', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <NumberSelect
        value={2}
        onChange={onChange}
        options={[
          { value: 1, label: 'One' },
          { value: 2, label: 'Two' },
        ]}
        id="number-select"
        label="Number"
      />
    );

    await user.click(screen.getByRole('combobox', { name: 'Number' }));
    await user.click(screen.getByText('One'));

    expect(onChange).toHaveBeenCalledWith(1);
  });

  it('supports disabled and description props', () => {
    render(
      <NumberSelect
        value={2}
        onChange={vi.fn()}
        options={[
          { value: 1, label: 'One' },
          { value: 2, label: 'Two' },
        ]}
        id="number-select-disabled"
        label="Disabled Number"
        disabled
        description="Selection is disabled"
      />
    );

    expect(screen.getByRole('combobox', { name: 'Disabled Number' })).toBeDisabled();
    expect(screen.getByText('Selection is disabled')).toBeInTheDocument();
  });
});
