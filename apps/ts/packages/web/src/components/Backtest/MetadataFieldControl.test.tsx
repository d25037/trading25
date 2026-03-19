import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { AuthoringFieldSchema } from '@/types/backtest';
import { MetadataFieldControl } from './MetadataFieldControl';

vi.mock('@/components/ui/select', () => ({
  Select: ({
    children,
    value,
    onValueChange,
    disabled,
  }: {
    children: ReactNode;
    value: string;
    onValueChange: (value: string) => void;
    disabled?: boolean;
  }) => (
    <select aria-label="mock-select" value={value} onChange={(event) => onValueChange(event.target.value)} disabled={disabled}>
      {children}
    </select>
  ),
  SelectTrigger: ({ children }: { children: ReactNode }) => <>{children}</>,
  SelectValue: ({ placeholder }: { placeholder?: string }) => <option value="">{placeholder ?? 'Select value'}</option>,
  SelectContent: ({ children }: { children: ReactNode }) => <>{children}</>,
  SelectItem: ({ children, value }: { children: ReactNode; value: string }) => <option value={value}>{children}</option>,
}));

vi.mock('@/components/ui/switch', () => ({
  Switch: ({
    id,
    checked,
    disabled,
    onCheckedChange,
  }: {
    id?: string;
    checked: boolean;
    disabled?: boolean;
    onCheckedChange: (value: boolean) => void;
  }) => (
    <input
      id={id}
      type="checkbox"
      checked={checked}
      disabled={disabled}
      onChange={(event) => onCheckedChange(event.target.checked)}
    />
  ),
}));

const baseField: AuthoringFieldSchema = {
  path: 'dataset',
  section: 'shared_config',
  group: 'data',
  label: 'Dataset',
  type: 'string',
  widget: 'text',
  description: 'Dataset name',
  summary: 'Dataset snapshot',
  default: 'default-dataset',
  options: undefined,
  constraints: undefined,
  placeholder: 'prime_20260316',
  unit: undefined,
  examples: [],
  required: false,
  advanced_only: false,
};

describe('MetadataFieldControl', () => {
  it('renders inherited metadata and reset state', () => {
    render(
      <MetadataFieldControl
        field={{ ...baseField, examples: ['prime_20260316'] }}
        value="default-dataset"
        effectiveValue="default-dataset"
        overridden={false}
        onChange={vi.fn()}
        onReset={vi.fn()}
      />
    );

    expect(screen.getByText('Inherited')).toBeInTheDocument();
    expect(screen.getByText('Effective: default-dataset')).toBeInTheDocument();
    expect(screen.getByText(/Example:/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reset' })).toBeDisabled();
  });

  it('formats unset, array, and object effective values', () => {
    const { rerender } = render(
      <MetadataFieldControl field={baseField} value="" effectiveValue={null} overridden={false} onChange={vi.fn()} />
    );

    expect(screen.getByText('Effective: unset')).toBeInTheDocument();

    rerender(
      <MetadataFieldControl
        field={baseField}
        value={['7203', '6758']}
        effectiveValue={['7203', '6758']}
        overridden={false}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByText('Effective: 7203, 6758')).toBeInTheDocument();

    rerender(
      <MetadataFieldControl
        field={baseField}
        value={{ dataset: 'demo' }}
        effectiveValue={{ dataset: 'demo' }}
        overridden={false}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByText('Effective: {"dataset":"demo"}')).toBeInTheDocument();
  });

  it('handles switch and number widgets', async () => {
    const user = userEvent.setup();
    const onSwitchChange = vi.fn();
    const onNumberChange = vi.fn();

    const { rerender } = render(
      <MetadataFieldControl
        field={{ ...baseField, path: 'include_margin_data', label: 'Include Margin Data', type: 'boolean', widget: 'switch' }}
        value={false}
        overridden={true}
        onChange={onSwitchChange}
      />
    );

    await user.click(screen.getByLabelText('Include Margin Data'));
    expect(onSwitchChange).toHaveBeenCalledWith(true);

    rerender(
      <MetadataFieldControl
        field={{ ...baseField, path: 'fees', label: 'Fees', type: 'number', widget: 'number' }}
        value={0.1}
        overridden={true}
        onChange={onNumberChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Fees'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Fees'), { target: { value: '1.5' } });
    expect(onNumberChange).toHaveBeenNthCalledWith(1, null);
    expect(onNumberChange).toHaveBeenNthCalledWith(2, 1.5);
  });

  it('handles textarea, select, combobox, string list, and text widgets', async () => {
    const onChange = vi.fn();

    const { rerender } = render(
      <MetadataFieldControl
        field={{ ...baseField, widget: 'textarea', label: 'Description' }}
        value="before"
        overridden={true}
        onChange={onChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Description'), { target: { value: 'after' } });
    expect(onChange).toHaveBeenLastCalledWith('after');

    rerender(
      <MetadataFieldControl
        field={{ ...baseField, widget: 'select', type: 'select', label: 'Execution Policy', options: ['standard', 'next_session_round_trip'] }}
        value="standard"
        overridden={true}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText('mock-select'), { target: { value: 'next_session_round_trip' } });
    expect(onChange).toHaveBeenLastCalledWith('next_session_round_trip');

    rerender(
      <MetadataFieldControl
        field={{ ...baseField, widget: 'combobox', label: 'Benchmark', options: ['topix'] }}
        value="topix"
        overridden={true}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText('Benchmark'), { target: { value: 'N225_UNDERPX' } });
    expect(onChange).toHaveBeenLastCalledWith('N225_UNDERPX');

    rerender(
      <MetadataFieldControl
        field={{ ...baseField, path: 'stock_codes', widget: 'string_list', type: 'string_list', label: 'Stock Codes' }}
        value={['7203']}
        overridden={true}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText('Stock Codes'), { target: { value: '7203\n6758,9984' } });
    expect(onChange).toHaveBeenLastCalledWith(['7203', '6758', '9984']);

    rerender(<MetadataFieldControl field={baseField} value="demo" overridden={true} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText('Dataset'), { target: { value: 'custom' } });
    expect(onChange).toHaveBeenLastCalledWith('custom');
  });
});
