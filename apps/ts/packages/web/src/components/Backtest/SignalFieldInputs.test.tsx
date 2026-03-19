import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { buildDefaultSignalParams, SignalFieldInputs } from './SignalFieldInputs';

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
  SelectValue: () => null,
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

const fields = [
  {
    name: 'enabled',
    label: 'Enabled',
    type: 'boolean' as const,
    description: 'Toggle signal',
    default: true,
  },
  {
    name: 'ratio_threshold',
    label: 'Ratio Threshold',
    type: 'number' as const,
    description: 'Numeric threshold',
    default: 1.5,
    placeholder: '1.5',
  },
  {
    name: 'condition',
    label: 'Condition',
    type: 'select' as const,
    description: 'Comparison type',
    default: 'above',
    options: ['above', 'below'],
  },
  {
    name: 'benchmark',
    label: 'Benchmark',
    type: 'string' as const,
    description: 'Benchmark code',
    default: 'topix',
    options: ['topix', 'N225_UNDERPX'],
    placeholder: 'topix',
  },
  {
    name: 'tag',
    label: 'Tag',
    type: 'string' as const,
    description: 'Optional tag',
    default: 'momentum',
    placeholder: 'momentum',
  },
];

describe('SignalFieldInputs', () => {
  it('builds default params and respects excluded fields', () => {
    expect(
      buildDefaultSignalParams({
        fields,
      } as never)
    ).toEqual({
      enabled: true,
      ratio_threshold: 1.5,
      condition: 'above',
      benchmark: 'topix',
      tag: 'momentum',
    });

    expect(
      buildDefaultSignalParams(
        {
          fields,
        } as never,
        { excludeFields: ['enabled', 'tag'] }
      )
    ).toEqual({
      ratio_threshold: 1.5,
      condition: 'above',
      benchmark: 'topix',
    });
  });

  it('renders empty state when all fields are excluded', () => {
    render(
      <SignalFieldInputs
        fields={fields}
        values={{}}
        excludeFields={fields.map((field) => field.name)}
        onFieldChange={vi.fn()}
      />
    );

    expect(screen.getByText('No configurable parameters.')).toBeInTheDocument();
  });

  it('handles boolean, number, select, datalist, and text inputs', async () => {
    const user = userEvent.setup();
    const onFieldChange = vi.fn();

    render(<SignalFieldInputs fields={fields} values={{}} onFieldChange={onFieldChange} />);

    await user.click(screen.getByLabelText('Enabled'));
    expect(onFieldChange).toHaveBeenCalledWith(fields[0], false);

    fireEvent.change(screen.getByLabelText('Ratio Threshold'), { target: { value: '' } });
    fireEvent.change(screen.getByLabelText('Ratio Threshold'), { target: { value: '2.5' } });
    expect(onFieldChange).toHaveBeenCalledWith(fields[1], null);
    expect(onFieldChange).toHaveBeenCalledWith(fields[1], 2.5);

    fireEvent.change(screen.getByLabelText('mock-select'), { target: { value: 'below' } });
    expect(onFieldChange).toHaveBeenCalledWith(fields[2], 'below');

    fireEvent.change(screen.getByLabelText('Benchmark'), { target: { value: 'N225_UNDERPX' } });
    expect(onFieldChange).toHaveBeenCalledWith(fields[3], 'N225_UNDERPX');

    fireEvent.change(screen.getByLabelText('Tag'), { target: { value: 'breakout' } });
    expect(onFieldChange).toHaveBeenCalledWith(fields[4], 'breakout');
  });

  it('supports compact mode and hidden descriptions', () => {
    render(
      <SignalFieldInputs
        fields={fields}
        values={{ tag: 'alpha' }}
        compact
        showDescriptions={false}
        columns={1}
        onFieldChange={vi.fn()}
      />
    );

    expect(screen.getByDisplayValue('alpha')).toBeInTheDocument();
    expect(screen.queryByText('Numeric threshold')).not.toBeInTheDocument();
  });
});
