import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { IndicatorToggle, NumberInput } from './IndicatorToggle';

describe('IndicatorToggle', () => {
  it('renders label and toggles switch state', async () => {
    const user = userEvent.setup();
    const onToggle = vi.fn();

    render(<IndicatorToggle label="Risk Adjusted Return" enabled={false} onToggle={onToggle} />);

    const toggle = screen.getByRole('switch', { name: /risk adjusted return/i });
    await user.click(toggle);

    expect(onToggle).toHaveBeenCalledWith(true);
  });

  it('renders meta text when provided', () => {
    render(
      <IndicatorToggle
        label="Volume Comparison"
        enabled={false}
        onToggle={vi.fn()}
        meta="Signal req: volume | Signals: volume"
      />
    );

    expect(screen.getByText('Signal req: volume | Signals: volume')).toBeInTheDocument();
  });

  it('renders children only when enabled', () => {
    const Child = <div>settings</div>;

    const { rerender } = render(
      <IndicatorToggle label="Trading Value MA" enabled={false} onToggle={vi.fn()}>
        {Child}
      </IndicatorToggle>
    );

    expect(screen.queryByText('settings')).not.toBeInTheDocument();

    rerender(
      <IndicatorToggle label="Trading Value MA" enabled onToggle={vi.fn()}>
        {Child}
      </IndicatorToggle>
    );

    expect(screen.getByText('settings')).toBeInTheDocument();
  });
});

describe('NumberInput', () => {
  it('parses integer values when step is omitted', () => {
    const onChange = vi.fn();

    render(<NumberInput label="Period" value={20} onChange={onChange} defaultValue={0} />);

    const input = screen.getByRole('spinbutton', { name: /period/i });
    fireEvent.change(input, { target: { value: '30' } });

    expect(onChange).toHaveBeenCalledWith(30);
  });

  it('parses float values when step is provided', () => {
    const onChange = vi.fn();

    render(<NumberInput label="Multiplier" value={3.0} onChange={onChange} step="0.1" defaultValue={1.0} />);

    const input = screen.getByRole('spinbutton', { name: /multiplier/i });
    fireEvent.change(input, { target: { value: '2.5' } });

    expect(onChange).toHaveBeenCalledWith(2.5);
  });

  it('falls back to defaultValue when parsing fails', () => {
    const onChange = vi.fn();

    render(<NumberInput label="Lookback" value={60} onChange={onChange} defaultValue={42} />);

    const input = screen.getByRole('spinbutton', { name: /lookback/i });
    fireEvent.change(input, { target: { value: 'not-number' } });

    expect(onChange).toHaveBeenCalledWith(42);
  });
});
