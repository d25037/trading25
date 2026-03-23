import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { AuthoringFieldSchema } from '@/types/backtest';
import { ReferenceSelectFieldCard } from './ReferenceSelectFieldCard';

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

describe('ReferenceSelectFieldCard', () => {
  it('renders inherited state with placeholder fallback when the current value is empty', () => {
    const onChange = vi.fn();

    render(
      <ReferenceSelectFieldCard
        field={{ ...baseField, summary: '', description: '', placeholder: '' }}
        value={null}
        effectiveValue={undefined}
        overridden={false}
        optionValues={['prime_20260316', 'default-dataset']}
        chooserLabel="Choose available dataset"
        placeholderLabel="Select a dataset"
        onChange={onChange}
        onReset={vi.fn()}
      />
    );

    expect(screen.getByText('Inherited')).toBeInTheDocument();
    expect(screen.getByText('Effective: unset')).toBeInTheDocument();
    expect(screen.queryByText('Dataset snapshot')).not.toBeInTheDocument();
    expect(screen.queryByText('Dataset name')).not.toBeInTheDocument();

    const select = screen.getByLabelText('Dataset');
    expect(select).toHaveValue('');
    expect((select as HTMLSelectElement).options[0]?.value).toBe('');
    expect(screen.getByText('2 option(s) available.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reset' })).toBeDisabled();

    fireEvent.change(select, { target: { value: 'prime_20260316' } });
    expect(onChange).toHaveBeenCalledWith('prime_20260316');
  });

  it('renders overridden state and preserves a current value missing from the live options', async () => {
    const user = userEvent.setup();
    const onReset = vi.fn();
    const onChange = vi.fn();

    render(
      <ReferenceSelectFieldCard
        field={{
          ...baseField,
          path: 'benchmark_table',
          label: 'Benchmark',
          summary: 'Benchmark used for relative return comparisons.',
          description: 'Values come from the available indices list.',
          placeholder: 'Select benchmark',
        }}
        value="legacy_benchmark"
        effectiveValue="legacy_benchmark"
        overridden={true}
        optionValues={['topix', 'N225_UNDERPX']}
        chooserLabel="Choose available benchmark"
        placeholderLabel="Select a benchmark"
        onChange={onChange}
        onReset={onReset}
      />
    );

    expect(screen.getByText('Overridden')).toBeInTheDocument();
    expect(screen.getByText('Benchmark used for relative return comparisons.')).toBeInTheDocument();
    expect(screen.getByText('Values come from the available indices list.')).toBeInTheDocument();
    expect(screen.queryByText(/Effective:/)).not.toBeInTheDocument();
    expect(screen.queryByRole('option', { name: 'Select a benchmark' })).not.toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'legacy_benchmark' })).toHaveValue('legacy_benchmark');
    expect(screen.getByText('3 option(s) available.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Benchmark'), { target: { value: 'N225_UNDERPX' } });
    expect(onChange).toHaveBeenCalledWith('N225_UNDERPX');

    await user.click(screen.getByRole('button', { name: 'Reset' }));
    expect(onReset).toHaveBeenCalledTimes(1);
  });
});
