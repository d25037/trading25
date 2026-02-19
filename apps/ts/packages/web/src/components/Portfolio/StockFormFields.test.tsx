import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { type StockFormData, StockFormFields, validateStockForm } from './StockFormFields';

vi.mock('@/components/Stock/StockSearchInput', () => ({
  StockSearchInput: ({
    id,
    value,
    onValueChange,
    onSelect,
  }: {
    id?: string;
    value: string;
    onValueChange: (value: string) => void;
    onSelect: (stock: { code: string }) => void;
  }) => (
    <div>
      <input
        id={id}
        aria-label="StockSearchInput"
        value={value}
        onChange={(e) => onValueChange(e.target.value)}
      />
      <button type="button" onClick={() => onSelect({ code: '6501' })}>
        SelectStock
      </button>
    </div>
  ),
}));

function createFormData(overrides: Partial<StockFormData> = {}): StockFormData {
  return {
    code: '7203',
    quantity: '100',
    purchasePrice: '2500',
    purchaseDate: '2026-02-19',
    account: '',
    notes: '',
    ...overrides,
  };
}

function StockFormFieldsHarness({
  initialData,
  onChangeSpy,
  showCodeField = true,
  idPrefix = 'stock',
}: {
  initialData: StockFormData;
  onChangeSpy: (data: StockFormData) => void;
  showCodeField?: boolean;
  idPrefix?: string;
}) {
  const [data, setData] = useState(initialData);
  return (
    <StockFormFields
      data={data}
      onChange={(next) => {
        onChangeSpy(next);
        setData(next);
      }}
      showCodeField={showCodeField}
      idPrefix={idPrefix}
    />
  );
}

describe('StockFormFields', () => {
  it('updates code from search input and selected candidate', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    const data = createFormData({ code: '' });

    render(<StockFormFieldsHarness initialData={data} onChangeSpy={onChange} showCodeField={true} idPrefix="add" />);

    await user.type(screen.getByLabelText('Stock Code'), '72');
    expect(onChange).toHaveBeenLastCalledWith({ ...data, code: '72' });

    await user.click(screen.getByRole('button', { name: 'SelectStock' }));
    expect(onChange).toHaveBeenLastCalledWith({ ...data, code: '6501' });
  });

  it('renders edit mode without stock code field', () => {
    const onChange = vi.fn();
    const data = createFormData();

    render(<StockFormFieldsHarness initialData={data} onChangeSpy={onChange} showCodeField={false} idPrefix="edit" />);

    expect(screen.queryByLabelText('Stock Code')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Purchase Price')).toBeInTheDocument();
    expect(screen.getByLabelText('Purchase Date')).toBeInTheDocument();
  });
});

describe('validateStockForm', () => {
  it('returns true for valid data when code is required', () => {
    expect(validateStockForm(createFormData(), true)).toBe(true);
  });

  it('returns false for invalid code when code is required', () => {
    expect(validateStockForm(createFormData({ code: 'abcd' }), true)).toBe(false);
  });

  it('ignores code when requireCode is false', () => {
    expect(validateStockForm(createFormData({ code: 'company-name' }), false)).toBe(true);
  });

  it('returns false for invalid numeric fields or date', () => {
    expect(validateStockForm(createFormData({ quantity: '99' }), true)).toBe(false);
    expect(validateStockForm(createFormData({ purchasePrice: '0' }), true)).toBe(false);
    expect(validateStockForm(createFormData({ purchaseDate: '' }), true)).toBe(false);
  });
});
