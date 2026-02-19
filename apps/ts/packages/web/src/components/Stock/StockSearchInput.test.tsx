import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { StockSearchInput } from './StockSearchInput';

const mockUseStockSearch = vi.fn();

vi.mock('@/hooks/useStockSearch', () => ({
  useStockSearch: (...args: unknown[]) => mockUseStockSearch(...args),
}));

const SEARCH_RESULT = {
  code: '7203',
  companyName: 'Toyota Motor',
  companyNameEnglish: null,
  marketCode: '0111',
  marketName: 'Prime',
  sector33Name: '輸送用機器',
};

function ControlledSearch({ onSelect }: { onSelect: (code: string) => void }) {
  const [value, setValue] = useState('');
  return (
    <StockSearchInput
      id="stock-search"
      name="stock-search"
      value={value}
      onValueChange={setValue}
      onSelect={(stock) => onSelect(stock.code)}
    />
  );
}

describe('StockSearchInput', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseStockSearch.mockImplementation((query: string) => ({
      data: query ? { results: [SEARCH_RESULT] } : { results: [] },
      isLoading: false,
    }));
  });

  it('shows suggestion list and selects by click', async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    render(<ControlledSearch onSelect={onSelect} />);

    await user.type(screen.getByRole('searchbox'), 'toyo');
    await user.click(await screen.findByRole('button', { name: /7203 Toyota Motor/i }));

    expect(onSelect).toHaveBeenCalledWith('7203');
  });

  it('supports keyboard selection from suggestions', async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    render(<ControlledSearch onSelect={onSelect} />);

    const input = screen.getByRole('searchbox');
    await user.type(input, 'toyo');
    await screen.findByText('Toyota Motor');

    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

    expect(onSelect).toHaveBeenCalledWith('7203');
  });
});
