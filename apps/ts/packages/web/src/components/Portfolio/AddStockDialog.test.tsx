import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useAddPortfolioItem } from '@/hooks/usePortfolio';
import { AddStockDialog } from './AddStockDialog';

const mockMutate = vi.fn();
const mockValidateStockForm = vi.fn((data: { code?: string; purchasePrice: string }) =>
  Boolean((data.code || '').trim()) && data.purchasePrice.length > 0
);

vi.mock('@/hooks/usePortfolio', () => ({
  useAddPortfolioItem: vi.fn(),
}));

vi.mock('./StockFormFields', () => ({
  StockFormFields: ({
    data,
    onChange,
    onStockSelect,
  }: {
    data: {
      code?: string;
      quantity: string;
      purchasePrice: string;
      purchaseDate: string;
      account: string;
      notes: string;
    };
    onChange: (next: {
      code?: string;
      quantity: string;
      purchasePrice: string;
      purchaseDate: string;
      account: string;
      notes: string;
    }) => void;
    onStockSelect?: (stock: { code: string; companyName: string }) => void;
  }) => (
    <div>
      <button
        type="button"
        onClick={() =>
          onChange({
            ...data,
            code: '7203',
            quantity: '100',
            purchasePrice: '2500',
            purchaseDate: '2026-02-19',
            account: '',
            notes: '',
          })
        }
      >
        Fill7203
      </button>
      <button type="button" onClick={() => onStockSelect?.({ code: '7203', companyName: 'Toyota Motor' })}>
        SelectToyota
      </button>
      <button type="button" onClick={() => onChange({ ...data, code: '6501' })}>
        ChangeCode6501
      </button>
    </div>
  ),
  validateStockForm: (data: { code?: string; purchasePrice: string }) => mockValidateStockForm(data),
}));

describe('AddStockDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useAddPortfolioItem).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
      error: null,
    } as unknown as ReturnType<typeof useAddPortfolioItem>);
  });

  it('submits selected company name when search candidate is selected', async () => {
    const user = userEvent.setup();
    render(<AddStockDialog portfolioId={1} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    const dialog = screen.getByRole('dialog');
    await user.click(screen.getByRole('button', { name: 'Fill7203' }));
    await user.click(screen.getByRole('button', { name: 'SelectToyota' }));
    await user.click(within(dialog).getByRole('button', { name: 'Add Stock' }));

    expect(mockMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        portfolioId: 1,
        data: expect.objectContaining({
          code: '7203',
          companyName: 'Toyota Motor',
          quantity: 100,
          purchasePrice: 2500,
        }),
      }),
      expect.any(Object)
    );
  });

  it('falls back to code as companyName when code diverges from selected stock', async () => {
    const user = userEvent.setup();
    render(<AddStockDialog portfolioId={1} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    const dialog = screen.getByRole('dialog');
    await user.click(screen.getByRole('button', { name: 'Fill7203' }));
    await user.click(screen.getByRole('button', { name: 'SelectToyota' }));
    await user.click(screen.getByRole('button', { name: 'ChangeCode6501' }));
    await user.click(within(dialog).getByRole('button', { name: 'Add Stock' }));

    expect(mockMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          code: '6501',
          companyName: '6501',
        }),
      }),
      expect.any(Object)
    );
  });

  it('resets selected stock when dialog is closed', async () => {
    const user = userEvent.setup();
    render(<AddStockDialog portfolioId={1} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    await user.click(screen.getByRole('button', { name: 'Fill7203' }));
    await user.click(screen.getByRole('button', { name: 'SelectToyota' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    const dialog = screen.getByRole('dialog');
    await user.click(screen.getByRole('button', { name: 'Fill7203' }));
    await user.click(within(dialog).getByRole('button', { name: 'Add Stock' }));

    expect(mockMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          code: '7203',
          companyName: '7203',
        }),
      }),
      expect.any(Object)
    );

    const callback = mockMutate.mock.calls[0]?.[1];
    if (callback?.onSuccess) {
      await act(async () => {
        callback.onSuccess();
      });
    }
  });

  it('does not submit when form is invalid', async () => {
    const user = userEvent.setup();
    render(<AddStockDialog portfolioId={1} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    const dialog = screen.getByRole('dialog');
    const submitButton = within(dialog).getByRole('button', { name: 'Add Stock' });

    expect(submitButton).toBeDisabled();
    await user.click(submitButton);

    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('renders mutation error and pending state', async () => {
    vi.mocked(useAddPortfolioItem).mockReturnValue({
      mutate: mockMutate,
      isPending: true,
      error: new Error('Failed to add'),
    } as unknown as ReturnType<typeof useAddPortfolioItem>);

    const user = userEvent.setup();
    render(<AddStockDialog portfolioId={1} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    const dialog = screen.getByRole('dialog');

    expect(within(dialog).getByText('Failed to add')).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: 'Add Stock' })).toBeDisabled();
    expect(document.querySelector('.animate-spin')).toBeTruthy();
  });
});
