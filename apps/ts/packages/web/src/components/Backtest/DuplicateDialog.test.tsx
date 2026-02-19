import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DuplicateDialog } from './DuplicateDialog';

const mockMutate = vi.fn();
const mockDuplicateState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useDuplicateStrategy: () => ({
    mutate: mockMutate,
    isPending: mockDuplicateState.isPending,
    isError: mockDuplicateState.isError,
    error: mockDuplicateState.error,
  }),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock('@/components/ui/button', () => ({
  Button: ({
    children,
    onClick,
    disabled,
  }: {
    children: ReactNode;
    onClick?: () => void;
    disabled?: boolean;
  }) => (
    <button type="button" onClick={onClick} disabled={disabled}>
      {children}
    </button>
  ),
}));

vi.mock('@/components/ui/label', () => ({
  Label: ({ children, htmlFor }: { children: ReactNode; htmlFor?: string }) => (
    <label htmlFor={htmlFor}>{children}</label>
  ),
}));

vi.mock('@/components/ui/input', () => ({
  Input: ({
    id,
    value,
    onChange,
    placeholder,
  }: {
    id?: string;
    value?: string;
    onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
    placeholder?: string;
  }) => <input id={id} value={value} onChange={onChange} placeholder={placeholder} />,
}));

describe('DuplicateDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDuplicateState.isPending = false;
    mockDuplicateState.isError = false;
    mockDuplicateState.error = null;
  });

  it('duplicates strategy with trimmed name and invokes success callbacks', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSuccess = vi.fn();

    mockMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.({ new_strategy_name: 'experimental/new_strategy' });
    });

    render(
      <DuplicateDialog
        open={true}
        onOpenChange={onOpenChange}
        strategyName="experimental/original"
        onSuccess={onSuccess}
      />
    );

    const duplicateButton = screen.getByRole('button', { name: 'Duplicate' });
    expect(duplicateButton).toBeDisabled();

    await user.type(screen.getByLabelText('New Strategy Name'), '  new_strategy  ');
    expect(duplicateButton).toBeEnabled();
    await user.click(duplicateButton);

    expect(mockMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/original',
        request: { new_name: 'new_strategy' },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
    expect(onSuccess).toHaveBeenCalledWith('experimental/new_strategy');
  });

  it('resets input when canceled', async () => {
    const user = userEvent.setup();

    render(<DuplicateDialog open={true} onOpenChange={vi.fn()} strategyName="experimental/original" />);

    const input = screen.getByLabelText('New Strategy Name') as HTMLInputElement;
    await user.type(input, 'draft_name');
    expect(input.value).toBe('draft_name');

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(input.value).toBe('');
  });

  it('shows pending and error states', () => {
    mockDuplicateState.isPending = true;
    mockDuplicateState.isError = true;
    mockDuplicateState.error = new Error('duplicate failed');

    render(<DuplicateDialog open={true} onOpenChange={vi.fn()} strategyName="experimental/original" />);

    expect(screen.getByRole('button', { name: 'Duplicating...' })).toBeDisabled();
    expect(screen.getByText('Error: duplicate failed')).toBeInTheDocument();
  });
});
