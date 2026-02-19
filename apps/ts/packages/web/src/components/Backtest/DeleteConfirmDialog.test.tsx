import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DeleteConfirmDialog } from './DeleteConfirmDialog';

const mockMutate = vi.fn();
const mockDeleteState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useDeleteStrategy: () => ({
    mutate: mockMutate,
    isPending: mockDeleteState.isPending,
    isError: mockDeleteState.isError,
    error: mockDeleteState.error,
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

describe('DeleteConfirmDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDeleteState.isPending = false;
    mockDeleteState.isError = false;
    mockDeleteState.error = null;
  });

  it('deletes strategy and invokes success callbacks', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSuccess = vi.fn();

    mockMutate.mockImplementation((_name, options) => {
      options?.onSuccess?.();
    });

    render(
      <DeleteConfirmDialog
        open={true}
        onOpenChange={onOpenChange}
        strategyName="experimental/test_strategy"
        onSuccess={onSuccess}
      />
    );

    expect(screen.getByText('experimental/test_strategy')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Delete' }));

    expect(mockMutate).toHaveBeenCalledWith('experimental/test_strategy', expect.any(Object));
    expect(onOpenChange).toHaveBeenCalledWith(false);
    expect(onSuccess).toHaveBeenCalledTimes(1);
  });

  it('closes dialog when cancel is clicked', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<DeleteConfirmDialog open={true} onOpenChange={onOpenChange} strategyName="experimental/alpha" />);

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('shows pending and error states', () => {
    mockDeleteState.isPending = true;
    mockDeleteState.isError = true;
    mockDeleteState.error = new Error('delete failed');

    render(<DeleteConfirmDialog open={true} onOpenChange={vi.fn()} strategyName="experimental/alpha" />);

    expect(screen.getByRole('button', { name: 'Deleting...' })).toBeDisabled();
    expect(screen.getByText('Error: delete failed')).toBeInTheDocument();
  });
});
