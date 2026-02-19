import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DatasetDeleteDialog } from './DatasetDeleteDialog';

const mockMutate = vi.fn();
const mockDeleteState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useDataset', () => ({
  useDeleteDataset: () => ({
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

describe('DatasetDeleteDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDeleteState.isPending = false;
    mockDeleteState.isError = false;
    mockDeleteState.error = null;
  });

  it('deletes dataset and closes dialog on success', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockMutate.mockImplementation((_name, options) => {
      options?.onSuccess?.();
    });

    render(<DatasetDeleteDialog open={true} onOpenChange={onOpenChange} datasetName="quickTesting.db" />);

    expect(screen.getByText('quickTesting.db')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: '削除' }));

    expect(mockMutate).toHaveBeenCalledWith('quickTesting.db', expect.any(Object));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('closes dialog when cancel is clicked', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<DatasetDeleteDialog open={true} onOpenChange={onOpenChange} datasetName="quickTesting.db" />);

    await user.click(screen.getByRole('button', { name: 'キャンセル' }));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('shows pending and error states', () => {
    mockDeleteState.isPending = true;
    mockDeleteState.isError = true;
    mockDeleteState.error = new Error('delete failed');

    render(<DatasetDeleteDialog open={true} onOpenChange={vi.fn()} datasetName="quickTesting.db" />);

    expect(screen.getByRole('button', { name: '削除中...' })).toBeDisabled();
    expect(screen.getByText('Error: delete failed')).toBeInTheDocument();
  });
});
