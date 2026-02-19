import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RenameDialog } from './RenameDialog';

const mockMutate = vi.fn();
const mockRenameState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useRenameStrategy: () => ({
    mutate: mockMutate,
    isPending: mockRenameState.isPending,
    isError: mockRenameState.isError,
    error: mockRenameState.error,
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

describe('RenameDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockRenameState.isPending = false;
    mockRenameState.isError = false;
    mockRenameState.error = null;
  });

  it('renames strategy with trimmed name and invokes success callbacks', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSuccess = vi.fn();

    mockMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.({ new_name: 'renamed_strategy' });
    });

    render(
      <RenameDialog
        open={true}
        onOpenChange={onOpenChange}
        strategyName="experimental/original_name"
        onSuccess={onSuccess}
      />
    );

    const renameButton = screen.getByRole('button', { name: 'Rename' });
    expect(renameButton).toBeDisabled();

    await user.type(screen.getByLabelText('New Strategy Name'), '  renamed_strategy  ');
    expect(renameButton).toBeEnabled();
    await user.click(renameButton);

    expect(mockMutate).toHaveBeenCalledWith(
      {
        name: 'experimental/original_name',
        request: { new_name: 'renamed_strategy' },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
    expect(onSuccess).toHaveBeenCalledWith('renamed_strategy');
  });

  it('keeps rename disabled when input equals current strategy name', async () => {
    const user = userEvent.setup();
    render(<RenameDialog open={true} onOpenChange={vi.fn()} strategyName="experimental/original_name" />);

    const renameButton = screen.getByRole('button', { name: 'Rename' });
    await user.type(screen.getByLabelText('New Strategy Name'), 'original_name');

    expect(renameButton).toBeDisabled();
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('resets input when canceled and shows pending/error states', async () => {
    const user = userEvent.setup();
    mockRenameState.isPending = true;
    mockRenameState.isError = true;
    mockRenameState.error = new Error('rename failed');

    render(<RenameDialog open={true} onOpenChange={vi.fn()} strategyName="experimental/original_name" />);

    const input = screen.getByLabelText('New Strategy Name') as HTMLInputElement;
    await user.type(input, 'tmp_name');
    expect(input.value).toBe('tmp_name');

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(input.value).toBe('');
    expect(screen.getByRole('button', { name: 'Renaming...' })).toBeDisabled();
    expect(screen.getByText('Error: rename failed')).toBeInTheDocument();
  });
});
