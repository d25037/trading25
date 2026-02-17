import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import * as React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MoveGroupDialog } from './MoveGroupDialog';

const mockMoveMutate = vi.fn();

const mockMoveState = {
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useMoveStrategy: () => ({
    mutate: mockMoveMutate,
    isPending: mockMoveState.isPending,
    isError: mockMoveState.isError,
    error: mockMoveState.error,
  }),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({
    open,
    children,
  }: {
    open: boolean;
    children: ReactNode;
  }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
}));

vi.mock('@/components/ui/label', () => ({
  Label: ({ children, htmlFor }: { children: ReactNode; htmlFor?: string }) => (
    <label htmlFor={htmlFor}>{children}</label>
  ),
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

vi.mock('@/components/ui/select', () => {
  const SelectContext = React.createContext<(value: string) => void>(() => {});

  return {
    Select: ({
      children,
      onValueChange,
    }: {
      children: ReactNode;
      onValueChange?: (value: string) => void;
    }) => <SelectContext.Provider value={onValueChange ?? (() => {})}>{children}</SelectContext.Provider>,
    SelectTrigger: ({ children, id }: { children: ReactNode; id?: string }) => (
      <div data-testid={id ?? 'select-trigger'}>{children}</div>
    ),
    SelectValue: ({ placeholder }: { placeholder?: string }) => <span>{placeholder}</span>,
    SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    SelectItem: ({ children, value }: { children: ReactNode; value: string }) => {
      const setValue = React.useContext(SelectContext);
      return (
        <button type="button" onClick={() => setValue(value)}>
          {children}
        </button>
      );
    },
  };
});

describe('MoveGroupDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockMoveState.isPending = false;
    mockMoveState.isError = false;
    mockMoveState.error = null;
  });

  it('shows target groups excluding current group', () => {
    render(
      <MoveGroupDialog
        open
        onOpenChange={vi.fn()}
        strategyName="production/range_break"
        currentCategory="production"
      />
    );

    expect(screen.getByText('Move Strategy Group')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Experimental' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Legacy' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Production' })).not.toBeInTheDocument();
  });

  it('submits move with default target and calls success callbacks', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    const onSuccess = vi.fn();

    mockMoveMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.({ new_strategy_name: 'experimental/range_break_moved' });
    });

    render(
      <MoveGroupDialog
        open
        onOpenChange={onOpenChange}
        strategyName="production/range_break"
        currentCategory="production"
        onSuccess={onSuccess}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Move' }));

    expect(mockMoveMutate).toHaveBeenCalledWith(
      {
        name: 'production/range_break',
        request: { target_category: 'experimental' },
      },
      expect.any(Object)
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
    expect(onSuccess).toHaveBeenCalledWith('experimental/range_break_moved');
  });

  it('uses selected target group when changed', async () => {
    const user = userEvent.setup();

    render(
      <MoveGroupDialog
        open
        onOpenChange={vi.fn()}
        strategyName="production/range_break"
        currentCategory="production"
      />
    );

    await user.click(screen.getByRole('button', { name: 'Legacy' }));
    await user.click(screen.getByRole('button', { name: 'Move' }));

    expect(mockMoveMutate).toHaveBeenCalledWith(
      {
        name: 'production/range_break',
        request: { target_category: 'legacy' },
      },
      expect.any(Object)
    );
  });

  it('shows error message when move fails', () => {
    mockMoveState.isError = true;
    mockMoveState.error = new Error('move failed');

    render(
      <MoveGroupDialog
        open
        onOpenChange={vi.fn()}
        strategyName="production/range_break"
        currentCategory="production"
      />
    );

    expect(screen.getByText('Error: move failed')).toBeInTheDocument();
  });

  it('shows pending state', () => {
    mockMoveState.isPending = true;

    render(
      <MoveGroupDialog
        open
        onOpenChange={vi.fn()}
        strategyName="production/range_break"
        currentCategory="production"
      />
    );

    expect(screen.getByRole('button', { name: 'Moving...' })).toBeDisabled();
  });
});
