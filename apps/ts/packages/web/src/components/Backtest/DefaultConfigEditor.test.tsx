import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DefaultConfigEditor } from './DefaultConfigEditor';

const mockMutate = vi.fn();
const mockReset = vi.fn();

const mockState = {
  configData: null as { content: string } | null,
  isLoading: false,
  isPending: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useBacktest', () => ({
  useDefaultConfig: () => ({
    data: mockState.configData,
    isLoading: mockState.isLoading,
  }),
  useUpdateDefaultConfig: () => ({
    mutate: mockMutate,
    reset: mockReset,
    isPending: mockState.isPending,
    isError: mockState.isError,
    error: mockState.error,
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

vi.mock('@/components/Editor/MonacoYamlEditor', () => ({
  MonacoYamlEditor: ({
    value,
    onChange,
  }: {
    value: string;
    onChange: (value: string) => void;
  }) => (
    <textarea
      aria-label="yaml-editor"
      value={value}
      onChange={(event: ChangeEvent<HTMLTextAreaElement>) => onChange(event.target.value)}
    />
  ),
}));

describe('DefaultConfigEditor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.configData = { content: 'filters:\n  min_price: 1000' };
    mockState.isLoading = false;
    mockState.isPending = false;
    mockState.isError = false;
    mockState.error = null;
  });

  it('renders loading state', () => {
    mockState.isLoading = true;

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('saves valid yaml and closes dialog on success', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    mockMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.();
    });

    render(<DefaultConfigEditor open={true} onOpenChange={onOpenChange} />);

    const editor = screen.getByLabelText('yaml-editor') as HTMLTextAreaElement;
    expect(editor.value).toContain('min_price: 1000');

    await user.clear(editor);
    await user.type(editor, 'filters:\n  min_price: 1200');
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockMutate).toHaveBeenCalledWith({ content: 'filters:\n  min_price: 1200' }, expect.any(Object));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('shows parse error and prevents save for invalid yaml', async () => {
    const user = userEvent.setup();
    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    const editor = screen.getByLabelText('yaml-editor');
    fireEvent.change(editor, { target: { value: 'filters: [' } });
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(screen.getByText(/YAML parse error:/)).toBeInTheDocument();
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('resets mutation state when cancel closes dialog', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<DefaultConfigEditor open={true} onOpenChange={onOpenChange} />);

    await user.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(mockReset).toHaveBeenCalledTimes(1);
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('shows mutation error banner', () => {
    mockState.isError = true;
    mockState.error = new Error('update failed');

    render(<DefaultConfigEditor open={true} onOpenChange={vi.fn()} />);

    expect(screen.getByText('Error: update failed')).toBeInTheDocument();
  });
});
