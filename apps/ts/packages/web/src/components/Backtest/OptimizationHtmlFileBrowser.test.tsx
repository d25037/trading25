import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, KeyboardEvent, ReactNode } from 'react';
import * as React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { OptimizationHtmlFileContentResponse, OptimizationHtmlFileInfo } from '@/types/backtest';
import { OptimizationHtmlFileBrowser } from './OptimizationHtmlFileBrowser';

const mockUseOptimizationHtmlFiles = vi.fn();
const mockUseOptimizationHtmlFileContent = vi.fn();
const mockRenameMutate = vi.fn();
const mockDeleteMutate = vi.fn();

const mockHookState = {
  filesData: null as { files: OptimizationHtmlFileInfo[]; total: number } | null,
  filesLoading: false,
  contentByKey: {} as Record<string, OptimizationHtmlFileContentResponse>,
  contentLoading: false,
  rename: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  delete: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
};

vi.mock('@/hooks/useOptimization', () => ({
  useOptimizationHtmlFiles: (...args: unknown[]) => mockUseOptimizationHtmlFiles(...args),
  useOptimizationHtmlFileContent: (...args: unknown[]) => mockUseOptimizationHtmlFileContent(...args),
  useRenameOptimizationHtmlFile: () => ({
    mutate: mockRenameMutate,
    isPending: mockHookState.rename.isPending,
    isError: mockHookState.rename.isError,
    error: mockHookState.rename.error,
  }),
  useDeleteOptimizationHtmlFile: () => ({
    mutate: mockDeleteMutate,
    isPending: mockHookState.delete.isPending,
    isError: mockHookState.delete.isError,
    error: mockHookState.delete.error,
  }),
}));

vi.mock('./ResultHtmlViewer', () => ({
  ResultHtmlViewer: ({
    htmlContent,
    isLoading,
  }: {
    htmlContent: string | null;
    isLoading: boolean;
  }) => <div data-testid="opt-viewer">{isLoading ? 'loading' : htmlContent ?? 'empty'}</div>,
}));

vi.mock('@/components/ui/card', () => ({
  Card: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  CardContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock('@/components/ui/button', () => ({
  Button: ({
    children,
    onClick,
    disabled,
    className,
  }: {
    children: ReactNode;
    onClick?: () => void;
    disabled?: boolean;
    className?: string;
  }) => (
    <button type="button" onClick={onClick} disabled={disabled} className={className}>
      {children}
    </button>
  ),
}));

vi.mock('@/components/ui/input', () => ({
  Input: React.forwardRef(
    (
      {
        value,
        onChange,
        onKeyDown,
        placeholder,
        className,
        disabled,
      }: {
        value?: string;
        onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
        onKeyDown?: (event: KeyboardEvent<HTMLInputElement>) => void;
        placeholder?: string;
        className?: string;
        disabled?: boolean;
      },
      ref: React.ForwardedRef<HTMLInputElement>
    ) => (
      <input
        ref={ref}
        value={value}
        onChange={onChange}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        className={className}
        disabled={disabled}
      />
    )
  ),
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
    SelectTrigger: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    SelectValue: ({ placeholder }: { placeholder?: string }) => <span>{placeholder}</span>,
    SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    SelectItem: ({ children, value }: { children: ReactNode; value: string }) => {
      const onValueChange = React.useContext(SelectContext);
      return (
        <button type="button" onClick={() => onValueChange(value)}>
          {children}
        </button>
      );
    },
  };
});

function createFile(overrides: Partial<OptimizationHtmlFileInfo>): OptimizationHtmlFileInfo {
  return {
    strategy_name: 'production/alpha',
    filename: 'optimization.html',
    dataset_name: 'dataset-a',
    created_at: '2026-02-10T12:00:00Z',
    ...overrides,
  } as OptimizationHtmlFileInfo;
}

describe('OptimizationHtmlFileBrowser', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHookState.filesData = { files: [], total: 0 };
    mockHookState.filesLoading = false;
    mockHookState.contentByKey = {};
    mockHookState.contentLoading = false;
    mockHookState.rename.isPending = false;
    mockHookState.rename.isError = false;
    mockHookState.rename.error = null;
    mockHookState.delete.isPending = false;
    mockHookState.delete.isError = false;
    mockHookState.delete.error = null;

    mockUseOptimizationHtmlFiles.mockImplementation((_strategy?: string) => ({
      data: mockHookState.filesData,
      isLoading: mockHookState.filesLoading,
    }));
    mockUseOptimizationHtmlFileContent.mockImplementation((strategy: string | null, filename: string | null) => {
      if (!strategy || !filename) {
        return { data: null, isLoading: false };
      }
      return {
        data: mockHookState.contentByKey[`${strategy}/${filename}`] ?? null,
        isLoading: mockHookState.contentLoading,
      };
    });
  });

  it('renders loading and empty states', () => {
    mockHookState.filesLoading = true;
    const { container, rerender } = render(<OptimizationHtmlFileBrowser />);
    expect(container.querySelector('.animate-spin')).toBeInTheDocument();

    mockHookState.filesLoading = false;
    mockHookState.filesData = { files: [], total: 0 };
    rerender(<OptimizationHtmlFileBrowser />);

    expect(screen.getByText('No optimization result files found')).toBeInTheDocument();
    expect(screen.getByText('Select a file to preview')).toBeInTheDocument();
  });

  it('filters files, selects file, and opens optimization report in a new tab', async () => {
    const user = userEvent.setup();
    mockHookState.filesData = {
      files: [
        createFile({
          strategy_name: 'production/alpha',
          filename: 'alpha-opt.html',
          dataset_name: 'dataset-alpha',
          created_at: '2026-02-10T08:00:00Z',
        }),
        createFile({
          strategy_name: 'experimental/beta',
          filename: 'beta-opt.html',
          dataset_name: 'dataset-beta',
          created_at: '2026-02-11T08:00:00Z',
        }),
      ],
      total: 4,
    };
    mockHookState.contentByKey['experimental/beta/beta-opt.html'] = {
      html_content: btoa('<html><body>opt-beta</body></html>'),
    } as OptimizationHtmlFileContentResponse;

    const createObjectUrlSpy = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:opt-report');
    const revokeObjectUrlSpy = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    const setTimeoutSpy = vi.spyOn(window, 'setTimeout').mockImplementation((handler) => {
      if (typeof handler === 'function') handler();
      return 0 as unknown as number;
    });

    render(<OptimizationHtmlFileBrowser />);

    expect(screen.getByText('2 files (4 total)')).toBeInTheDocument();
    await user.type(screen.getByPlaceholderText('Search files...'), 'beta');
    expect(screen.getByText('beta-opt.html')).toBeInTheDocument();
    expect(screen.queryByText('alpha-opt.html')).not.toBeInTheDocument();

    const betaFileButton = screen.getByText('beta-opt.html').closest('button');
    expect(betaFileButton).not.toBeNull();
    await user.click(betaFileButton as HTMLButtonElement);
    expect(mockUseOptimizationHtmlFileContent).toHaveBeenLastCalledWith('experimental/beta', 'beta-opt.html');
    expect(screen.getByTestId('opt-viewer')).toHaveTextContent('<html><body>opt-beta</body></html>');

    await user.click(screen.getByRole('button', { name: 'Open in new tab' }));
    expect(createObjectUrlSpy).toHaveBeenCalledTimes(1);
    expect(openSpy).toHaveBeenCalledWith('blob:opt-report', '_blank');
    expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), 60000);
    expect(revokeObjectUrlSpy).toHaveBeenCalledWith('blob:opt-report');

    await user.click(screen.getByRole('button', { name: 'production/alpha' }));
    expect(mockUseOptimizationHtmlFiles).toHaveBeenLastCalledWith('production/alpha');
  });

  it('renames and deletes optimization result files', async () => {
    const user = userEvent.setup();
    mockHookState.filesData = {
      files: [createFile({ filename: 'alpha-opt.html' })],
      total: 1,
    };
    mockHookState.contentByKey['production/alpha/alpha-opt.html'] = {
      html_content: btoa('<html><body>opt-alpha</body></html>'),
    } as OptimizationHtmlFileContentResponse;

    mockRenameMutate.mockImplementation(
      (
        _payload: unknown,
        options?: {
          onSuccess?: (data: { new_filename: string }) => void;
        }
      ) => {
        options?.onSuccess?.({ new_filename: 'renamed-opt.html' });
      }
    );
    mockDeleteMutate.mockImplementation(
      (
        _payload: unknown,
        options?: {
          onSuccess?: () => void;
        }
      ) => {
        options?.onSuccess?.();
      }
    );

    const { container } = render(<OptimizationHtmlFileBrowser />);
    const alphaFileButton = screen.getByText('alpha-opt.html').closest('button');
    expect(alphaFileButton).not.toBeNull();
    await user.click(alphaFileButton as HTMLButtonElement);

    const renameButton = container.querySelector('svg.lucide-pencil')?.closest('button');
    await user.click(renameButton as HTMLButtonElement);

    const renameInput = screen.getByDisplayValue('alpha-opt.html');
    await user.clear(renameInput);
    await user.type(renameInput, 'renamed-opt');
    await user.keyboard('{Enter}');

    expect(mockRenameMutate).toHaveBeenCalledWith(
      {
        strategy: 'production/alpha',
        filename: 'alpha-opt.html',
        request: { new_filename: 'renamed-opt.html' },
      },
      expect.any(Object)
    );
    expect(screen.getByText('renamed-opt.html')).toBeInTheDocument();

    const deleteButton = container.querySelector('svg.lucide-trash2')?.closest('button');
    await user.click(deleteButton as HTMLButtonElement);
    await user.click(screen.getByRole('button', { name: 'Delete' }));

    expect(mockDeleteMutate).toHaveBeenCalledWith(
      {
        strategy: 'production/alpha',
        filename: 'renamed-opt.html',
      },
      expect.any(Object)
    );
    expect(screen.getByText('Select a file to preview')).toBeInTheDocument();
  });

  it('handles invalid base64 and mutation errors', async () => {
    const user = userEvent.setup();
    mockHookState.filesData = {
      files: [createFile({ filename: 'bad-opt.html' })],
      total: 1,
    };
    mockHookState.contentByKey['production/alpha/bad-opt.html'] = {
      html_content: '###invalid###',
    } as OptimizationHtmlFileContentResponse;
    mockHookState.rename.isError = true;
    mockHookState.rename.error = new Error('rename optimization failed');
    mockHookState.delete.isError = true;
    mockHookState.delete.error = new Error('delete optimization failed');

    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { container } = render(<OptimizationHtmlFileBrowser />);
    const badFileButton = screen.getByText('bad-opt.html').closest('button');
    expect(badFileButton).not.toBeNull();
    await user.click(badFileButton as HTMLButtonElement);
    expect(screen.getByText('rename optimization failed')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Open in new tab' }));
    expect(consoleErrorSpy).toHaveBeenCalledWith('Failed to decode base64 HTML content');
    expect(openSpy).not.toHaveBeenCalled();

    const deleteButton = container.querySelector('svg.lucide-trash2')?.closest('button');
    await user.click(deleteButton as HTMLButtonElement);
    expect(screen.getByText('Error: delete optimization failed')).toBeInTheDocument();
  });
});
