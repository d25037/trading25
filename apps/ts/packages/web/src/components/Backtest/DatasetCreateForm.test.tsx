import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import * as React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DatasetCreateForm } from './DatasetCreateForm';

const mockCreateMutate = vi.fn();
const mockResumeMutate = vi.fn();
const mockSetActiveDatasetJobId = vi.fn();

const mockStore = {
  activeDatasetJobId: null as string | null,
};

const mockState = {
  create: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  resume: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
};

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => ({
    activeDatasetJobId: mockStore.activeDatasetJobId,
    setActiveDatasetJobId: mockSetActiveDatasetJobId,
  }),
}));

vi.mock('@/hooks/useDataset', () => ({
  useCreateDataset: () => ({
    mutate: mockCreateMutate,
    isPending: mockState.create.isPending,
    isError: mockState.create.isError,
    error: mockState.create.error,
  }),
  useResumeDataset: () => ({
    mutate: mockResumeMutate,
    isPending: mockState.resume.isPending,
    isError: mockState.resume.isError,
    error: mockState.resume.error,
  }),
}));

vi.mock('./DatasetJobProgress', () => ({
  DatasetJobProgress: () => <div>Dataset Job Progress</div>,
}));

vi.mock('@/components/ui/card', () => ({
  Card: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  CardHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  CardTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  CardContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
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
    type = 'text',
  }: {
    id?: string;
    value?: string | number;
    onChange?: (event: ChangeEvent<HTMLInputElement>) => void;
    placeholder?: string;
    type?: string;
  }) => <input id={id} type={type} value={value} onChange={onChange} placeholder={placeholder} />,
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

describe('DatasetCreateForm', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStore.activeDatasetJobId = null;
    mockState.create.isPending = false;
    mockState.create.isError = false;
    mockState.create.error = null;
    mockState.resume.isPending = false;
    mockState.resume.isError = false;
    mockState.resume.error = null;
  });

  it('renders default values and preset information', () => {
    render(<DatasetCreateForm />);

    expect(screen.getByDisplayValue('quickTesting.db')).toBeInTheDocument();
    expect(screen.getByText(/テスト用小規模データセット/)).toBeInTheDocument();
    expect(screen.getByText('Dataset Job Progress')).toBeInTheDocument();
  });

  it('updates filename when preset changes', async () => {
    const user = userEvent.setup();
    render(<DatasetCreateForm />);

    await user.click(screen.getByRole('button', { name: 'TOPIX 100' }));
    expect(screen.getByDisplayValue('topix100.db')).toBeInTheDocument();
  });

  it('creates dataset and stores active job id on success', async () => {
    const user = userEvent.setup();
    mockCreateMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.({ jobId: 'dataset-job-1' });
    });

    render(<DatasetCreateForm />);

    await user.click(screen.getByRole('button', { name: '作成' }));

    expect(mockCreateMutate).toHaveBeenCalledWith(
      {
        name: 'quickTesting.db',
        preset: 'quickTesting',
        overwrite: false,
        timeoutMinutes: 30,
      },
      expect.any(Object)
    );
    expect(mockSetActiveDatasetJobId).toHaveBeenCalledWith('dataset-job-1');
  });

  it('resumes dataset and stores active job id on success', async () => {
    const user = userEvent.setup();
    mockResumeMutate.mockImplementation((_payload, options) => {
      options?.onSuccess?.({ jobId: 'dataset-job-2' });
    });

    render(<DatasetCreateForm />);

    await user.click(screen.getByRole('button', { name: 'レジューム' }));

    expect(mockResumeMutate).toHaveBeenCalledWith(
      {
        name: 'quickTesting.db',
        preset: 'quickTesting',
        timeoutMinutes: 30,
      },
      expect.any(Object)
    );
    expect(mockSetActiveDatasetJobId).toHaveBeenCalledWith('dataset-job-2');
  });

  it('disables actions when a dataset job is active', () => {
    mockStore.activeDatasetJobId = 'running-job';

    render(<DatasetCreateForm />);

    expect(screen.getByRole('button', { name: '作成' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'レジューム' })).toBeDisabled();
  });

  it('shows create and resume errors', () => {
    mockState.create.isError = true;
    mockState.create.error = new Error('create failed');
    mockState.resume.isError = true;
    mockState.resume.error = new Error('resume failed');

    render(<DatasetCreateForm />);

    expect(screen.getByText('Error: create failed')).toBeInTheDocument();
    expect(screen.getByText('Error: resume failed')).toBeInTheDocument();
  });
});
