import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { DatasetManager } from './DatasetManager';

vi.mock('./DatasetCreateForm', () => ({
  DatasetCreateForm: () => <div>Dataset Create Form</div>,
}));

vi.mock('./DatasetList', () => ({
  DatasetList: () => <div>Dataset List</div>,
}));

describe('DatasetManager', () => {
  it('renders dataset create form and dataset list', () => {
    render(<DatasetManager />);

    expect(screen.getByText('Dataset Create Form')).toBeInTheDocument();
    expect(screen.getByText('Dataset List')).toBeInTheDocument();
  });
});
