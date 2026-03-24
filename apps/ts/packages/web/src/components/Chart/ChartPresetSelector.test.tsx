import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import * as React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ChartPresetSelector } from './ChartPresetSelector';

const mockChartStore = {
  presets: [
    { id: 'preset-1', name: 'Alpha', settings: {}, createdAt: 1, updatedAt: 1 },
    { id: 'preset-2', name: 'Beta', settings: {}, createdAt: 2, updatedAt: 2 },
  ],
  activePresetId: 'preset-1' as string | null,
  createPreset: vi.fn(),
  updatePreset: vi.fn(),
  deletePreset: vi.fn(),
  loadPreset: vi.fn(),
  renamePreset: vi.fn(),
  duplicatePreset: vi.fn(),
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

vi.mock('@/components/ui/select', async () => {
  const React = await import('react');

  const SelectContext = React.createContext<{
    value: string;
    onValueChange: (value: string) => void;
  } | null>(null);

  function Select({
    value,
    onValueChange,
    children,
  }: {
    value: string;
    onValueChange: (value: string) => void;
    children: React.ReactNode;
  }) {
    return <SelectContext.Provider value={{ value, onValueChange }}>{children}</SelectContext.Provider>;
  }

  function SelectTrigger({ children }: { children: React.ReactNode }) {
    return (
      <button type="button" role="combobox" aria-label="Chart preset">
        {children}
      </button>
    );
  }

  function SelectValue({ placeholder }: { placeholder?: string }) {
    const context = React.useContext(SelectContext);
    if (!context) {
      return null;
    }
    const label = context.value === 'none' ? placeholder : context.value;
    return <span>{label}</span>;
  }

  function SelectContent({ children }: { children: React.ReactNode }) {
    return <div>{children}</div>;
  }

  function SelectItem({ value, children }: { value: string; children: React.ReactNode }) {
    const context = React.useContext(SelectContext);
    if (!context) {
      return null;
    }
    return (
      <button type="button" role="option" onClick={() => context.onValueChange(value)}>
        {children}
      </button>
    );
  }

  return {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
  };
});

describe('ChartPresetSelector', () => {
  beforeEach(() => {
    mockChartStore.activePresetId = 'preset-1';
    mockChartStore.createPreset.mockReset();
    mockChartStore.updatePreset.mockReset();
    mockChartStore.deletePreset.mockReset();
    mockChartStore.loadPreset.mockReset();
    mockChartStore.renamePreset.mockReset();
    mockChartStore.duplicatePreset.mockReset();
  });

  it('loads another preset when selected', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByRole('option', { name: 'Beta' }));

    expect(mockChartStore.loadPreset).toHaveBeenCalledWith('preset-2');
  });

  it('does not load the none option', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByRole('option', { name: '(No preset)' }));

    expect(mockChartStore.loadPreset).not.toHaveBeenCalled();
  });

  it('saves the active preset', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('Save to preset'));

    expect(mockChartStore.updatePreset).toHaveBeenCalledWith('preset-1');
  });

  it('creates a preset from the dialog', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('Create preset'));
    await user.type(screen.getByLabelText('Preset Name'), 'Momentum');
    await user.click(screen.getByRole('button', { name: 'Create' }));

    expect(mockChartStore.createPreset).toHaveBeenCalledWith('Momentum');
  });

  it('submits create, rename, and duplicate dialogs with the Enter key', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('Create preset'));
    await user.type(screen.getByLabelText('Preset Name'), 'Breakout{Enter}');
    expect(mockChartStore.createPreset).toHaveBeenCalledWith('Breakout');

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Rename' }));
    const renameInput = screen.getByLabelText('Preset Name');
    await user.clear(renameInput);
    await user.type(renameInput, 'Alpha Next{Enter}');
    expect(mockChartStore.renamePreset).toHaveBeenCalledWith('preset-1', 'Alpha Next');

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Duplicate' }));
    const duplicateInput = screen.getByLabelText('New Preset Name');
    await user.clear(duplicateInput);
    await user.type(duplicateInput, 'Alpha Fork{Enter}');
    expect(mockChartStore.duplicatePreset).toHaveBeenCalledWith('preset-1', 'Alpha Fork');
  });

  it('renames and duplicates the active preset from the actions menu', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Rename' }));
    const renameInput = screen.getByLabelText('Preset Name');
    await user.clear(renameInput);
    await user.type(renameInput, 'Alpha Prime');
    await user.click(screen.getByRole('button', { name: 'Rename' }));

    expect(mockChartStore.renamePreset).toHaveBeenCalledWith('preset-1', 'Alpha Prime');

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Duplicate' }));
    const duplicateInput = screen.getByLabelText('New Preset Name');
    await user.clear(duplicateInput);
    await user.type(duplicateInput, 'Alpha Copy');
    await user.click(screen.getByRole('button', { name: 'Duplicate' }));

    expect(mockChartStore.duplicatePreset).toHaveBeenCalledWith('preset-1', 'Alpha Copy');
  });

  it('deletes the active preset from the actions menu', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Delete' }));
    await user.click(screen.getByRole('button', { name: 'Delete' }));

    expect(mockChartStore.deletePreset).toHaveBeenCalledWith('preset-1');
  });

  it('closes the actions menu and each dialog without submitting changes', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('More actions'));
    expect(screen.getByRole('button', { name: 'Rename' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close menu' }));
    expect(screen.queryByRole('button', { name: 'Rename' })).not.toBeInTheDocument();

    await user.click(screen.getByTitle('Create preset'));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('button', { name: 'Create' })).not.toBeInTheDocument();

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Rename' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('button', { name: 'Rename' })).not.toBeInTheDocument();

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Duplicate' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('button', { name: 'Duplicate' })).not.toBeInTheDocument();

    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Delete' }));
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(mockChartStore.deletePreset).not.toHaveBeenCalled();
  });

  it('does not submit blank preset names from dialogs', async () => {
    const user = userEvent.setup();
    render(<ChartPresetSelector />);

    await user.click(screen.getByTitle('Create preset'));
    await user.type(screen.getByLabelText('Preset Name'), '   {Enter}');
    expect(mockChartStore.createPreset).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Rename' }));
    const renameInput = screen.getByLabelText('Preset Name');
    await user.clear(renameInput);
    await user.type(renameInput, '   {Enter}');
    expect(mockChartStore.renamePreset).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByTitle('More actions'));
    await user.click(screen.getByRole('button', { name: 'Duplicate' }));
    const duplicateInput = screen.getByLabelText('New Preset Name');
    await user.clear(duplicateInput);
    await user.type(duplicateInput, '   {Enter}');
    expect(mockChartStore.duplicatePreset).not.toHaveBeenCalled();
  });

  it('disables save and more-actions buttons when no preset is active', () => {
    mockChartStore.activePresetId = null;

    render(<ChartPresetSelector />);

    expect(screen.getByTitle('Save to preset')).toBeDisabled();
    expect(screen.getByTitle('More actions')).toBeDisabled();
  });
});
