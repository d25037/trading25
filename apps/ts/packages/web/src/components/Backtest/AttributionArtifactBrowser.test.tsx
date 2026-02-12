import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { AttributionArtifactBrowser } from './AttributionArtifactBrowser';

const mockUseAttributionArtifactFiles = vi.fn();
const mockUseAttributionArtifactContent = vi.fn();

vi.mock('@/hooks/useBacktest', () => ({
  useAttributionArtifactFiles: (...args: unknown[]) => mockUseAttributionArtifactFiles(...args),
  useAttributionArtifactContent: (...args: unknown[]) => mockUseAttributionArtifactContent(...args),
}));

describe('AttributionArtifactBrowser', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseAttributionArtifactFiles.mockReturnValue({
      data: { files: [], total: 0 },
      isLoading: false,
    });
    mockUseAttributionArtifactContent.mockReturnValue({
      data: null,
      isLoading: false,
    });
  });

  it('renders loading state', () => {
    mockUseAttributionArtifactFiles.mockReturnValue({
      data: null,
      isLoading: true,
    });

    const { container } = render(<AttributionArtifactBrowser />);
    expect(container.querySelector('svg.animate-spin')).toBeInTheDocument();
  });

  it('renders empty state when no files', () => {
    render(<AttributionArtifactBrowser />);
    expect(screen.getByText('No attribution artifacts found')).toBeInTheDocument();
    expect(screen.getByText('Select an attribution artifact from the list')).toBeInTheDocument();
  });

  it('renders selected artifact metadata and json', async () => {
    const user = userEvent.setup();
    mockUseAttributionArtifactFiles.mockReturnValue({
      data: {
        files: [
          {
            strategy_name: 'experimental/range_break_v18',
            filename: 'attribution_20260112_120000_job-1.json',
            created_at: '2026-01-12T12:00:00Z',
            size_bytes: 2048,
            job_id: 'job-1',
          },
          {
            strategy_name: 'experimental/range_break_v19',
            filename: 'attribution_20260112_120100_job-2.json',
            created_at: '2026-01-12T12:01:00Z',
            size_bytes: 512,
            job_id: 'job-2',
          },
        ],
        total: 2,
      },
      isLoading: false,
    });
    mockUseAttributionArtifactContent.mockImplementation((strategy: string | null, filename: string | null) => {
      if (!strategy || !filename) {
        return { data: null, isLoading: false };
      }
      return {
        data: {
          artifact: {
            saved_at: '2026-01-12T12:00:00+00:00',
            strategy: {
              name: strategy,
              yaml_path: '/tmp/strategies/experimental/range_break_v18.yaml',
            },
            runtime: {
              shapley_top_n: 5,
              shapley_permutations: 128,
              random_seed: 42,
            },
            databases: {
              dataset_name: 'prime_202601',
              market_db: { name: 'market.db' },
              portfolio_db: { name: 'portfolio.db' },
            },
          },
        },
        isLoading: false,
      };
    });

    render(<AttributionArtifactBrowser />);

    await user.click(screen.getByText('attribution_20260112_120000_job-1.json'));

    expect(screen.getByText('Saved At')).toBeInTheDocument();
    expect(screen.getByText('/tmp/strategies/experimental/range_break_v18.yaml')).toBeInTheDocument();
    expect(screen.getByText('prime_202601')).toBeInTheDocument();
    expect(screen.getByText('portfolio.db')).toBeInTheDocument();
    expect(screen.getByText('JSON').nextElementSibling).toHaveTextContent(/"shapley_top_n":\s*5/);
    expect(screen.getByText((content) => /2\.0\s*KB/.test(content))).toBeInTheDocument();
  });

  it('shows artifact loading state after selecting a file', async () => {
    const user = userEvent.setup();
    mockUseAttributionArtifactFiles.mockReturnValue({
      data: {
        files: [
          {
            strategy_name: 'experimental/range_break_v18',
            filename: 'attribution_20260112_120000_job-1.json',
            created_at: '2026-01-12T12:00:00Z',
            size_bytes: 512,
            job_id: 'job-1',
          },
        ],
        total: 1,
      },
      isLoading: false,
    });
    mockUseAttributionArtifactContent.mockImplementation((strategy: string | null, filename: string | null) => {
      if (!strategy || !filename) {
        return { data: null, isLoading: false };
      }
      return { data: null, isLoading: true };
    });

    const { container } = render(<AttributionArtifactBrowser />);
    await user.click(screen.getByText('attribution_20260112_120000_job-1.json'));
    expect(container.querySelector('svg.animate-spin')).toBeInTheDocument();
  });
});
