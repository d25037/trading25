import { Briefcase, ChevronRight } from 'lucide-react';
import { Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { cn } from '@/lib/utils';
import type { PortfolioSummary } from '@/types/portfolio';
import { CreatePortfolioDialog } from './CreatePortfolioDialog';

interface PortfolioListProps {
  portfolios: PortfolioSummary[];
  selectedId: number | null;
  onSelect: (id: number) => void;
  isLoading: boolean;
}

function EmptyPortfolioState({ onSelect }: { onSelect: (id: number) => void }) {
  return (
    <Surface className="border border-dashed border-border/70 bg-transparent px-4 py-8">
      <div className="flex flex-col items-center justify-center">
        <Briefcase className="h-12 w-12 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-center">No portfolios found</p>
        <p className="text-sm text-muted-foreground text-center mt-2 mb-4">
          Create your first portfolio to start tracking your investments.
        </p>
        <CreatePortfolioDialog onSuccess={onSelect} />
      </div>
    </Surface>
  );
}

function PortfolioListContent({
  portfolios,
  selectedId,
  onSelect,
}: {
  portfolios: PortfolioSummary[];
  selectedId: number | null;
  onSelect: (id: number) => void;
}) {
  if (portfolios.length === 0) {
    return <EmptyPortfolioState onSelect={onSelect} />;
  }

  return (
    <div className="space-y-2">
      <div className="flex justify-end pb-1">
        <CreatePortfolioDialog onSuccess={onSelect} />
      </div>
      {portfolios.map((portfolio) => (
        <button
          key={portfolio.id}
          type="button"
          onClick={() => onSelect(portfolio.id)}
          aria-label={`Select ${portfolio.name} portfolio`}
          aria-pressed={selectedId === portfolio.id}
          className={cn(
            'w-full rounded-2xl border px-4 py-3 text-left transition-colors',
            selectedId === portfolio.id
              ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
              : 'border-transparent bg-transparent text-foreground hover:border-border/60 hover:bg-[var(--app-surface-muted)]'
          )}
        >
          <div className="flex items-center justify-between">
            <div className="min-w-0 flex-1">
              <h3 className="font-semibold truncate">{portfolio.name}</h3>
              {portfolio.description && <p className="mt-1 text-sm text-muted-foreground truncate">{portfolio.description}</p>}
              <div className="mt-2 flex gap-4 text-xs text-muted-foreground">
                <span>{portfolio.stockCount} stocks</span>
                <span>{portfolio.totalShares.toLocaleString()} shares</span>
              </div>
            </div>
            <ChevronRight
              className={cn(
                'ml-2 h-4 w-4 flex-shrink-0 text-muted-foreground transition-transform',
                selectedId === portfolio.id && 'translate-x-0.5 text-foreground'
              )}
            />
          </div>
        </button>
      ))}
    </div>
  );
}

export function PortfolioList({ portfolios, selectedId, onSelect, isLoading }: PortfolioListProps) {
  return (
    <DataStateWrapper isLoading={isLoading}>
      <PortfolioListContent portfolios={portfolios} selectedId={selectedId} onSelect={onSelect} />
    </DataStateWrapper>
  );
}
