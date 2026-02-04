import { Briefcase, ChevronRight } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
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
    <Card className="glass-panel">
      <CardContent className="flex flex-col items-center justify-center py-8">
        <Briefcase className="h-12 w-12 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-center">No portfolios found</p>
        <p className="text-sm text-muted-foreground text-center mt-2 mb-4">
          Create your first portfolio to start tracking your investments.
        </p>
        <CreatePortfolioDialog onSuccess={onSelect} />
      </CardContent>
    </Card>
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
    <div className="space-y-3">
      <div className="flex justify-end">
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
            'w-full text-left p-4 rounded-xl transition-all duration-200',
            'hover:scale-[1.01] active:scale-[0.99]',
            selectedId === portfolio.id ? 'gradient-primary text-white shadow-lg' : 'glass-panel hover:bg-accent/50'
          )}
        >
          <div className="flex items-center justify-between">
            <div className="min-w-0 flex-1">
              <h3 className="font-semibold truncate">{portfolio.name}</h3>
              {portfolio.description && <p className="text-sm opacity-80 truncate">{portfolio.description}</p>}
              <div className="flex gap-4 mt-2 text-sm opacity-70">
                <span>{portfolio.stockCount} stocks</span>
                <span>{portfolio.totalShares.toLocaleString()} shares</span>
              </div>
            </div>
            <ChevronRight className="h-5 w-5 flex-shrink-0 ml-2" />
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
