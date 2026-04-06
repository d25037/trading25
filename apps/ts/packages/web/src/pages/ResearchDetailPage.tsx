import { useNavigate } from '@tanstack/react-router';
import {
  ResearchDetailEmptyState,
  ResearchDetailErrorState,
  ResearchDetailLoadingState,
  ResearchDetailView,
} from '@/components/Research/ResearchDetailView';
import { useResearchDetail } from '@/hooks/useResearch';
import { serializeResearchSearch } from '@/lib/routeSearch';
import { researchDetailRoute } from '@/router';

export function ResearchDetailPage() {
  const navigate = useNavigate();
  const search = researchDetailRoute.useSearch();
  const experimentId = search.experimentId ?? null;
  const runId = search.runId ?? null;

  const detailQuery = useResearchDetail(experimentId, runId);

  const goToCatalog = () => {
    void navigate({ to: '/research' });
  };

  if (!experimentId) {
    return (
      <div className="min-h-0 flex-1 overflow-auto px-4 py-4 sm:px-6 sm:py-5">
        <div className="mx-auto w-full max-w-[1180px]">
          <ResearchDetailEmptyState onBack={goToCatalog} />
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-0 flex-1 overflow-auto px-4 py-4 sm:px-6 sm:py-5">
      <div className="mx-auto w-full max-w-[1180px]">
        {detailQuery.isLoading ? (
          <ResearchDetailLoadingState />
        ) : detailQuery.error ? (
          <ResearchDetailErrorState message={detailQuery.error.message} />
        ) : detailQuery.data ? (
          <ResearchDetailView
            detail={detailQuery.data}
            onBack={goToCatalog}
            onSelectRun={(nextRunId) => {
              void navigate({
                to: '/research/detail',
                search: serializeResearchSearch({
                  experimentId,
                  runId: nextRunId,
                }),
              });
            }}
          />
        ) : (
          <ResearchDetailEmptyState onBack={goToCatalog} />
        )}
      </div>
    </div>
  );
}
