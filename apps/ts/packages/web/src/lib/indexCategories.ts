export const INDEX_CATEGORY_ORDER = ['synthetic', 'topix', 'sector17', 'sector33', 'market', 'style', 'growth', 'reit'];

export const INDEX_CATEGORY_LABELS: Record<string, string> = {
  synthetic: 'Benchmarks',
  topix: 'TOPIX',
  sector33: '33 Sectors',
  sector17: 'TOPIX-17 Sectors',
  market: 'Market',
  growth: 'Growth',
  reit: 'REIT',
  style: 'Style',
};

export function getIndexCategorySortOrder(category: string): number {
  const index = INDEX_CATEGORY_ORDER.indexOf(category);
  return index === -1 ? Number.MAX_SAFE_INTEGER : index;
}
