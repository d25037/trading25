import type { ShikihoSnapshotV1 } from '@trading25/shikiho-extension/contract';

const metrics = [
  ['growth', '成長性'],
  ['profitability', '収益性'],
  ['safety', '安全性'],
  ['scale', '規模'],
  ['value', '割安度'],
  ['priceMomentum', '値上がり'],
] as const satisfies ReadonlyArray<[keyof ShikihoSnapshotV1['score'], string]>;

const CENTER = 60;
const RADIUS = 46;
const STAR_NUMBERS = [1, 2, 3, 4, 5] as const;

function polygonPoints(values: readonly number[]): string {
  return values
    .map((value, index) => {
      const angle = (Math.PI / 3) * index - Math.PI / 2;
      const radius = (RADIUS * value) / 5;
      return `${CENTER + Math.cos(angle) * radius},${CENTER + Math.sin(angle) * radius}`;
    })
    .join(' ');
}

const gridPolygons = [1, 2, 3, 4, 5].map((level) => polygonPoints(Array.from({ length: 6 }, () => level)));

function ScoreRadar({ values, label }: { values: number[]; label: string }) {
  return (
    <svg role="img" aria-label={label} viewBox="0 0 120 120" className="h-32 w-32 shrink-0 text-border">
      {gridPolygons.map((points) => (
        <polygon key={points} points={points} fill="none" stroke="currentColor" strokeWidth="0.75" />
      ))}
      {metrics.map(([key], index) => {
        const angle = (Math.PI / 3) * index - Math.PI / 2;
        return (
          <line
            key={key}
            x1={CENTER}
            y1={CENTER}
            x2={CENTER + Math.cos(angle) * RADIUS}
            y2={CENTER + Math.sin(angle) * RADIUS}
            stroke="currentColor"
            strokeWidth="0.75"
          />
        );
      })}
      <polygon
        points={polygonPoints(values)}
        className="fill-primary/25 stroke-primary"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function ShikihoScoreCard({ score }: { score: ShikihoSnapshotV1['score'] }) {
  const overall = score.overall;
  const values = metrics.map(([key]) => score[key]);
  const completeValues = values.every((value): value is number => value !== null) ? values : null;
  const radarLabel = metrics.map(([, label], index) => `${label} ${values[index] ?? '—'}`).join('、');

  return (
    <section
      data-testid="shikiho-score-card"
      aria-label="四季報スコア"
      className="col-span-full rounded-lg bg-[var(--app-surface-muted)] p-3"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="text-xs font-semibold tracking-wide text-muted-foreground">四季報スコア</h4>
        <div className="flex items-center gap-2">
          <span className="flex text-base leading-none text-amber-500" aria-hidden="true">
            {STAR_NUMBERS.map((starNumber) => {
              const filled = overall !== null && starNumber <= overall;
              return (
                <span key={starNumber} data-testid="shikiho-score-star">
                  {filled ? <span data-testid="shikiho-score-star-filled">★</span> : '☆'}
                </span>
              );
            })}
          </span>
          <span className="text-sm font-semibold tabular-nums text-foreground">総合 {overall ?? '—'} / 5</span>
        </div>
      </div>
      <div className="mt-2 flex flex-wrap items-center justify-center gap-x-6 gap-y-3 sm:justify-start">
        {completeValues ? <ScoreRadar values={completeValues} label={`四季報スコア ${radarLabel}`} /> : null}
        <dl data-testid="shikiho-score-values" className="grid min-w-56 flex-1 grid-cols-2 gap-x-5 gap-y-1 text-xs">
          {metrics.map(([key, label]) => (
            <div key={key} className="flex items-center justify-between gap-2">
              <dt className="text-muted-foreground">{label}</dt>
              <dd className="font-semibold tabular-nums text-foreground">{score[key] ?? '—'}</dd>
            </div>
          ))}
        </dl>
      </div>
    </section>
  );
}
