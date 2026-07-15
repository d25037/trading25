import type { ShikihoSnapshotV1 } from '@trading25/shikiho-extension/contract';

const metrics = [
  ['growth', '成長性'],
  ['profitability', '収益性'],
  ['safety', '安全性'],
  ['scale', '規模'],
  ['value', '割安度'],
  ['priceMomentum', '値上がり'],
] as const satisfies ReadonlyArray<[keyof ShikihoSnapshotV1['score'], string]>;

const tableMetrics = [metrics[0], metrics[3], metrics[1], metrics[4], metrics[2], metrics[5]] as const;

const CENTER_X = 120;
const CENTER_Y = 110;
const RADIUS = 68;
const STAR_NUMBERS = [1, 2, 3, 4, 5] as const;

const axisLabels = [
  { x: CENTER_X, y: 16, textAnchor: 'middle' },
  { x: 205, y: 61, textAnchor: 'start' },
  { x: 205, y: 164, textAnchor: 'start' },
  { x: CENTER_X, y: 214, textAnchor: 'middle' },
  { x: 35, y: 164, textAnchor: 'end' },
  { x: 35, y: 61, textAnchor: 'end' },
] as const;

function pointAt(index: number, radius: number): { x: number; y: number } {
  const angle = (Math.PI / 3) * index - Math.PI / 2;
  return {
    x: CENTER_X + Math.cos(angle) * radius,
    y: CENTER_Y + Math.sin(angle) * radius,
  };
}

function polygonPoints(values: readonly number[]): string {
  return values
    .map((value, index) => {
      const point = pointAt(index, (RADIUS * value) / 5);
      return `${point.x},${point.y}`;
    })
    .join(' ');
}

const gridPolygons = [1, 2, 3, 4, 5].map((level) => polygonPoints(Array.from({ length: 6 }, () => level)));

function ScoreRadar({ values, label }: { values: number[]; label: string }) {
  return (
    <svg
      role="img"
      aria-label={label}
      viewBox="0 0 240 220"
      className="mx-auto h-auto w-full max-w-[260px] shrink-0 overflow-visible"
    >
      <g className="stroke-border/80">
        {gridPolygons.map((points) => (
          <polygon key={points} points={points} fill="none" stroke="currentColor" strokeWidth="0.8" />
        ))}
        {metrics.map(([key], index) => {
          const outerPoint = pointAt(index, RADIUS);
          return (
            <line
              key={key}
              x1={CENTER_X}
              y1={CENTER_Y}
              x2={outerPoint.x}
              y2={outerPoint.y}
              stroke="currentColor"
              strokeWidth="0.8"
            />
          );
        })}
      </g>
      <polygon
        data-testid="shikiho-score-data-polygon"
        points={polygonPoints(values)}
        className="fill-orange-400/20 stroke-orange-500"
        strokeWidth="1.6"
        strokeLinejoin="round"
      />
      {values.map((value, index) => {
        const metric = metrics[index];
        if (!metric) return null;
        const point = pointAt(index, (RADIUS * value) / 5);
        return (
          <circle
            key={metric[0]}
            data-testid="shikiho-score-vertex"
            cx={point.x}
            cy={point.y}
            r="2.7"
            className="fill-orange-500 stroke-background"
            strokeWidth="1.2"
          />
        );
      })}
      {metrics.map(([, metricLabel], index) => {
        const position = axisLabels[index];
        if (!position) return null;
        return (
          <text
            key={metricLabel}
            x={position.x}
            y={position.y}
            textAnchor={position.textAnchor}
            dominantBaseline="middle"
            className="fill-muted-foreground text-[10px] font-semibold"
          >
            {metricLabel}
          </text>
        );
      })}
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
      className="col-span-full rounded-xl border border-border/70 bg-background px-4 py-3 shadow-sm"
    >
      <div data-testid="shikiho-score-header" className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <h4 className="text-base font-bold tracking-wide text-foreground">四季報スコア</h4>
        <div className="flex items-center gap-3">
          <span className="flex gap-0.5 text-xl leading-none" aria-hidden="true">
            {STAR_NUMBERS.map((starNumber) => {
              const filled = overall !== null && starNumber <= overall;
              return (
                <span
                  key={starNumber}
                  data-testid="shikiho-score-star"
                  data-state={filled ? 'filled' : 'empty'}
                  className={filled ? 'text-red-500' : 'text-muted-foreground/25'}
                >
                  ★
                </span>
              );
            })}
          </span>
          <span className="flex items-baseline gap-1 font-semibold text-muted-foreground">
            総合
            <span className="text-2xl font-bold tabular-nums text-red-500">{overall ?? '—'}</span>
            <span className="text-xs">/ 5</span>
          </span>
        </div>
      </div>

      <div
        data-testid="shikiho-score-body"
        className="mx-auto mt-3 grid max-w-3xl grid-cols-1 items-center gap-5 md:grid-cols-[minmax(220px,260px)_minmax(0,1fr)] md:gap-8"
      >
        {completeValues ? <ScoreRadar values={completeValues} label={`四季報スコア ${radarLabel}`} /> : null}
        <dl data-testid="shikiho-score-values" className="grid grid-cols-2 gap-x-6 text-sm">
          {tableMetrics.map(([key, label]) => (
            <div key={key} className="flex items-center justify-between gap-3 border-b border-border/70 py-2">
              <dt className="font-medium text-muted-foreground">{label}</dt>
              <dd className="text-lg font-bold tabular-nums text-foreground">{score[key] ?? '—'}</dd>
            </div>
          ))}
        </dl>
      </div>
    </section>
  );
}
