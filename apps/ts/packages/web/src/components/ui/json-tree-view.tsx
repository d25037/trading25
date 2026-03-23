import { cn } from '@/lib/utils';

type JsonTreeViewProps = {
  data: unknown;
  shouldExpandNode?: (level: number, value: unknown, field?: string) => boolean;
  className?: string;
};

type JsonTreeNodeProps = {
  field?: string;
  value: unknown;
  level: number;
  shouldExpandNode?: (level: number, value: unknown, field?: string) => boolean;
};

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isContainerValue(value: unknown): value is Record<string, unknown> | unknown[] {
  return Array.isArray(value) || isObjectRecord(value);
}

function formatScalarValue(value: unknown): string {
  if (typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
    return String(value);
  }
  if (value === undefined) {
    return 'undefined';
  }
  return JSON.stringify(value);
}

function getChildEntries(value: Record<string, unknown> | unknown[]): Array<[string, unknown]> {
  if (Array.isArray(value)) {
    return value.map((item, index) => [String(index), item] as const);
  }
  return Object.entries(value);
}

function getContainerSummary(value: Record<string, unknown> | unknown[]): string {
  if (Array.isArray(value)) {
    return `[${value.length}]`;
  }
  return `{${Object.keys(value).length}}`;
}

function JsonTreeNode({ field, value, level, shouldExpandNode }: JsonTreeNodeProps) {
  if (!isContainerValue(value)) {
    return (
      <div className="flex min-w-0 flex-wrap gap-x-2 break-all">
        {field ? <span className="font-medium text-foreground/90">{field}:</span> : null}
        <span className="text-muted-foreground">{formatScalarValue(value)}</span>
      </div>
    );
  }

  const entries = getChildEntries(value);
  const isExpanded = shouldExpandNode?.(level, value, field) ?? level < 1;

  return (
    <details open={isExpanded} className="group">
      <summary className="cursor-pointer list-none select-none">
        <span className="inline-flex min-w-0 flex-wrap items-center gap-x-2 break-all">
          {field ? <span className="font-medium text-foreground/90">{field}</span> : null}
          <span className="text-muted-foreground">{getContainerSummary(value)}</span>
        </span>
      </summary>
      <div className="mt-2 border-l border-border/60 pl-3">
        {entries.length === 0 ? (
          <div className="text-muted-foreground">{Array.isArray(value) ? '[]' : '{}'}</div>
        ) : (
          <div className="space-y-1">
            {entries.map(([childField, childValue]) => (
              <JsonTreeNode
                key={childField}
                field={childField}
                value={childValue}
                level={level + 1}
                shouldExpandNode={shouldExpandNode}
              />
            ))}
          </div>
        )}
      </div>
    </details>
  );
}

export function JsonTreeView({ data, shouldExpandNode, className }: JsonTreeViewProps) {
  if (!isContainerValue(data)) {
    return <div className={cn('font-mono text-[11px] leading-5', className)}>{formatScalarValue(data)}</div>;
  }

  return (
    <div className={cn('space-y-1 font-mono text-[11px] leading-5', className)}>
      {getChildEntries(data).map(([field, value]) => (
        <JsonTreeNode key={field} field={field} value={value} level={0} shouldExpandNode={shouldExpandNode} />
      ))}
    </div>
  );
}
