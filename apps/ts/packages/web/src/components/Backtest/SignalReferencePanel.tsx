/**
 * Signal Reference Panel
 *
 * YAMLエディター横に表示するシグナルリファレンスパネル
 * bt APIからシグナル定義を動的に取得
 */

import { Check, ChevronDown, ChevronRight, Copy, Loader2, Search } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useSignalReference } from '@/hooks/useBacktest';
import type { SignalCategory, SignalDefinition } from '@/types/backtest';
import { logger } from '@/utils/logger';

function formatConstraints(constraints: SignalDefinition['fields'][number]['constraints']): string[] {
  if (!constraints) return [];
  const parts: string[] = [];
  if (constraints.gt !== undefined) parts.push(`>${constraints.gt}`);
  if (constraints.ge !== undefined) parts.push(`>=${constraints.ge}`);
  if (constraints.lt !== undefined) parts.push(`<${constraints.lt}`);
  if (constraints.le !== undefined) parts.push(`<=${constraints.le}`);
  return parts;
}

interface SignalItemProps {
  signal: SignalDefinition;
  onCopy: (snippet: string) => void;
}

function SignalItem({ signal, onCopy }: SignalItemProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    onCopy(signal.yaml_snippet);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="border-b border-muted last:border-b-0">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 p-2 text-left hover:bg-muted/50 transition-colors"
      >
        {isExpanded ? (
          <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
        )}
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium truncate">{signal.name}</p>
          <p className="text-xs text-muted-foreground truncate">{signal.key}</p>
        </div>
      </button>

      {isExpanded && (
        <div className="px-4 pb-3 space-y-3">
          <p className="text-sm text-muted-foreground">{signal.description}</p>

          <div className="bg-muted/30 rounded p-2">
            <p className="text-xs font-medium text-muted-foreground mb-1">Usage Hint</p>
            <p className="text-xs">{signal.usage_hint}</p>
          </div>

          <div>
            <p className="text-xs font-medium text-muted-foreground mb-1">Parameters</p>
            <ul className="text-xs space-y-1">
              {signal.fields.map((field) => {
                const constraintParts = formatConstraints(field.constraints);
                return (
                  <li key={field.name} className="flex items-start gap-2">
                    <code className="text-primary">{field.name}</code>
                    <span className="text-muted-foreground">-</span>
                    <span className="text-muted-foreground">{field.description}</span>
                    {field.options && <span className="text-muted-foreground">({field.options.join(' | ')})</span>}
                    {constraintParts.length > 0 && (
                      <span className="text-xs text-amber-600">[{constraintParts.join(', ')}]</span>
                    )}
                  </li>
                );
              })}
            </ul>
          </div>

          <div>
            <div className="flex items-center justify-between mb-1">
              <p className="text-xs font-medium text-muted-foreground">YAML Snippet</p>
              <Button variant="ghost" size="sm" className="h-6 px-2" onClick={handleCopy}>
                {copied ? (
                  <>
                    <Check className="h-3 w-3 mr-1 text-green-500" />
                    <span className="text-xs text-green-500">Copied!</span>
                  </>
                ) : (
                  <>
                    <Copy className="h-3 w-3 mr-1" />
                    <span className="text-xs">Copy</span>
                  </>
                )}
              </Button>
            </div>
            <pre className="text-xs bg-muted/50 p-2 rounded overflow-x-auto">
              <code>{signal.yaml_snippet}</code>
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

interface CategoryGroupProps {
  category: SignalCategory;
  signals: SignalDefinition[];
  onCopy: (snippet: string) => void;
  defaultExpanded?: boolean;
}

function CategoryGroup({ category, signals, onCopy, defaultExpanded = false }: CategoryGroupProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);

  if (signals.length === 0) return null;

  return (
    <div className="border rounded-md overflow-hidden">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 p-3 text-left bg-muted/30 hover:bg-muted/50 transition-colors"
      >
        {isExpanded ? <ChevronDown className="h-4 w-4 shrink-0" /> : <ChevronRight className="h-4 w-4 shrink-0" />}
        <span className="font-medium">{category.label}</span>
        <span className="text-xs text-muted-foreground ml-auto">{signals.length}</span>
      </button>

      {isExpanded && (
        <div className="divide-y divide-muted">
          {signals.map((signal) => (
            <SignalItem key={signal.key} signal={signal} onCopy={onCopy} />
          ))}
        </div>
      )}
    </div>
  );
}

interface SignalReferencePanelProps {
  onCopySnippet: (snippet: string) => void;
}

export function SignalReferencePanel({ onCopySnippet }: SignalReferencePanelProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const { data, isLoading, error } = useSignalReference();

  // Group signals by category
  const signalsByCategory = useMemo(() => {
    if (!data) return new Map<string, { category: SignalCategory; signals: SignalDefinition[] }>();
    const map = new Map<string, { category: SignalCategory; signals: SignalDefinition[] }>();
    for (const cat of data.categories) {
      map.set(cat.key, { category: cat, signals: [] });
    }
    for (const signal of data.signals) {
      const entry = map.get(signal.category);
      if (entry) {
        entry.signals.push(signal);
      } else {
        logger.warn(`Unknown signal category: ${signal.category} (signal: ${signal.key})`);
      }
    }
    return map;
  }, [data]);

  // Filter signals by search query
  const filteredByCategory = useMemo(() => {
    if (!searchQuery) return signalsByCategory;
    const lowerQuery = searchQuery.toLowerCase();
    const filtered = new Map<string, { category: SignalCategory; signals: SignalDefinition[] }>();
    for (const [key, entry] of signalsByCategory) {
      const matchingSignals = entry.signals.filter(
        (signal) =>
          signal.key.toLowerCase().includes(lowerQuery) ||
          signal.name.toLowerCase().includes(lowerQuery) ||
          signal.description.toLowerCase().includes(lowerQuery)
      );
      filtered.set(key, { category: entry.category, signals: matchingSignals });
    }
    return filtered;
  }, [signalsByCategory, searchQuery]);

  const totalFiltered = useMemo(() => {
    let count = 0;
    for (const entry of filteredByCategory.values()) {
      count += entry.signals.length;
    }
    return count;
  }, [filteredByCategory]);

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-sm text-muted-foreground">Loading signals...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center text-muted-foreground">
          <p className="text-sm">Failed to load signal reference</p>
          <p className="text-xs mt-1">{error instanceof Error ? error.message : 'Unknown error occurred'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <div className="p-3 border-b">
        <h3 className="font-medium mb-2">Signal Reference</h3>
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search signals..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-3 space-y-3">
        {Array.from(filteredByCategory.entries()).map(([key, entry]) => (
          <CategoryGroup
            key={key}
            category={entry.category}
            signals={entry.signals}
            onCopy={onCopySnippet}
            defaultExpanded={key === 'breakout'}
          />
        ))}

        {searchQuery && totalFiltered === 0 && (
          <div className="text-center text-muted-foreground py-8">
            <p className="text-sm">No signals found</p>
          </div>
        )}
      </div>
    </div>
  );
}
