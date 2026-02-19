import { Loader2 } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { KeyboardEvent, RefObject } from 'react';
import { Input } from '@/components/ui/input';
import { type StockSearchResultItem, useStockSearch } from '@/hooks/useStockSearch';
import { cn } from '@/lib/utils';

interface StockSearchInputProps {
  value: string;
  onValueChange: (value: string) => void;
  onSelect: (stock: StockSearchResultItem) => void;
  id?: string;
  name?: string;
  placeholder?: string;
  required?: boolean;
  autoFocus?: boolean;
  disabled?: boolean;
  className?: string;
  maxLength?: number;
  searchLimit?: number;
}

interface SearchSuggestionsProps {
  containerRef: RefObject<HTMLDivElement | null>;
  results: StockSearchResultItem[];
  selectedIndex: number;
  onSelect: (stock: StockSearchResultItem) => void;
}

function SearchSuggestions({ containerRef, results, selectedIndex, onSelect }: SearchSuggestionsProps) {
  return (
    <div
      ref={containerRef}
      className="absolute left-0 top-full z-50 mt-1 w-full max-h-96 overflow-auto rounded-lg border border-border/50 bg-background/95 backdrop-blur-md shadow-xl"
    >
      {results.map((stock, index) => (
        <button
          key={stock.code}
          type="button"
          onClick={() => onSelect(stock)}
          className={cn(
            'w-full px-4 py-3 text-left hover:bg-accent/50 transition-colors',
            'border-b border-border/30 last:border-b-0',
            index === selectedIndex && 'bg-accent/50'
          )}
          aria-label={`${stock.code} ${stock.companyName}`}
        >
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-3 min-w-0">
              <span className="font-mono font-bold text-primary text-base">{stock.code}</span>
              <span className="text-sm text-foreground truncate">{stock.companyName}</span>
            </div>
            <span className="text-xs text-muted-foreground whitespace-nowrap">{stock.marketName}</span>
          </div>
          <div className="text-xs text-muted-foreground mt-1">{stock.sector33Name}</div>
        </button>
      ))}
    </div>
  );
}

export function StockSearchInput({
  value,
  onValueChange,
  onSelect,
  id,
  name = 'stock-search',
  placeholder = '銘柄コードまたは会社名で検索...',
  required = false,
  autoFocus = false,
  disabled = false,
  className,
  maxLength,
  searchLimit = 50,
}: StockSearchInputProps) {
  const [debouncedQuery, setDebouncedQuery] = useState(value);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(value), 300);
    return () => clearTimeout(timer);
  }, [value]);

  const { data: searchResults, isLoading: isSearching } = useStockSearch(debouncedQuery, {
    limit: searchLimit,
    enabled: debouncedQuery.trim().length >= 1,
  });
  const searchCandidates = searchResults?.results ?? [];

  const closeSuggestions = useCallback(() => {
    setShowSuggestions(false);
    setSelectedIndex(-1);
  }, []);

  useEffect(() => {
    if (selectedIndex >= 0 && suggestionsRef.current) {
      const selectedElement = suggestionsRef.current.children[selectedIndex] as HTMLElement;
      selectedElement?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, [selectedIndex]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(event.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(event.target as Node)
      ) {
        closeSuggestions();
      }
    }

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [closeSuggestions]);

  const handleSelectStock = useCallback(
    (stock: StockSearchResultItem) => {
      onSelect(stock);
      closeSuggestions();
    },
    [closeSuggestions, onSelect]
  );

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Escape') {
      closeSuggestions();
      return;
    }

    if (!showSuggestions || searchCandidates.length === 0) {
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setSelectedIndex((prev) => Math.min(prev + 1, searchCandidates.length - 1));
        break;
      case 'ArrowUp':
        e.preventDefault();
        setSelectedIndex((prev) => Math.max(prev - 1, -1));
        break;
      case 'Enter':
        if (selectedIndex >= 0 && searchCandidates[selectedIndex]) {
          e.preventDefault();
          handleSelectStock(searchCandidates[selectedIndex]);
        }
        break;
    }
  };

  return (
    <div className="relative">
      <Input
        ref={inputRef}
        id={id}
        type="search"
        name={name}
        placeholder={placeholder}
        value={value}
        onChange={(e) => {
          onValueChange(e.target.value);
          setShowSuggestions(true);
          setSelectedIndex(-1);
        }}
        onFocus={() => setShowSuggestions(true)}
        onKeyDown={handleKeyDown}
        className={cn('w-full pr-10', className)}
        required={required}
        autoFocus={autoFocus}
        maxLength={maxLength}
        disabled={disabled}
        autoComplete="off"
        autoCapitalize="off"
        autoCorrect="off"
        spellCheck={false}
        inputMode="search"
        enterKeyHint="search"
        data-form-type="other"
        data-lpignore="true"
        data-1p-ignore="true"
      />
      {isSearching && (
        <div className="absolute right-3 top-1/2 -translate-y-1/2">
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        </div>
      )}
      {showSuggestions && searchCandidates.length > 0 && (
        <SearchSuggestions
          containerRef={suggestionsRef}
          results={searchCandidates}
          selectedIndex={selectedIndex}
          onSelect={handleSelectStock}
        />
      )}
    </div>
  );
}
