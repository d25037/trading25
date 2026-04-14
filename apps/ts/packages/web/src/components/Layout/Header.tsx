import { useEffect, useRef, useState } from 'react';
import { Link, useRouterState } from '@tanstack/react-router';
import {
  BarChart3,
  Briefcase,
  ChevronDown,
  Database,
  FileSearch,
  FlaskConical,
  LineChart,
  type LucideIcon,
  MoreHorizontal,
  TrendingUp,
} from 'lucide-react';
import { ThemeToggle } from '@/components/ui/theme-toggle';
import { cn } from '@/lib/utils';

interface NavigationItem {
  path: string;
  label: string;
  icon: LucideIcon;
}

const navigationItems: NavigationItem[] = [
  { path: '/symbol-workbench', label: 'Symbol Workbench', icon: LineChart },
  { path: '/portfolio', label: 'Portfolio', icon: Briefcase },
  { path: '/indices', label: 'Indices', icon: TrendingUp },
  { path: '/research', label: 'Research', icon: FileSearch },
  { path: '/options-225', label: 'N225 Options', icon: LineChart },
  { path: '/screening', label: 'Screening', icon: BarChart3 },
  { path: '/ranking', label: 'Ranking', icon: BarChart3 },
  { path: '/backtest', label: 'Backtest', icon: FlaskConical },
  { path: '/market-db', label: 'Market DB', icon: Database },
];

const primaryNavigationPaths = new Set([
  '/symbol-workbench',
  '/portfolio',
  '/indices',
  '/screening',
  '/ranking',
  '/backtest',
]);
const moreMenuId = 'global-header-overflow-nav';

const primaryNavigationItems = navigationItems.filter((item) => primaryNavigationPaths.has(item.path));
const overflowNavigationItems = navigationItems.filter((item) => !primaryNavigationPaths.has(item.path));

function isNavigationItemActive(item: NavigationItem, pathname: string): boolean {
  return (
    pathname === item.path ||
    pathname.startsWith(`${item.path}/`) ||
    (item.path === '/market-db' && pathname === '/settings')
  );
}

function getNavigationItemClasses(isActive: boolean): string {
  return cn(
    'app-interactive flex shrink-0 items-center gap-1.5 rounded-xl px-2.5 py-2 text-sm font-medium',
    isActive
      ? 'bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
      : 'text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground'
  );
}

export function Header() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const [isMoreMenuOpen, setIsMoreMenuOpen] = useState(false);
  const moreMenuRef = useRef<HTMLDivElement | null>(null);
  const previousPathnameRef = useRef(pathname);

  const activeOverflowItem = overflowNavigationItems.find((item) => isNavigationItemActive(item, pathname));
  const MoreMenuIcon = activeOverflowItem?.icon ?? MoreHorizontal;

  useEffect(() => {
    if (previousPathnameRef.current === pathname) {
      return;
    }

    previousPathnameRef.current = pathname;
    setIsMoreMenuOpen(false);
  }, [pathname]);

  useEffect(() => {
    if (!isMoreMenuOpen) {
      return;
    }

    const handlePointerDown = (event: PointerEvent) => {
      if (!moreMenuRef.current?.contains(event.target as Node)) {
        setIsMoreMenuOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsMoreMenuOpen(false);
      }
    };

    document.addEventListener('pointerdown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);

    return () => {
      document.removeEventListener('pointerdown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isMoreMenuOpen]);

  return (
    <header className="sticky top-0 z-20 border-b border-border/70 bg-background/92 backdrop-blur-xl">
      <div className="mx-auto flex h-16 w-full max-w-[1180px] items-center gap-2 px-3 sm:px-4 lg:px-5">
        <div className="flex min-w-0 flex-1 items-center gap-2 sm:gap-3">
          <Link
            to="/symbol-workbench"
            className="app-interactive flex shrink-0 items-center gap-2 rounded-xl px-2 py-1.5 text-left hover:bg-[var(--app-surface-muted)]"
          >
            <div className="app-panel-emphasis flex h-9 w-9 items-center justify-center rounded-xl text-primary shadow-none">
              <TrendingUp className="h-4 w-4" />
            </div>
            <div className="min-w-0">
              <p className="hidden text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground xl:block">
                Trading Workspace
              </p>
              <h1 className="truncate text-sm font-semibold tracking-tight text-foreground">Trading25</h1>
            </div>
          </Link>

          <div className="flex min-w-0 flex-1 items-center gap-1.5 border-l border-border/70 pl-2 sm:pl-3">
            <nav className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto">
              {primaryNavigationItems.map((item) => {
                const Icon = item.icon;
                const isActive = isNavigationItemActive(item, pathname);

                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    aria-current={isActive ? 'page' : undefined}
                    data-state={isActive ? 'active' : 'inactive'}
                    className={getNavigationItemClasses(isActive)}
                  >
                    <Icon className="hidden h-4 w-4 xl:block" />
                    <span>{item.label}</span>
                  </Link>
                );
              })}
            </nav>

            <div ref={moreMenuRef} className="relative shrink-0">
              <button
                type="button"
                aria-controls={moreMenuId}
                aria-expanded={isMoreMenuOpen}
                aria-haspopup="true"
                data-state={activeOverflowItem ? 'active' : 'inactive'}
                onClick={() => {
                  setIsMoreMenuOpen((current) => !current);
                }}
                className={cn(
                  'app-interactive flex items-center gap-1.5 rounded-xl px-2.5 py-2 text-sm font-medium',
                  activeOverflowItem
                    ? 'bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                    : 'text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground'
                )}
              >
                <MoreMenuIcon className="h-4 w-4" />
                <span>{activeOverflowItem?.label ?? 'More'}</span>
                <ChevronDown
                  className={cn('h-4 w-4 transition-transform duration-150', isMoreMenuOpen && 'rotate-180')}
                />
              </button>

              {isMoreMenuOpen ? (
                <div
                  id={moreMenuId}
                  className="absolute right-0 top-full z-30 mt-2 w-56 rounded-2xl border border-border/70 bg-background/98 p-2 shadow-lg backdrop-blur-xl"
                >
                  {overflowNavigationItems.map((item) => {
                    const Icon = item.icon;
                    const isActive = isNavigationItemActive(item, pathname);

                    return (
                      <Link
                        key={item.path}
                        to={item.path}
                        aria-current={isActive ? 'page' : undefined}
                        data-state={isActive ? 'active' : 'inactive'}
                        onClick={() => {
                          setIsMoreMenuOpen(false);
                        }}
                        className={cn(
                          getNavigationItemClasses(isActive),
                          'w-full text-left',
                        )}
                      >
                        <Icon className="h-4 w-4 shrink-0" />
                        <span>{item.label}</span>
                      </Link>
                    );
                  })}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="app-panel-muted flex items-center rounded-xl p-1">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
