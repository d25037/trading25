import { useNavigate, useRouterState } from '@tanstack/react-router';
import { BarChart3, Briefcase, Database, FileSearch, FlaskConical, LineChart, TrendingUp } from 'lucide-react';
import { ThemeToggle } from '@/components/ui/theme-toggle';
import { cn } from '@/lib/utils';

const navigationItems = [
  { path: '/charts', label: 'Charts', icon: LineChart },
  { path: '/portfolio', label: 'Portfolio', icon: Briefcase },
  { path: '/indices', label: 'Indices', icon: TrendingUp },
  { path: '/research', label: 'Research', icon: FileSearch },
  { path: '/options-225', label: 'N225 Options', icon: LineChart },
  { path: '/screening', label: 'Screening', icon: BarChart3 },
  { path: '/ranking', label: 'Ranking', icon: BarChart3 },
  { path: '/backtest', label: 'Backtest', icon: FlaskConical },
  { path: '/market-db', label: 'Market DB', icon: Database },
];

export function Header() {
  const navigate = useNavigate();
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  return (
    <header className="sticky top-0 z-20 border-b border-border/70 bg-background/92 backdrop-blur-xl">
      <div className="flex h-16 items-center gap-4 px-4 lg:px-6">
        <div className="flex min-w-0 items-center gap-4">
          <button
            type="button"
            onClick={() => {
              void navigate({ to: '/charts' });
            }}
            className="app-interactive flex items-center gap-3 rounded-xl px-2 py-1.5 text-left hover:bg-[var(--app-surface-muted)]"
          >
            <div className="app-panel-emphasis flex h-9 w-9 items-center justify-center rounded-xl text-primary shadow-none">
              <TrendingUp className="h-4 w-4" />
            </div>
            <div className="min-w-0">
              <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                Trading Workspace
              </p>
              <h1 className="truncate text-sm font-semibold tracking-tight text-foreground">Trading25</h1>
            </div>
          </button>

          <nav className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto border-l border-border/70 pl-4">
            {navigationItems.map((item) => {
              const Icon = item.icon;
              const isActive =
                pathname === item.path ||
                pathname.startsWith(`${item.path}/`) ||
                (item.path === '/market-db' && pathname === '/settings');

              return (
                <button
                  key={item.path}
                  type="button"
                  aria-pressed={isActive}
                  data-state={isActive ? 'active' : 'inactive'}
                  onClick={() => {
                    void navigate({ to: item.path });
                  }}
                  className={cn(
                    'app-interactive flex shrink-0 items-center gap-2 rounded-xl px-3 py-2 text-sm font-medium',
                    isActive
                      ? 'bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                      : 'text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground'
                  )}
                >
                  <Icon className="h-4 w-4" />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>
        </div>

        <div className="app-panel-muted flex items-center rounded-xl p-1">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
