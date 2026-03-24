import { useNavigate, useRouterState } from '@tanstack/react-router';
import { BarChart3, Bell, Briefcase, Database, FlaskConical, LineChart, TrendingUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from '@/components/ui/theme-toggle';
import { cn } from '@/lib/utils';

const navigationItems = [
  { path: '/charts', label: 'Charts', icon: LineChart },
  { path: '/portfolio', label: 'Portfolio', icon: Briefcase },
  { path: '/indices', label: 'Indices', icon: TrendingUp },
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
    <header className="sticky top-0 z-20 border-b border-border/70 bg-background/88 backdrop-blur-xl">
      <div className="flex h-14 items-center justify-between gap-4 px-4">
        <div className="flex min-w-0 items-center gap-5">
          <button
            type="button"
            onClick={() => {
              void navigate({ to: '/charts' });
            }}
            className="flex items-center gap-2.5 rounded-lg px-1 py-1 transition-colors hover:bg-accent/50"
          >
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary text-primary-foreground shadow-sm">
              <TrendingUp className="h-4 w-4" />
            </div>
            <h1 className="text-base font-semibold tracking-tight text-foreground">Trading25</h1>
          </button>

          <nav className="flex min-w-0 items-center gap-1 overflow-x-auto">
            {navigationItems.map((item) => {
              const Icon = item.icon;
              const isActive = pathname === item.path || (item.path === '/market-db' && pathname === '/settings');

              return (
                <button
                  key={item.path}
                  type="button"
                  onClick={() => {
                    void navigate({ to: item.path });
                  }}
                  className={cn(
                    'flex items-center gap-2 rounded-lg border px-3 py-2 text-sm font-medium transition-colors duration-200',
                    isActive
                      ? 'border-primary/25 bg-primary/12 text-primary shadow-sm'
                      : 'border-transparent text-muted-foreground hover:border-border/70 hover:bg-accent/50 hover:text-accent-foreground'
                  )}
                >
                  <Icon className="h-4 w-4" />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" className="h-8 w-8 rounded-lg hover:bg-accent/50">
            <Bell className="h-4 w-4" />
          </Button>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
