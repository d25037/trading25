import { BarChart3, Bell, Briefcase, FlaskConical, LineChart, Settings, TrendingUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from '@/components/ui/theme-toggle';
import { cn } from '@/lib/utils';
import { useUiStore } from '@/stores/uiStore';

const navigationItems = [
  { id: 'charts', label: 'Charts', icon: LineChart },
  { id: 'portfolio', label: 'Portfolio', icon: Briefcase },
  { id: 'indices', label: 'Indices', icon: TrendingUp },
  { id: 'analysis', label: 'Analysis', icon: BarChart3 },
  { id: 'backtest', label: 'Backtest', icon: FlaskConical },
  { id: 'settings', label: 'Settings', icon: Settings },
];

export function Header() {
  const { activeTab, setActiveTab } = useUiStore();

  return (
    <header
      className={cn('relative flex h-14 items-center justify-between border-b border-border/50 px-4', 'glass-panel')}
    >
      {/* Gradient background */}
      <div className="absolute inset-0 gradient-secondary opacity-30" />

      <div className="relative z-10 flex items-center gap-6">
        {/* Logo */}
        <div className="flex items-center gap-2">
          <div className="gradient-primary rounded-lg p-1.5">
            <TrendingUp className="h-5 w-5 text-white" />
          </div>
          <h1 className="text-lg font-bold bg-gradient-to-r from-primary to-primary/70 bg-clip-text text-transparent">
            Trading25
          </h1>
        </div>

        {/* Navigation */}
        <nav className="flex items-center gap-1">
          {navigationItems.map((item) => {
            const Icon = item.icon;
            const isActive = activeTab === item.id;

            return (
              <button
                key={item.id}
                type="button"
                onClick={() => setActiveTab(item.id)}
                className={cn(
                  'flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-all duration-200',
                  'hover:scale-[1.02] active:scale-[0.98]',
                  isActive
                    ? 'gradient-primary text-white shadow-md shadow-primary/20'
                    : 'text-muted-foreground hover:bg-accent/50 hover:text-accent-foreground'
                )}
              >
                <Icon className="h-4 w-4" />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
      </div>

      <div className="relative z-10 flex items-center gap-3">
        {/* Action buttons */}
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 rounded-lg hover:bg-accent/50 transition-all duration-200"
          >
            <Bell className="h-4 w-4" />
          </Button>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
