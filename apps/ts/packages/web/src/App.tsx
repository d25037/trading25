import { MainLayout } from '@/components/Layout/MainLayout';
import { useHistorySync } from '@/hooks/useHistorySync';
import { AnalysisPage } from '@/pages/AnalysisPage';
import { BacktestPage } from '@/pages/BacktestPage';
import { ChartsPage } from '@/pages/ChartsPage';
import { HistoryPage } from '@/pages/HistoryPage';
import { IndicesPage } from '@/pages/IndicesPage';
import { PortfolioPage } from '@/pages/PortfolioPage';
import { SettingsPage } from '@/pages/SettingsPage';
import { QueryProvider } from '@/providers/QueryProvider';
import { ThemeProvider } from '@/providers/ThemeProvider';
import { useUiStore } from '@/stores/uiStore';

function AppContent() {
  const { activeTab } = useUiStore();

  // Sync browser history with app state
  useHistorySync();

  const renderActivePage = () => {
    switch (activeTab) {
      case 'charts':
        return <ChartsPage />;
      case 'portfolio':
        return <PortfolioPage />;
      case 'indices':
        return <IndicesPage />;
      case 'analysis':
        return <AnalysisPage />;
      case 'backtest':
        return <BacktestPage />;
      case 'history':
        return <HistoryPage />;
      case 'settings':
        return <SettingsPage />;
      default:
        return <ChartsPage />;
    }
  };

  return <MainLayout>{renderActivePage()}</MainLayout>;
}

function App() {
  return (
    <ThemeProvider defaultTheme="system">
      <QueryProvider>
        <AppContent />
      </QueryProvider>
    </ThemeProvider>
  );
}

export default App;
