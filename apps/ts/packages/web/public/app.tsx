import { Play, RefreshCw, Square } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Button } from './components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card';

interface EngineStatus {
  isRunning: boolean;
  activeStrategies: number;
  totalTrades: number;
  profitLoss: number;
}

function App() {
  const [status, setStatus] = useState<EngineStatus | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchStatus = useCallback(async () => {
    try {
      const response = await fetch('/api/status');
      const data = await response.json();
      setStatus(data);
    } catch (error) {
      console.error('Failed to fetch status:', error);
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleStart = async () => {
    setLoading(true);
    try {
      await fetch('/api/start', { method: 'POST' });
      await fetchStatus();
    } catch (error) {
      console.error('Failed to start engine:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async () => {
    setLoading(true);
    try {
      await fetch('/api/stop', { method: 'POST' });
      await fetchStatus();
    } catch (error) {
      console.error('Failed to stop engine:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <h1 className="text-4xl font-bold mb-8">Trading25</h1>

        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Engine Status</CardTitle>
            <CardDescription>Trading engine monitoring dashboard</CardDescription>
          </CardHeader>
          <CardContent>
            {status && (
              <div className="grid grid-cols-2 gap-4 mb-6">
                <div>
                  <p className="text-sm text-muted-foreground">Status</p>
                  <p className="text-xl font-medium">
                    {status.isRunning ? (
                      <span className="text-green-600">Running</span>
                    ) : (
                      <span className="text-red-600">Stopped</span>
                    )}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Active Strategies</p>
                  <p className="text-xl font-medium">{status.activeStrategies}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total Trades</p>
                  <p className="text-xl font-medium">{status.totalTrades}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">P&L</p>
                  <p className={`text-xl font-medium ${status.profitLoss >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    ¥{status.profitLoss.toLocaleString()}
                  </p>
                </div>
              </div>
            )}

            <div className="flex gap-4">
              <Button onClick={handleStart} disabled={loading || status?.isRunning} variant="default">
                <Play className="w-4 h-4 mr-2" />
                Start Engine
              </Button>
              <Button onClick={handleStop} disabled={loading || !status?.isRunning} variant="destructive">
                <Square className="w-4 h-4 mr-2" />
                Stop Engine
              </Button>
              <Button onClick={fetchStatus} disabled={loading} variant="outline">
                <RefreshCw className="w-4 h-4 mr-2" />
                Refresh
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
