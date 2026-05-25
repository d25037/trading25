import { useEffect, useState } from 'react';

export function useElapsedSeconds(isActive: boolean, startTime?: string | null): number {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (!isActive || !startTime) {
      setElapsed(0);
      return;
    }

    const start = new Date(startTime).getTime();
    if (Number.isNaN(start)) {
      setElapsed(0);
      return;
    }

    const update = () => setElapsed(Math.max(0, Math.floor((Date.now() - start) / 1000)));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [isActive, startTime]);

  return elapsed;
}
