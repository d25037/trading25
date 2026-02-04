import { Loader2 } from 'lucide-react';
import type { ReactNode } from 'react';

interface DataStateWrapperProps {
  isLoading: boolean;
  error?: Error | null;
  isEmpty?: boolean;
  emptyMessage?: string;
  emptySubMessage?: string;
  emptyIcon?: ReactNode;
  loadingMessage?: string;
  height?: string;
  children: ReactNode;
}

/**
 * Loading state component
 */
function LoadingState({ height, message }: { height: string; message?: string }): ReactNode {
  return (
    <div className={`flex items-center justify-center ${height}`}>
      <div className="text-center">
        <Loader2 className="h-6 w-6 animate-spin mx-auto text-muted-foreground" />
        {message && <p className="text-sm text-muted-foreground mt-2">{message}</p>}
      </div>
    </div>
  );
}

/**
 * Error state component
 */
function ErrorState({ height, message }: { height: string; message: string }): ReactNode {
  return (
    <div className={`flex items-center justify-center ${height} text-destructive text-sm p-4 text-center`}>
      {message}
    </div>
  );
}

/**
 * Empty state component
 */
function EmptyState({
  height,
  message,
  subMessage,
  icon,
}: {
  height: string;
  message: string;
  subMessage?: string;
  icon?: ReactNode;
}): ReactNode {
  return (
    <div className={`flex flex-col items-center justify-center ${height} text-muted-foreground`}>
      {icon && <div className="mb-2 opacity-50">{icon}</div>}
      <p className="text-sm">{message}</p>
      {subMessage && <p className="text-xs mt-1">{subMessage}</p>}
    </div>
  );
}

/**
 * Wrapper component for handling loading, error, and empty states
 */
export function DataStateWrapper({
  isLoading,
  error,
  isEmpty = false,
  emptyMessage = 'No data available',
  emptySubMessage,
  emptyIcon,
  loadingMessage,
  height = 'h-32',
  children,
}: DataStateWrapperProps): ReactNode {
  if (isLoading) {
    return <LoadingState height={height} message={loadingMessage} />;
  }

  if (error) {
    return <ErrorState height={height} message={error.message} />;
  }

  if (isEmpty) {
    return <EmptyState height={height} message={emptyMessage} subMessage={emptySubMessage} icon={emptyIcon} />;
  }

  return <>{children}</>;
}
