import type { ReactNode } from 'react';
import { Header } from './Header';

interface MainLayoutProps {
  children: ReactNode;
}

export function MainLayout({ children }: MainLayoutProps) {
  return (
    <div className="flex h-screen min-h-0 flex-col bg-background">
      <Header />
      <main className="flex min-h-0 flex-1 overflow-auto bg-gradient-to-br from-background via-background to-muted/20">
        {children}
      </main>
    </div>
  );
}
