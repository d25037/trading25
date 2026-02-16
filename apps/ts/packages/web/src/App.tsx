import { RouterProvider } from '@tanstack/react-router';
import { QueryProvider } from '@/providers/QueryProvider';
import { ThemeProvider } from '@/providers/ThemeProvider';
import { router } from '@/router';

function App() {
  return (
    <ThemeProvider defaultTheme="system">
      <QueryProvider>
        <RouterProvider router={router} />
      </QueryProvider>
    </ThemeProvider>
  );
}

export default App;
