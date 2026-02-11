import { createContext, useContext, useEffect, useState } from 'react';

type Theme = 'light' | 'dark' | 'system';

type ThemeProviderProps = {
  children: React.ReactNode;
  defaultTheme?: Theme;
};

type ThemeProviderState = {
  theme: Theme;
  setTheme: (theme: Theme) => void;
};

const initialState: ThemeProviderState = {
  theme: 'system',
  setTheme: () => null,
};

const ThemeProviderContext = createContext<ThemeProviderState>(initialState);

function isTheme(value: unknown): value is Theme {
  return value === 'light' || value === 'dark' || value === 'system';
}

function getStoredTheme(defaultTheme: Theme): Theme {
  if (typeof window === 'undefined') return defaultTheme;
  try {
    const storedTheme = localStorage.getItem('theme');
    return isTheme(storedTheme) ? storedTheme : defaultTheme;
  } catch {
    return defaultTheme;
  }
}

function resolveThemeClass(theme: Theme): 'light' | 'dark' {
  if (theme === 'system') {
    return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  return theme;
}

export function ThemeProvider({ children, defaultTheme = 'system', ...props }: ThemeProviderProps) {
  const [theme, setTheme] = useState<Theme>(() => getStoredTheme(defaultTheme));

  useEffect(() => {
    const root = window.document.documentElement;
    root.classList.remove('light', 'dark');
    root.classList.add(resolveThemeClass(theme));
  }, [theme]);

  useEffect(() => {
    try {
      localStorage.setItem('theme', theme);
    } catch {
      // localStorage can fail in private browsing or restricted environments.
    }
  }, [theme]);

  const value = {
    theme,
    setTheme: (theme: Theme) => {
      setTheme(theme);
    },
  };

  return (
    <ThemeProviderContext.Provider {...props} value={value}>
      {children}
    </ThemeProviderContext.Provider>
  );
}

export const useTheme = () => {
  const context = useContext(ThemeProviderContext);

  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }

  return context;
};
