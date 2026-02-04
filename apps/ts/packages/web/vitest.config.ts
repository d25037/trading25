import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [react()],
	test: {
		globals: true,
		environment: 'happy-dom',
		include: ['src/**/*.{test,spec}.{js,ts,tsx}'],
		exclude: ['node_modules', 'dist'],
		setupFiles: ['./src/test-setup.ts'],
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html', 'lcov'],
			include: ['src/**/*.{ts,tsx}'],
			exclude: [
				'src/**/*.test.{ts,tsx}',
				'src/**/*.spec.{ts,tsx}',
				'src/types/**',
				'src/lib/constants.ts',
				'src/hooks/useTheme.ts',
				'src/test-utils.tsx',
				'src/test-setup.ts',
				'src/main.tsx',
				'src/vite-env.d.ts',
				'src/App.tsx',
				'src/providers/**',
				'src/components/ui/**',
				'src/components/Editor/**',
				'src/**/index.ts',
				'src/components/shared/filters/**',
			],
		},
	},
	resolve: {
		alias: {
			'@': `${import.meta.dirname}/src`,
		},
	},
});
