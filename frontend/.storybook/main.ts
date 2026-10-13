import type { StorybookConfig } from '@storybook/react-vite';
import { resolve } from 'node:path';

const config: StorybookConfig = {
  framework: '@storybook/react-vite',
  stories: ['../review/stories/*.stories.ts'],
  staticDirs: ['../public', '../.ui-review/worker'],
  core: { disableTelemetry: true },
  previewHead: (head) => `${head}\n<link rel="icon" href="/favicon/favicon.ico">`,
  async viteFinal(config) {
    // Storybook loads the production Vite config, including its StyleX compiler.
    // Only the native application's atomic index publisher is not a catalog plugin.
    const plugins = config.plugins?.filter((plugin) => !plugin || !('name' in plugin) || plugin.name !== 'tree-entry');
    return {
      ...config,
      plugins,
      resolve: {
        ...config.resolve,
        alias: [
          { find: '@/app/navigation', replacement: resolve(import.meta.dirname, '../review/navigation.ts') },
          { find: '@', replacement: resolve(import.meta.dirname, '../src') },
        ],
      },
      build: { ...config.build, sourcemap: true },
    };
  },
};

export default config;
