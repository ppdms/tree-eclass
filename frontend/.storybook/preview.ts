import type { Preview } from '@storybook/react-vite';
import { prepareScenario } from '../review/prepare';
import '../src/styles/entry.css';

const preview: Preview = {
  parameters: { layout: 'fullscreen', controls: { disable: true } },
  loaders: [prepareScenario],
};

export default preview;
