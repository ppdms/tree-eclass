import { createElement } from 'react';
import type { HttpHandler } from 'msw';
import type { StoryContext } from '@storybook/react-vite';
import { ReviewApplication, applicationGeneration } from './application';
import { currentReview, type ExpectedDownload, type ExpectedHttpError, type ExpectedNetworkError } from './state';

export interface ReviewFixture {
  id: string;
  handlers: () => HttpHandler[];
}

export interface ReviewSpec {
  path: string;
  fixture: ReviewFixture;
  checkpoints: string[];
  theme?: 'dark' | 'light' | 'eink';
  expectedHttpErrors?: ExpectedHttpError[];
  expectedNetworkErrors?: ExpectedNetworkError[];
  downloads?: ExpectedDownload[];
}

export async function checkpoint(name: string) {
  const state = currentReview();
  if (!state.requiredCheckpoints.includes(name) || state.checkpoints.includes(name)) {
    throw new Error(`Unexpected or repeated checkpoint: ${name}`);
  }
  await document.fonts.ready;
  await window.__treeCaptureCheckpoint?.(name);
  state.checkpoints.push(name);
}

export function reviewed(spec: ReviewSpec, interaction: (context: StoryContext) => Promise<void>) {
  return {
    render: () => createElement(ReviewApplication, { key: applicationGeneration() }),
    tags: ['synthetic-fixture'],
    parameters: {
      review: spec,
      docs: { description: { story: `Offline synthetic fixture: ${spec.fixture.id}. No live data or services.` } },
    },
    async play(context: StoryContext) {
      const state = currentReview();
      try {
        await interaction(context);
        const missing = spec.checkpoints.filter((name) => !state.checkpoints.includes(name));
        if (missing.length) throw new Error(`Missing checkpoints: ${missing.join(', ')}`);
        if (state.errors.length) throw new Error(state.errors.join('\n'));
        state.status = 'passed';
      } catch (error) {
        state.status = 'failed';
        state.errors.push(error instanceof Error ? error.message : String(error));
        throw error;
      }
    },
  };
}
