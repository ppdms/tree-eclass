import { expect, within, userEvent, waitFor } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import {
  alternateDocumentName,
  alternatePdfTitle,
  sessionContentPath,
  sessionFixture,
  sessionInverseStatement,
  sessionPath,
} from '../fixtures/session';

export default { title: 'Workflows/Session', id: 'session', tags: ['ui-review'] };

function expectPdfCanvas(canvasElement: HTMLElement) {
  const page = canvasElement.querySelector<HTMLCanvasElement>('[data-page="1"] canvas');
  if (!page) throw new Error('The real PDF page canvas is not attached yet.');
  expect(page).toBeVisible();
  expect(page.width).toBeGreaterThan(0);
  expect(page.height).toBeGreaterThan(0);
  const context = page.getContext('2d');
  if (!context) throw new Error('The real PDF page has no 2D rendering context.');
  const pixels = context.getImageData(0, 0, page.width, Math.ceil(page.height / 4)).data;
  const hasPrintedText = pixels.some(
    (channel, index) => index % 4 === 0 && channel < 128 && (pixels[index + 3] ?? 0) > 0,
  );
  expect(hasPrintedText).toBe(true);
}

async function loadedPage(canvasElement: HTMLElement) {
  const canvas = within(canvasElement);
  await waitFor(
    () => {
      const textLayer = { selector: '[data-page="1"] span' };
      expect(canvas.getByText('Synthetic Linear Algebra Lecture', textLayer)).toBeVisible();
      expect(canvas.getByText('Order matters: AB need not equal BA.', textLayer)).toBeVisible();
      expect(canvas.getByText(sessionInverseStatement, textLayer)).toBeVisible();
      expect(canvas.queryByText('Page 1', textLayer)).toBeNull();
      expectPdfCanvas(canvasElement);
    },
    { timeout: 10_000 },
  );
  await expect(canvas.getByRole('spinbutton', { name: 'Go to page' })).toHaveValue(1);
  return canvas;
}

async function bookmarkPage(canvasElement: HTMLElement) {
  const canvas = within(canvasElement);
  await userEvent.click(canvas.getByRole('button', { name: 'Bookmark page' }));
  await expect(await canvas.findByRole('button', { name: 'Delete bookmark' })).toHaveAttribute('aria-pressed', 'true');
  await expect(await canvas.findByRole('img', { name: 'Bookmarked page' })).toBeVisible();
}

export const Reading = {
  name: 'Synthetic PDF reading, bookmark and finish',
  ...reviewed(
    {
      path: sessionPath,
      fixture: sessionFixture(),
      checkpoints: ['loaded-page', 'bookmarked', 'inspector', 'finished'],
    },
    async ({ canvasElement }) => {
      const canvas = await loadedPage(canvasElement);
      await checkpoint('loaded-page');
      await bookmarkPage(canvasElement);
      await checkpoint('bookmarked');
      await userEvent.click(canvas.getByRole('button', { name: 'Inspector' }));
      await expect(
        await canvas.findByText(
          'Synthetic lecture: matrix multiplication composes transformations; the order changes the result.',
        ),
      ).toBeVisible();
      await loadedPage(canvasElement);
      await checkpoint('inspector');
      await userEvent.click(canvas.getByRole('button', { name: 'Close panel' }));
      await loadedPage(canvasElement);
      const finish = canvas.getByRole('button', { name: 'Finish session' });
      await waitFor(() => expect(finish).toBeEnabled());
      await userEvent.click(finish);
      await userEvent.click(await within(document.body).findByRole('button', { name: 'Done' }));
      await expect(await canvas.findByRole('button', { name: 'Recorded' })).toBeVisible();
      await expect(await within(document.body).findByText('3 min recorded.')).toBeVisible();
      await checkpoint('finished');
    },
  ),
};

export const BookmarkRemoval = {
  name: 'Synthetic reader bookmark deletion',
  ...reviewed(
    { path: sessionPath, fixture: sessionFixture(), checkpoints: ['bookmarked', 'removed'] },
    async ({ canvasElement }) => {
      const canvas = await loadedPage(canvasElement);
      await bookmarkPage(canvasElement);
      await checkpoint('bookmarked');
      await userEvent.click(canvas.getByRole('button', { name: 'Delete bookmark' }));
      await expect(await canvas.findByRole('button', { name: 'Bookmark page' })).toHaveAttribute(
        'aria-pressed',
        'false',
      );
      await waitFor(() => expect(canvas.queryByRole('img', { name: 'Bookmarked page' })).toBeNull());
      await checkpoint('removed');
    },
  ),
};

export const DocumentSwitch = {
  name: 'Synthetic material switch during an active PDF render',
  ...reviewed(
    {
      path: sessionPath,
      fixture: sessionFixture('document-switch'),
      checkpoints: ['render-in-flight', 'switched-document', 'returned-document'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      const selector = await canvas.findByRole('combobox', { name: 'Choose document' });
      await waitFor(() => {
        const page = canvasElement.querySelector<HTMLCanvasElement>('[data-page="1"] canvas');
        expect(page?.width).toBeGreaterThan(0);
        expect(canvas.queryByText('Synthetic Linear Algebra Lecture', { selector: '[data-page="1"] span' })).toBeNull();
      });
      await checkpoint('render-in-flight');
      await userEvent.click(selector);
      await userEvent.click(await within(document.body).findByRole('option', { name: alternateDocumentName }));
      await expect(await canvas.findByText(alternatePdfTitle, { selector: '[data-page="1"] span' })).toBeVisible();
      await expect(selector).toHaveTextContent(alternateDocumentName);
      expectPdfCanvas(canvasElement);
      await checkpoint('switched-document');
      await userEvent.click(selector);
      await userEvent.click(await within(document.body).findByRole('option', { name: 'Matrices — synthetic lecture' }));
      await loadedPage(canvasElement);
      await expect(selector).toHaveTextContent('Matrices — synthetic lecture');
      await checkpoint('returned-document');
    },
  ),
};

export const Recall = {
  name: 'Synthetic source-linked recall and self-grading',
  ...reviewed(
    {
      path: sessionPath,
      fixture: sessionFixture(),
      checkpoints: ['recall-question', 'revealed-answer', 'recall-recorded'],
    },
    async ({ canvasElement }) => {
      const canvas = await loadedPage(canvasElement);
      await userEvent.click(canvas.getByRole('button', { name: 'Inspector' }));
      await userEvent.click(canvas.getByRole('tab', { name: 'Recall' }));
      await expect(await canvas.findByText('Why can AB differ from BA?')).toBeVisible();
      await userEvent.type(
        canvas.getByRole('textbox', { name: 'Your recall answer' }),
        'They apply transformations in different orders.',
      );
      await checkpoint('recall-question');
      await userEvent.click(canvas.getByRole('button', { name: 'Show the answer' }));
      await expect(
        await canvas.findByText('Matrix multiplication composes transformations in a specific order.'),
      ).toBeVisible();
      await checkpoint('revealed-answer');
      await userEvent.click(canvas.getByRole('button', { name: 'Got it' }));
      await expect(await canvas.findByText('Nothing due for this unit right now.')).toBeVisible();
      await checkpoint('recall-recorded');
    },
  ),
};

const workspaceLoading = sessionFixture('loading');
export const Loading = {
  name: 'Synthetic workspace context loading',
  ...reviewed(
    { path: sessionPath, fixture: workspaceLoading, checkpoints: ['loading', 'loaded-page'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Opening the study session…' })).toBeVisible();
      await checkpoint('loading');
      workspaceLoading.release();
      await loadedPage(canvasElement);
      await bookmarkPage(canvasElement);
      await checkpoint('loaded-page');
    },
  ),
};

const readerLoading = sessionFixture('reader-loading');
export const ReaderLoading = {
  name: 'Synthetic PDF content transport loading',
  ...reviewed(
    { path: sessionPath, fixture: readerLoading, checkpoints: ['reader-loading', 'loaded-page'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByText('Loading page 1…')).toBeVisible();
      await expect(canvas.getByText('Matrices — synthetic lecture', { selector: 'p' })).toBeVisible();
      await checkpoint('reader-loading');
      readerLoading.release();
      await loadedPage(canvasElement);
      await bookmarkPage(canvasElement);
      await checkpoint('loaded-page');
    },
  ),
};

export const ReaderRecovery = {
  name: 'Synthetic unavailable PDF storage and retry',
  ...reviewed(
    {
      path: sessionPath,
      fixture: sessionFixture('reader-recovery'),
      checkpoints: ['reader-error', 'recovered-page'],
      expectedHttpErrors: [{ method: 'GET', path: sessionContentPath, status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Unable to open this source' })).toBeVisible();
      await expect(canvas.getByRole('link', { name: 'Open source file' })).toHaveAttribute(
        'href',
        `${sessionContentPath}?course_id=101`,
      );
      await checkpoint('reader-error');
      await userEvent.click(canvas.getByRole('button', { name: 'Retry loading the document' }));
      await loadedPage(canvasElement);
      await bookmarkPage(canvasElement);
      await checkpoint('recovered-page');
    },
  ),
};

export const MissingSource = {
  name: 'Synthetic action with no attached source',
  ...reviewed(
    {
      path: '/study/session?course_id=101&action_id=action-101-matrix-recall',
      fixture: sessionFixture('empty-document'),
      checkpoints: ['missing-source', 'back-to-plan'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'This action has no document' })).toBeVisible();
      await checkpoint('missing-source');
      await userEvent.click(canvas.getByRole('link', { name: 'Back to the plan' }));
      await expect(await canvas.findByRole('link', { name: 'Start session' })).toBeVisible();
      await checkpoint('back-to-plan');
    },
  ),
};

export const ContextFailure = {
  name: 'Synthetic unavailable session context',
  ...reviewed(
    {
      path: sessionPath,
      fixture: sessionFixture('context-error'),
      checkpoints: ['context-error', 'back-to-plan'],
      expectedHttpErrors: [{ method: 'GET', path: '/api/study/session/context', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Unable to open this study session' })).toBeVisible();
      await checkpoint('context-error');
      await userEvent.click(canvas.getByRole('link', { name: 'Back to the plan' }));
      await expect(await canvas.findByRole('link', { name: 'Start session' })).toBeVisible();
      await checkpoint('back-to-plan');
    },
  ),
};
