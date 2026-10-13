import { expect, within, userEvent, waitFor } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { askFixture, matrixAnswer, matrixQuestion } from '../fixtures/ask';

export default { title: 'Workflows/Ask', id: 'ask', tags: ['ui-review'] };

async function expectMatrixAnswer(canvasElement: HTMLElement) {
  const canvas = within(canvasElement);
  await waitFor(() => {
    const answer = within(canvas.getByRole('article', { name: 'Answer' }));
    expect(answer.getByText(matrixAnswer)).toBeVisible();
  });
}

async function savedConversationLink(canvasElement: HTMLElement) {
  const history = within(within(canvasElement).getByRole('navigation', { name: 'Saved conversations' }));
  const link = await history.findByRole('link', { name: /^Matrix inverses\b/ });
  await expect(link).toBeVisible();
  await expect(within(link).getByText('Matrix inverses')).toBeVisible();
  await expect(link).toHaveTextContent(matrixAnswer);
  await expect(link).toHaveAttribute('href', '/ask?c=701');
  return link;
}

const streaming = askFixture('new', true);
export const SourceBackedAnswer = {
  name: 'Synthetic source-backed conversation',
  ...reviewed(
    { path: '/ask', fixture: streaming, checkpoints: ['empty', 'looking-up', 'answer'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'What are you working through?' })).toBeVisible();
      await expect(canvas.getByRole('button', { name: 'Send question' })).toBeDisabled();
      await checkpoint('empty');
      await userEvent.type(canvas.getByRole('textbox', { name: 'Question' }), matrixQuestion);
      await userEvent.click(canvas.getByRole('button', { name: 'Send question' }));
      await expect(await canvas.findByText('Reading source material')).toBeVisible();
      await expect(canvas.getByRole('button', { name: 'Stop response' })).toBeEnabled();
      await checkpoint('looking-up');
      streaming.release();
      await expectMatrixAnswer(canvasElement);
      const link = canvas.getByRole('link', { name: /official Matrices.pdf p. 1/ });
      await expect(link).toBeVisible();
      await expect(link).toHaveAttribute(
        'href',
        '/study/session?course_id=101&document_id=document-101-matrices&page=1',
      );
      await savedConversationLink(canvasElement);
      await waitFor(() => expect(canvas.getByRole('textbox', { name: 'Question' })).toHaveValue(''));
      await checkpoint('answer');
    },
  ),
};

export const SavedConversation = {
  name: 'Synthetic saved conversation and follow-up',
  ...reviewed(
    { path: '/ask?c=701', fixture: askFixture('saved'), checkpoints: ['restored', 'filtered', 'follow-up'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expectMatrixAnswer(canvasElement);
      await expect(await savedConversationLink(canvasElement)).toHaveAttribute('aria-current', 'page');
      await checkpoint('restored');
      const search = canvas.getByRole('searchbox');
      await userEvent.type(search, 'no matching synthetic title');
      await expect(canvas.getByText('No conversations match that search.')).toBeVisible();
      await checkpoint('filtered');
      await userEvent.clear(search);
      await userEvent.type(canvas.getByRole('textbox', { name: 'Question' }), 'How does row reduction help?{Enter}');
      await expect(
        await canvas.findByText(
          'Row reduction preserves the solution set; a pivot in every column confirms invertibility.',
        ),
      ).toBeVisible();
      await expect(canvas.getAllByRole('article', { name: 'Answer' })).toHaveLength(2);
      await checkpoint('follow-up');
    },
  ),
};

export const ProviderUnavailable = {
  name: 'Synthetic unavailable provider',
  ...reviewed(
    { path: '/ask', fixture: askFixture('offline'), checkpoints: ['offline', 'setup-details'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByText('Ask is offline — no AI provider is connected.')).toBeVisible();
      await expect(canvas.getByRole('textbox', { name: 'Question' })).toBeDisabled();
      await expect(canvas.getByRole('link', { name: 'Configure Ask in Settings' })).toHaveAttribute(
        'href',
        '/settings#ai',
      );
      await checkpoint('offline');
      await userEvent.click(canvas.getByText('Show setup details'));
      await expect(
        canvas.getByText('Ask needs a configured chat provider before it can answer questions.'),
      ).toBeVisible();
      await checkpoint('setup-details');
    },
  ),
};

export const ProviderFailureRecovery = {
  name: 'Synthetic provider HTTP failure and recovery',
  ...reviewed(
    {
      path: '/ask',
      fixture: askFixture('http-error'),
      checkpoints: ['provider-error', 'recovered'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/ask/stream', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.type(await canvas.findByRole('textbox', { name: 'Question' }), `${matrixQuestion}{Enter}`);
      await expect(await canvas.findByRole('alert')).toHaveTextContent('No answer model is currently available.');
      await checkpoint('provider-error');
      await userEvent.click(canvas.getByRole('button', { name: 'Try again' }));
      await expectMatrixAnswer(canvasElement);
      await waitFor(() => expect(canvas.queryByRole('alert')).not.toBeInTheDocument());
      await expect(canvas.getByRole('link', { name: /official Matrices.pdf p. 1/ })).toBeVisible();
      await checkpoint('recovered');
    },
  ),
};

export const StreamFailureRecovery = {
  name: 'Synthetic streaming failure and retry',
  ...reviewed(
    {
      path: '/ask',
      fixture: askFixture('stream-error'),
      checkpoints: ['stream-error', 'recovered'],
      expectedNetworkErrors: [
        {
          method: 'POST',
          path: '/api/ask/stream',
          errorText: 'net::ERR_ABORTED',
          count: 1,
          requestOrdinal: 1,
          responseStatus: 200,
          responseContentType: 'text/event-stream',
          responseHeader: { name: 'x-vercel-ai-ui-message-stream', value: 'v1' },
          failedCheckpoint: 'stream-error',
          recoveredCheckpoint: 'recovered',
        },
      ],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.type(await canvas.findByRole('textbox', { name: 'Question' }), `${matrixQuestion}{Enter}`);
      await expect(await canvas.findByRole('alert')).toHaveTextContent(
        'The answer model could not complete this question.',
      );
      await checkpoint('stream-error');
      await userEvent.click(canvas.getByRole('button', { name: 'Try again' }));
      await expectMatrixAnswer(canvasElement);
      await waitFor(() => expect(canvas.queryByRole('alert')).not.toBeInTheDocument());
      await checkpoint('recovered');
    },
  ),
};

export const HistoryFailureRecovery = {
  name: 'Synthetic conversation history failure and recovery',
  ...reviewed(
    {
      path: '/ask',
      fixture: askFixture('history-error'),
      checkpoints: ['history-error', 'history-empty'],
      expectedHttpErrors: [{ method: 'GET', path: '/api/ask/conversations', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('alert')).toHaveTextContent('Conversation history is unavailable.');
      await checkpoint('history-error');
      await userEvent.click(canvas.getByRole('button', { name: 'Retry' }));
      await expect(await canvas.findByText(/No saved questions yet/)).toBeVisible();
      await expect(canvas.getByRole('textbox', { name: 'Question' })).toBeEnabled();
      await checkpoint('history-empty');
    },
  ),
};

export const MissingConversation = {
  name: 'Synthetic missing saved conversation',
  ...reviewed(
    {
      path: '/ask?c=999',
      fixture: askFixture('missing'),
      checkpoints: ['missing', 'new-question'],
      expectedHttpErrors: [{ method: 'GET', path: '/api/ask/bootstrap', status: 404, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Page not found' })).toBeVisible();
      await expect(canvas.getByText('The page you are looking for does not exist or has moved.')).toBeVisible();
      await checkpoint('missing');
      await userEvent.click(canvas.getByRole('link', { name: 'Ask your courses' }));
      await expect(await canvas.findByRole('heading', { name: 'What are you working through?' })).toBeVisible();
      await expect(canvas.getByRole('textbox', { name: 'Question' })).toBeEnabled();
      await checkpoint('new-question');
    },
  ),
};
