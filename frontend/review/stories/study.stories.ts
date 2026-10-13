import { expect, within, userEvent, waitFor } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { studyFixture } from '../fixtures/study';

export default { title: 'Workflows/Study', id: 'study', tags: ['ui-review'] };

async function openPlanner(canvasElement: HTMLElement) {
  await userEvent.click(within(canvasElement).getByRole('button', { name: 'Edit' }));
  return within(await within(document.body).findByRole('dialog', { name: 'Study plan' }));
}

async function editAvailability(canvasElement: HTMLElement) {
  const planner = await openPlanner(canvasElement);
  await userEvent.click(planner.getByRole('tab', { name: 'Availability' }));
  await userEvent.clear(planner.getByLabelText('Session minutes'));
  await userEvent.type(planner.getByLabelText('Session minutes'), '45');
  return planner;
}

export const Decision = {
  name: 'Synthetic source-linked decision and progress',
  ...reviewed(
    { path: '/study', fixture: studyFixture(), checkpoints: ['decision', 'source', 'recorded'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(
        await canvas.findByRole('heading', {
          name: 'Read matrix multiplication and explain the order of operations',
        }),
      ).toBeVisible();
      await expect(canvas.getByRole('link', { name: 'Start session' })).toHaveAttribute(
        'href',
        '/study/session?course_id=101&action_id=action-101-matrix-recall',
      );
      await checkpoint('decision');
      const source = canvas.getByRole('button', { name: 'source' });
      await expect(source).toHaveTextContent('1 source');
      await userEvent.click(source);
      const body = within(document.body);
      await expect(await body.findByRole('link', { name: 'Matrices — synthetic lecture · p. 1' })).toHaveAttribute(
        'href',
        '/study/session?course_id=101&document_id=document-101-matrices&action_id=action-101-matrix-recall',
      );
      await waitFor(() =>
        expect(
          body.getByRole('link', {
            name: 'Matrices — synthetic lecture · p. 1',
          }),
        ).toBeVisible(),
      );
      await checkpoint('source');
      await userEvent.keyboard('{Escape}');
      await userEvent.click(canvas.getByRole('button', { name: 'Finish' }));
      await userEvent.click(await body.findByRole('button', { name: 'Completed' }));
      await expect(await canvas.findByRole('heading', { name: 'Progress recorded' })).toBeVisible();
      await checkpoint('recorded');
    },
  ),
};

export const PlannerValidation = {
  name: 'Synthetic planner validation and persisted edits',
  ...reviewed(
    { path: '/study', fixture: studyFixture(), checkpoints: ['editing', 'validation', 'saved'] },
    async ({ canvasElement }) => {
      await within(canvasElement).findByRole('button', { name: 'Edit' });
      const planner = await editAvailability(canvasElement);
      await userEvent.clear(planner.getByLabelText('Days off'));
      await userEvent.type(planner.getByLabelText('Days off'), 'next Sunday');
      await checkpoint('editing');
      await userEvent.click(planner.getByRole('button', { name: 'Save plan' }));
      await expect(await planner.findByRole('alert')).toHaveTextContent('must use YYYY-MM-DD');
      await expect(planner.getByLabelText('Session minutes')).toHaveValue(45);
      await checkpoint('validation');
      await userEvent.clear(planner.getByLabelText('Days off'));
      await userEvent.type(planner.getByLabelText('Days off'), '2026-10-19');
      await userEvent.click(planner.getByRole('button', { name: 'Save plan' }));
      await waitFor(() => expect(within(document.body).queryByRole('dialog', { name: 'Study plan' })).toBeNull());
      const saved = await openPlanner(canvasElement);
      await userEvent.click(saved.getByRole('tab', { name: 'Availability' }));
      await expect(saved.getByLabelText('Session minutes')).toHaveValue(45);
      await expect(saved.getByLabelText('Days off')).toHaveValue('2026-10-19');
      await checkpoint('saved');
    },
  ),
};

export const PlannerRecovery = {
  name: 'Synthetic failed planner save retains edits',
  ...reviewed(
    {
      path: '/study',
      fixture: studyFixture('save-recovery'),
      checkpoints: ['failed-save', 'recovered'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/v1/study/planner', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      await within(canvasElement).findByRole('button', { name: 'Edit' });
      const planner = await editAvailability(canvasElement);
      await userEvent.click(planner.getByRole('button', { name: 'Save plan' }));
      await expect(await planner.findByRole('alert')).toHaveTextContent('Your edits are still here');
      await expect(planner.getByLabelText('Session minutes')).toHaveValue(45);
      await checkpoint('failed-save');
      await userEvent.click(planner.getByRole('button', { name: 'Save plan' }));
      await waitFor(() => expect(within(document.body).queryByRole('dialog', { name: 'Study plan' })).toBeNull());
      const saved = await openPlanner(canvasElement);
      await userEvent.click(saved.getByRole('tab', { name: 'Availability' }));
      await expect(saved.getByLabelText('Session minutes')).toHaveValue(45);
      await checkpoint('recovered');
    },
  ),
};

export const Empty = {
  name: 'Synthetic empty plan and exam entry',
  ...reviewed(
    { path: '/study', fixture: studyFixture('empty'), checkpoints: ['empty', 'exam-entry'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'No next action is ready' })).toBeVisible();
      await checkpoint('empty');
      await userEvent.click(canvas.getByRole('button', { name: 'Add exam dates' }));
      const planner = within(await within(document.body).findByRole('dialog', { name: 'Study plan' }));
      await expect(planner.getByRole('checkbox', { name: 'Include Linear Algebra' })).not.toBeChecked();
      await userEvent.click(planner.getByRole('checkbox', { name: 'Include Linear Algebra' }));
      await expect(planner.getByRole('checkbox', { name: 'Include Linear Algebra' })).toBeChecked();
      await expect(planner.getAllByLabelText('Exam')[0]).toBeRequired();
      await checkpoint('exam-entry');
    },
  ),
};

export const ProjectionPending = {
  name: 'Synthetic pending projection with available source',
  ...reviewed(
    { path: '/study', fixture: studyFixture('pending'), checkpoints: ['pending', 'fallback-source'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      const pending = await canvas.findByText(
        'The study plan is being prepared. Available course updates are shown below.',
      );
      await expect(pending).toHaveAttribute('role', 'status');
      await expect(pending).toBeVisible();
      await expect(canvas.getByText('File suggestion')).toBeVisible();
      await checkpoint('pending');
      const source = canvas.getByRole('button', { name: 'source' });
      await expect(source).toHaveTextContent('1 source');
      await userEvent.click(source);
      await waitFor(() =>
        expect(
          within(document.body).getByRole('link', {
            name: 'Matrices — synthetic lecture · p. 1',
          }),
        ).toBeVisible(),
      );
      await checkpoint('fallback-source');
    },
  ),
};

const loadingFixture = studyFixture('loading');
export const Loading = {
  name: 'Synthetic study transport loading',
  ...reviewed(
    { path: '/study', fixture: loadingFixture, checkpoints: ['loading', 'loaded'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByText('Opening the page…')).toBeVisible();
      await checkpoint('loading');
      loadingFixture.release();
      await expect(await canvas.findByRole('link', { name: 'Start session' })).toBeVisible();
      await userEvent.click(canvas.getByRole('button', { name: 'Edit' }));
      await expect(await within(document.body).findByRole('dialog', { name: 'Study plan' })).toBeVisible();
      await checkpoint('loaded');
    },
  ),
};

export const LoadRecovery = {
  name: 'Synthetic study loader error and reopen',
  ...reviewed(
    {
      path: '/study',
      fixture: studyFixture('load-recovery'),
      checkpoints: ['load-error', 'retried'],
      expectedHttpErrors: [{ method: 'GET', path: '/api/v1/study', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('alert')).toHaveTextContent('This page could not load');
      await checkpoint('load-error');
      await userEvent.click(canvas.getByRole('link', { name: 'Open Study' }));
      await expect(await canvas.findByRole('link', { name: 'Start session' })).toBeVisible();
      await checkpoint('retried');
    },
  ),
};
