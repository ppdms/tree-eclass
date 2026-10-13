import { expect, userEvent, waitFor, within } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { exercisesFixture } from '../fixtures/exercises';

export default { title: 'Workflows/Exercises', id: 'exercises', tags: ['ui-review'] };

export const AssignmentTriage = {
  name: 'Synthetic assignments: deadline filters and completed feedback',
  ...reviewed(
    {
      path: '/exercises',
      fixture: exercisesFixture(),
      theme: 'light',
      checkpoints: ['assignment-inbox', 'overdue-assignment', 'upcoming-assignment', 'submitted-feedback'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(
        await canvas.findByRole('link', {
          name: 'Open external assignment: Matrix systems problem set',
        }),
      ).toBeVisible();
      await expect(
        canvas.getByRole('link', { name: 'Open external assignment: Probability simulation report' }),
      ).toBeVisible();
      await checkpoint('assignment-inbox');
      await userEvent.click(canvas.getByRole('button', { name: /^Overdue/ }));
      await expect(canvas.getByText('Overdue · 2 days')).toBeVisible();
      await expect(
        canvas.queryByRole('link', {
          name: 'Open external assignment: Algorithm complexity analysis',
        }),
      ).not.toBeInTheDocument();
      await checkpoint('overdue-assignment');
      await userEvent.click(canvas.getByRole('button', { name: /^Upcoming/ }));
      await expect(
        canvas.getByRole('link', { name: 'Open external assignment: Algorithm complexity analysis' }),
      ).toBeVisible();
      await checkpoint('upcoming-assignment');
      await userEvent.click(canvas.getByRole('button', { name: /^Completed/ }));
      await userEvent.click(canvas.getByLabelText('View assignment details for Probability simulation report'));
      await expect(
        await canvas.findByText('Clear plots and sound interpretation; explain the seed choice.'),
      ).toBeVisible();
      await expect(canvas.getByText('Grade 8.5 / 10')).toBeVisible();
      await checkpoint('submitted-feedback');
    },
  ),
};

const recoveringDetails = exercisesFixture('detail-recovery');
export const DetailsRecovery = {
  name: 'Synthetic assignment details: loading, failure, and retry',
  ...reviewed(
    {
      path: '/exercises',
      fixture: recoveringDetails,
      expectedHttpErrors: [
        { method: 'GET', path: '/api/v1/courses/101/exercises/matrix-problems', status: 503, count: 1 },
      ],
      checkpoints: ['details-loading', 'details-failed', 'details-recovered'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByLabelText('View assignment details for Matrix systems problem set'));
      await expect(await canvas.findByText('Loading assignment details…')).toBeVisible();
      await checkpoint('details-loading');
      recoveringDetails.releaseDetails();
      await expect(await canvas.findByRole('alert')).toHaveTextContent('Assignment details could not load.');
      await checkpoint('details-failed');
      await userEvent.click(canvas.getByRole('button', { name: 'Try again' }));
      await expect(await canvas.findByText('Solve the three linear systems using row reduction.')).toBeVisible();
      await expect(canvas.getByText('State rank and describe all solutions.')).toBeVisible();
      await checkpoint('details-recovered');
    },
  ),
};

async function confirmHide(canvasElement: HTMLElement) {
  const canvas = within(canvasElement);
  await userEvent.click(canvas.getByRole('button', { name: 'Hide exercise' }));
  const dialog = await within(document.body).findByRole('dialog', { name: 'Hide this exercise?' });
  await userEvent.click(within(dialog).getByRole('button', { name: 'Hide exercise' }));
}

export const HideRecovery = {
  name: 'Synthetic assignment hide: confirmation, failure, retry, and restore',
  ...reviewed(
    {
      path: '/exercises',
      fixture: exercisesFixture('hide-recovery'),
      expectedHttpErrors: [
        { method: 'POST', path: '/api/v1/courses/101/exercises/matrix-problems/ignore', status: 503, count: 1 },
      ],
      checkpoints: ['hide-confirmation', 'hide-failed', 'hidden-assignment', 'restored-assignment'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByLabelText('View assignment details for Matrix systems problem set'));
      await userEvent.click(await canvas.findByRole('button', { name: 'Hide exercise' }));
      const dialog = await within(document.body).findByRole('dialog', { name: 'Hide this exercise?' });
      await waitFor(() => expect(within(dialog).getByText(/It will leave the active exercise list/)).toBeVisible());
      await checkpoint('hide-confirmation');
      await userEvent.click(within(dialog).getByRole('button', { name: 'Hide exercise' }));
      await expect(await canvas.findByRole('alert')).toHaveTextContent('Synthetic hide operation was not saved.');
      await checkpoint('hide-failed');
      await confirmHide(canvasElement);
      await expect(await canvas.findByText('Assignment hidden.')).toBeVisible();
      await userEvent.click(canvas.getByRole('button', { name: /^Hidden/ }));
      await userEvent.click(canvas.getByLabelText('View assignment details for Matrix systems problem set'));
      await expect(await canvas.findByRole('button', { name: 'Restore exercise' })).toBeVisible();
      await checkpoint('hidden-assignment');
      await userEvent.click(canvas.getByRole('button', { name: 'Restore exercise' }));
      await waitFor(() =>
        expect(
          canvas.queryByLabelText('View assignment details for Matrix systems problem set'),
        ).not.toBeInTheDocument(),
      );
      await userEvent.click(canvas.getByRole('button', { name: /^All/ }));
      await expect(
        await canvas.findByRole('link', {
          name: 'Open external assignment: Matrix systems problem set',
        }),
      ).toBeVisible();
      await checkpoint('restored-assignment');
    },
  ),
};

export const HideUndo = {
  name: 'Synthetic assignment hide with immediate local undo',
  ...reviewed(
    {
      path: '/exercises',
      fixture: exercisesFixture(),
      theme: 'eink',
      checkpoints: ['assignment-hidden', 'hide-undone'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByLabelText('View assignment details for Matrix systems problem set'));
      await canvas.findByRole('button', { name: 'Hide exercise' });
      await confirmHide(canvasElement);
      await expect(await canvas.findByText('Assignment hidden.')).toBeVisible();
      await expect(
        canvas.queryByRole('link', {
          name: 'Open external assignment: Matrix systems problem set',
        }),
      ).not.toBeInTheDocument();
      await checkpoint('assignment-hidden');
      await userEvent.click(canvas.getByRole('button', { name: 'Undo' }));
      await expect(
        await canvas.findByRole('link', {
          name: 'Open external assignment: Matrix systems problem set',
        }),
      ).toBeVisible();
      await expect(canvas.queryByRole('button', { name: 'Undo' })).not.toBeInTheDocument();
      await checkpoint('hide-undone');
    },
  ),
};

export const EmptyAssignments = {
  name: 'Synthetic empty assignment inbox with keyboard filter selection',
  ...reviewed(
    {
      path: '/exercises',
      fixture: exercisesFixture('empty'),
      theme: 'light',
      checkpoints: ['no-assignments', 'empty-filter-selected'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByText('No exercises match this filter.')).toBeVisible();
      await expect(canvas.getByText('0 assignments')).toBeVisible();
      await checkpoint('no-assignments');
      const all = canvas.getByRole('button', { name: /^All/ });
      await userEvent.click(all);
      await expect(all).toHaveAttribute('aria-pressed', 'true');
      await expect(all).toHaveFocus();
      await checkpoint('empty-filter-selected');
    },
  ),
};
