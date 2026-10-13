import { expect, userEvent, within, waitFor } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { activityFixture, olderUpdatesFixture } from '../fixtures/activity';

export default { title: 'Workflows/Activity', id: 'activity', tags: ['ui-review'] };

export const Triage = {
  name: 'Synthetic updates: read marking and importance triage',
  ...reviewed(
    {
      path: '/activity',
      fixture: activityFixture(),
      checkpoints: ['mixed-updates', 'marked-read', 'important-only', 'file-changes'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('link', { name: 'Matrices lecture revised' })).toBeVisible();
      await expect(canvas.getByRole('link', { name: 'Midterm room and submission deadline' })).toBeVisible();
      await checkpoint('mixed-updates');
      await userEvent.click(canvas.getByRole('button', { name: 'Mark Matrices lecture revised read' }));
      await expect(canvas.getByRole('button', { name: 'Mark Matrices lecture revised unread' })).toHaveAttribute(
        'aria-pressed',
        'true',
      );
      await checkpoint('marked-read');
      await userEvent.click(canvas.getByRole('tab', { name: 'Important' }));
      await waitFor(() => {
        expect(canvas.getByRole('link', { name: 'Midterm room and submission deadline' })).toBeVisible();
        expect(canvas.queryByRole('link', { name: 'Matrices lecture revised' })).not.toBeInTheDocument();
      });
      await checkpoint('important-only');
      await userEvent.click(canvas.getByRole('tab', { name: 'Course files' }));
      await waitFor(() => {
        expect(canvas.getByRole('link', { name: 'Matrices lecture revised' })).toBeVisible();
        expect(canvas.queryByRole('link', { name: 'Midterm room and submission deadline' })).not.toBeInTheDocument();
      });
      await checkpoint('file-changes');
    },
  ),
};

export const EmptyFilters = {
  name: 'Synthetic empty updates and filtered empty state',
  ...reviewed(
    {
      path: '/activity',
      fixture: activityFixture(true),
      theme: 'light',
      checkpoints: ['empty-inbox', 'empty-important'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('tab', { name: 'All updates' })).toBeVisible();
      await expect(canvas.getAllByText('Nothing in this view yet.')[0]).toBeVisible();
      await checkpoint('empty-inbox');
      await userEvent.click(canvas.getByRole('tab', { name: 'Important' }));
      await expect(
        (
          await canvas.findAllByText(
            'Nothing important yet. Deadline and course-change updates appear here automatically.',
          )
        )[0],
      ).toBeVisible();
      await checkpoint('empty-important');
    },
  ),
};

const older = olderUpdatesFixture();
export const OlderUpdatesRecovery = {
  name: 'Synthetic older updates: loading, failure, and retry',
  ...reviewed(
    {
      path: '/activity',
      fixture: older,
      expectedHttpErrors: [{ method: 'GET', path: '/api/v1/inbox', status: 503, count: 1 }],
      checkpoints: ['older-loading', 'older-failed', 'older-recovered'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      (await canvas.findByRole('link', { name: 'Matrices lecture revised' })).scrollIntoView();
      const sentinel = canvasElement.querySelector('[aria-live="polite"]');
      sentinel?.scrollIntoView();
      await expect(await canvas.findByText('Fetching older updates…')).toBeVisible();
      await checkpoint('older-loading');
      older.releaseOlder();
      await expect(await canvas.findByRole('alert')).toHaveTextContent('Older updates could not load.');
      await checkpoint('older-failed');
      await userEvent.click(canvas.getByRole('button', { name: 'Try again' }));
      await expect(
        await canvas.findByRole('link', { name: 'Reading list for probability distributions' }),
      ).toBeVisible();
      await expect(canvas.queryByRole('alert')).not.toBeInTheDocument();
      await checkpoint('older-recovered');
    },
  ),
};
