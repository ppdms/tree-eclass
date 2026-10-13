import { expect, userEvent, waitFor, within } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { courseDetailFixture, courseShelfFixture, unavailableCourseFixture } from '../fixtures/courses';

export default { title: 'Workflows/Courses', id: 'courses', tags: ['ui-review'] };

export const ShelfFiltering = {
  name: 'Synthetic course shelf: name, code, and empty search',
  ...reviewed(
    {
      path: '/courses',
      fixture: courseShelfFixture(),
      theme: 'light',
      checkpoints: ['course-shelf', 'code-filter', 'no-search-results', 'search-cleared'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      const list = within(await canvas.findByRole('list', { name: 'Active courses' }));
      await expect(list.getAllByRole('link')).toHaveLength(3);
      await checkpoint('course-shelf');
      const search = canvas.getByRole('searchbox', { name: 'Filter courses' });
      await userEvent.type(search, 'CS202');
      await expect(list.getByRole('link', { name: /Algorithms/ })).toBeVisible();
      await expect(list.queryByRole('link', { name: /Linear Algebra/ })).not.toBeInTheDocument();
      await checkpoint('code-filter');
      await userEvent.clear(search);
      await userEvent.type(search, 'No matching subject');
      await expect(canvas.getByText(/No courses match/)).toBeVisible();
      await checkpoint('no-search-results');
      await userEvent.clear(search);
      await expect(within(canvas.getByRole('list', { name: 'Active courses' })).getAllByRole('link')).toHaveLength(3);
      await checkpoint('search-cleared');
    },
  ),
};

export const ShelfOrderRecovery = {
  name: 'Synthetic keyboard course order: rollback and successful retry',
  ...reviewed(
    {
      path: '/courses',
      fixture: courseShelfFixture('order-recovery'),
      expectedHttpErrors: [{ method: 'POST', path: '/api/courses/reorder', status: 503, count: 1 }],
      checkpoints: ['order-rollback', 'order-saved'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      const list = within(await canvas.findByRole('list', { name: 'Active courses' }));
      list.getByRole('link', { name: /Linear Algebra/ }).focus();
      await userEvent.keyboard('{ArrowRight}');
      await expect(await canvas.findByRole('alert')).toHaveTextContent('The previous order was restored.');
      await expect(list.getAllByRole('link')[0]).toHaveTextContent('Linear Algebra');
      await checkpoint('order-rollback');
      list.getByRole('link', { name: /Linear Algebra/ }).focus();
      await userEvent.keyboard('{ArrowRight}');
      await expect(await canvas.findByText('Course order saved')).toBeVisible();
      await expect(list.getAllByRole('link')[0]).toHaveTextContent('Algorithms');
      await checkpoint('order-saved');
    },
  ),
};

export const DetailLibrary = {
  name: 'Synthetic course detail: progress, files, guide, and study level',
  ...reviewed(
    {
      path: '/courses/101',
      fixture: courseDetailFixture(),
      theme: 'eink',
      checkpoints: ['course-overview', 'lecture-tree', 'file-guide', 'studied-file', 'file-search-empty'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Linear Algebra', level: 1 })).toBeVisible();
      await expect(
        canvas.getByRole('heading', { name: 'Practice row reduction and identify pivot columns' }),
      ).toBeVisible();
      await expect(canvas.getByText('6 of 12 actions complete')).toBeVisible();
      await checkpoint('course-overview');
      await userEvent.click(canvas.getByRole('tab', { name: 'Files' }));
      await expect(await canvas.findByRole('link', { name: 'Open Matrices.pdf' })).toBeVisible();
      await checkpoint('lecture-tree');
      await userEvent.click(canvas.getByRole('button', { name: /Guide/ }));
      await expect(await canvas.findByText(/The lecture connects elementary row operations/)).toBeVisible();
      await checkpoint('file-guide');
      await userEvent.click(canvas.getByRole('button', { name: 'Study level: familiar. Activate to change.' }));
      await userEvent.click(canvas.getByRole('button', { name: 'Set study level to mastered' }));
      await waitFor(() =>
        expect(
          canvas.getByRole('button', {
            name: 'Study level: mastered. Activate to change.',
          }),
        ).toBeEnabled(),
      );
      await checkpoint('studied-file');
      await userEvent.type(canvas.getByRole('textbox', { name: 'Find a course file' }), 'nonexistent-material');
      await expect(canvas.getByText('No matching eClass files.')).toBeVisible();
      await checkpoint('file-search-empty');
    },
  ),
};

export const ExternalLibrary = {
  name: 'Synthetic external material classification and folder expansion',
  ...reviewed(
    {
      path: '/courses/101',
      fixture: courseDetailFixture(),
      checkpoints: ['expanded-practice', 'external-notes', 'reclassified-material'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByRole('tab', { name: 'Files' }));
      const practice = await canvas.findByRole('button', { name: 'Practice' });
      await expect(practice).toHaveAttribute('aria-expanded', 'false');
      await userEvent.click(practice);
      await expect(await canvas.findByRole('link', { name: 'Open Row-reduction.pdf' })).toBeVisible();
      await expect(
        canvas.getByRole('link', {
          name: 'Open Interactive matrix calculator in the external course source',
        }),
      ).toHaveAttribute('href', 'https://example.invalid/algebra/matrix-calculator');
      await checkpoint('expanded-practice');
      await userEvent.click(canvas.getByText('External library', { selector: 'summary' }));
      const type = await canvas.findByRole('combobox', { name: 'Document type for Matrix revision notes.txt' });
      await expect(canvas.getByRole('heading', { name: 'Student notes' })).toBeVisible();
      await expect(canvas.getByRole('link', { name: 'Open file' })).toHaveAttribute(
        'href',
        '/api/study/document/document-101-revision-notes/content?course_id=101',
      );
      await checkpoint('external-notes');
      await userEvent.selectOptions(type, 'study_guide');
      await expect(await canvas.findByRole('heading', { name: 'Student-made study guides' })).toBeVisible();
      await expect(canvas.getByRole('combobox', { name: 'Document type for Matrix revision notes.txt' })).toHaveValue(
        'study_guide',
      );
      await checkpoint('reclassified-material');
    },
  ),
};

const recoveringFiles = courseDetailFixture('files-recovery');
export const FilesRecovery = {
  name: 'Synthetic file library: pending, failure, and retry',
  ...reviewed(
    {
      path: '/courses/101',
      fixture: recoveringFiles,
      expectedHttpErrors: [{ method: 'GET', path: '/api/v1/courses/101/files', status: 503, count: 1 }],
      checkpoints: ['files-loading', 'files-failed', 'files-recovered'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByRole('tab', { name: 'Files' }));
      await expect(await canvas.findByText('Loading course files…')).toBeVisible();
      await checkpoint('files-loading');
      recoveringFiles.releaseFiles();
      await expect(await canvas.findByRole('alert')).toHaveTextContent('The course files could not load.');
      await checkpoint('files-failed');
      await userEvent.click(canvas.getByRole('button', { name: 'Try again' }));
      await expect(await canvas.findByRole('link', { name: 'Open Matrices.pdf' })).toBeVisible();
      await expect(canvas.queryByRole('alert')).not.toBeInTheDocument();
      await checkpoint('files-recovered');
    },
  ),
};

export const EmptyLibrary = {
  name: 'Synthetic unsynchronized course files and empty external library',
  ...reviewed(
    {
      path: '/courses/101',
      fixture: courseDetailFixture('empty-files'),
      theme: 'light',
      checkpoints: ['no-synchronized-files', 'empty-external-library'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await userEvent.click(await canvas.findByRole('tab', { name: 'Files' }));
      await expect(await canvas.findByText(/No synchronized eClass files yet/)).toBeVisible();
      await checkpoint('no-synchronized-files');
      await userEvent.click(canvas.getByText('External library', { selector: 'summary' }));
      await expect(await canvas.findByText(/No files in this course’s external folder yet/)).toBeVisible();
      await checkpoint('empty-external-library');
    },
  ),
};

export const EmptyShelf = {
  name: 'Synthetic empty course shelf: keyboard-accessible setup action',
  ...reviewed(
    { path: '/courses', fixture: courseShelfFixture('empty'), checkpoints: ['no-courses', 'setup-action-focused'] },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'No courses yet' })).toBeVisible();
      const add = canvas.getByRole('link', { name: 'Add course' });
      await expect(add).toHaveAttribute('href', '/settings#add-course');
      await checkpoint('no-courses');
      add.focus();
      await userEvent.keyboard('{Shift>}{Tab}{/Shift}');
      await userEvent.tab();
      await expect(add).toHaveFocus();
      await checkpoint('setup-action-focused');
    },
  ),
};

export const UnavailableCourse = {
  name: 'Synthetic unavailable course: route error and return to activity',
  ...reviewed(
    {
      path: '/courses/101',
      fixture: unavailableCourseFixture(),
      expectedHttpErrors: [{ method: 'GET', path: '/api/v1/courses/101/overview', status: 404, count: 1 }],
      checkpoints: ['unavailable-course', 'returned-to-activity'],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'Page not found' })).toBeVisible();
      await expect(canvas.getByRole('alert')).toHaveTextContent('The page you are looking for does not exist');
      await checkpoint('unavailable-course');
      await userEvent.click(canvas.getByRole('link', { name: 'Return to Activity' }));
      await expect(await canvas.findByRole('link', { name: 'Matrices lecture revised' })).toBeVisible();
      await checkpoint('returned-to-activity');
    },
  ),
};
