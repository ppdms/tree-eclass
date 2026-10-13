import { expect, within, userEvent, waitFor } from 'storybook/test';
import { checkpoint, reviewed } from '../scenario';
import { settingsFixture } from '../fixtures/settings';

export default { title: 'Workflows/Settings', id: 'settings', tags: ['ui-review'] };

async function settingsReady(canvasElement: HTMLElement) {
  const canvas = within(canvasElement);
  await expect(await canvas.findByRole('heading', { name: 'Settings' })).toBeVisible();
  return canvas;
}

async function openSection(canvasElement: HTMLElement, id: string) {
  await settingsReady(canvasElement);
  const section = canvasElement.querySelector<HTMLElement>(`#${id}`);
  if (!section) throw new Error(`Missing real settings section: ${id}`);
  if (section instanceof HTMLDetailsElement && !section.open) {
    const summary = section.querySelector('summary');
    if (!summary) throw new Error(`Missing settings disclosure: ${id}`);
    await userEvent.click(summary);
  }
  section.scrollIntoView({ block: 'start', behavior: 'instant' });
  return within(section);
}

export const PreferencesValidationRecovery = {
  name: 'Synthetic preferences validation, save failure and recovery',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture('save-error'),
      checkpoints: ['preferences', 'invalid-interval', 'save-error', 'saved'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/v1/settings/preferences', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const preferences = await openSection(canvasElement, 'preferences');
      const interval = preferences.getByRole('spinbutton', { name: 'Check interval (minutes)' });
      await expect(interval).toHaveValue(60);
      await checkpoint('preferences');
      await userEvent.clear(interval);
      await userEvent.type(interval, '2');
      await userEvent.click(preferences.getByRole('button', { name: 'Save changes' }));
      await expect(interval).toBeVisible();
      await expect(interval).toBeInvalid();
      await checkpoint('invalid-interval');
      await userEvent.clear(interval);
      await userEvent.type(interval, '30');
      await userEvent.clear(preferences.getByRole('textbox', { name: 'Local mirror path' }));
      await userEvent.type(preferences.getByRole('textbox', { name: 'Local mirror path' }), '/SyntheticReviewCourses');
      await userEvent.click(
        preferences.getByRole('checkbox', { name: 'Show undergraduate announcements in Activity' }),
      );
      await userEvent.click(preferences.getByRole('button', { name: 'Save changes' }));
      await expect(await preferences.findByRole('alert')).toHaveTextContent('Preferences could not be saved.');
      await checkpoint('save-error');
      await userEvent.click(preferences.getByRole('button', { name: 'Save changes' }));
      await expect(await preferences.findByText('Saved')).toBeVisible();
      await expect(interval).toHaveValue(30);
      await expect(
        preferences.getByRole('checkbox', {
          name: 'Show undergraduate announcements in Activity',
        }),
      ).toBeChecked();
      await expect(preferences.getByRole('button', { name: 'Save changes' })).toBeDisabled();
      await checkpoint('saved');
    },
  ),
};

export const ProviderConfiguration = {
  name: 'Synthetic provider model validation and save',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture('provider-validation'),
      checkpoints: ['provider', 'required-model', 'model-validation', 'provider-saved'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/v1/settings/ai', status: 422, count: 1 }],
    },
    async ({ canvasElement }) => {
      const ai = await openSection(canvasElement, 'ai');
      await expect(ai.getByText('No key · quota tracked · available')).toBeVisible();
      const order = within(ai.getByRole('group', { name: 'Ask fallback order' }));
      await expect(order.getByRole('combobox', { name: 'Primary' })).toHaveValue('ollama');
      const model = ai.getByRole('textbox', { name: 'Default model' });
      model.scrollIntoView({ block: 'center', behavior: 'instant' });
      await checkpoint('provider');
      await userEvent.clear(model);
      await userEvent.click(ai.getByRole('button', { name: 'Save changes' }));
      await expect(model).toBeInvalid();
      model.scrollIntoView({ block: 'center', behavior: 'instant' });
      await checkpoint('required-model');
      await userEvent.type(model, 'review-missing-model');
      await userEvent.click(ai.getByRole('button', { name: 'Save changes' }));
      await expect(await ai.findByRole('alert')).toHaveTextContent('The default model must appear');
      await checkpoint('model-validation');
      await userEvent.clear(model);
      await userEvent.type(model, 'review-small-model');
      await userEvent.click(ai.getByRole('checkbox', { name: /Ask uses the model’s reasoning tokens/ }));
      await userEvent.click(ai.getByRole('button', { name: 'Save changes' }));
      await expect(await ai.findByText('Saved')).toBeVisible();
      await expect(model).toHaveValue('review-small-model');
      await expect(ai.getByRole('checkbox', { name: /Ask uses the model’s reasoning tokens/ })).toBeChecked();
      await checkpoint('provider-saved');
    },
  ),
};

export const CourseConfiguration = {
  name: 'Synthetic course add failure, recovery and restore',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture('course-error'),
      checkpoints: ['catalog-filtered', 'course-error', 'course-added', 'course-restored'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/v1/courses', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = await settingsReady(canvasElement);
      await userEvent.type(await canvas.findByRole('searchbox', { name: 'Filter registered courses' }), 'Algorithms');
      const courses = within(canvas.getByRole('list', { name: 'Registered eClass courses' }));
      await expect(courses.getByText('Algorithms')).toBeVisible();
      await expect(courses.queryByText('Linear Algebra')).not.toBeInTheDocument();
      await userEvent.type(courses.getByRole('textbox', { name: 'Short name (optional)' }), 'Algorithms');
      await checkpoint('catalog-filtered');
      await userEvent.click(courses.getByRole('button', { name: 'Add' }));
      await expect(await courses.findByRole('alert')).toHaveTextContent('The course could not be added.');
      await checkpoint('course-error');
      await userEvent.click(courses.getByRole('button', { name: 'Add' }));
      await expect(await courses.findByText('Added')).toBeVisible();
      await checkpoint('course-added');
      await userEvent.click(canvas.getByRole('button', { name: 'Show Statistics' }));
      await expect(await canvas.findByText('No hidden courses.')).toBeVisible();
      await waitFor(() => expect(canvas.queryByRole('button', { name: 'Show Statistics' })).not.toBeInTheDocument());
      await checkpoint('course-restored');
    },
  ),
};

const loadingCatalog = settingsFixture('catalog-loading');
export const CatalogLoading = {
  name: 'Synthetic loading registered course catalog',
  ...reviewed(
    { path: '/settings', fixture: loadingCatalog, checkpoints: ['catalog-loading', 'catalog-ready'] },
    async ({ canvasElement }) => {
      const canvas = await settingsReady(canvasElement);
      await expect(await canvas.findByText('Loading registered courses…')).toBeVisible();
      await checkpoint('catalog-loading');
      loadingCatalog.release();
      await expect(await canvas.findByRole('searchbox', { name: 'Filter registered courses' })).toBeVisible();
      await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter registered courses' }), 'CS202');
      await expect(
        within(canvas.getByRole('list', { name: 'Registered eClass courses' })).getByText('Algorithms'),
      ).toBeVisible();
      await checkpoint('catalog-ready');
    },
  ),
};

export const ThemePreferences = {
  name: 'Synthetic light and e-ink theme interaction',
  ...reviewed(
    { path: '/settings', fixture: settingsFixture(), theme: 'light', checkpoints: ['light', 'eink', 'dark'] },
    async ({ canvasElement }) => {
      const canvas = await settingsReady(canvasElement);
      await waitFor(() => expect(canvas.getByRole('radio', { name: 'Light' })).toBeChecked());
      await expect(document.documentElement).toHaveAttribute('data-theme', 'light');
      await expect(canvas.getByRole('group', { name: 'Theme' })).toBeVisible();
      await checkpoint('light');
      await userEvent.click(canvas.getByText('E-ink', { exact: true }));
      await expect(canvas.getByRole('radio', { name: 'E-ink' })).toBeChecked();
      await expect(document.documentElement).toHaveAttribute('data-theme', 'eink');
      await checkpoint('eink');
      await userEvent.click(canvas.getByText('Dark', { exact: true }));
      await expect(canvas.getByRole('radio', { name: 'Dark' })).toBeChecked();
      await expect(document.documentElement).toHaveAttribute('data-theme', 'dark');
      await checkpoint('dark');
    },
  ),
};

export const ConnectionUnavailableRecovery = {
  name: 'Synthetic document storage unavailable and recovery',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture('connection-unavailable'),
      checkpoints: ['connection-unavailable', 'connection-ready'],
      expectedHttpErrors: [{ method: 'POST', path: '/api/settings/test-storage', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const storage = await openSection(canvasElement, 'storage');
      await userEvent.click(storage.getByRole('button', { name: 'Check document storage' }));
      await expect(await storage.findByRole('alert')).toHaveTextContent('Local document storage is unavailable.');
      await checkpoint('connection-unavailable');
      await userEvent.click(storage.getByRole('button', { name: 'Check document storage' }));
      await expect(await storage.findByRole('status')).toHaveTextContent('Local document storage is ready.');
      await waitFor(() => expect(storage.queryByRole('alert')).not.toBeInTheDocument());
      await checkpoint('connection-ready');
    },
  ),
};

export const LearnerDataExport = {
  name: 'Synthetic learner data download',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture(),
      checkpoints: ['export-ready', 'export-downloaded'],
      downloads: [
        {
          checkpoint: 'export-downloaded',
          filename: 'tree-eclass-learner-data.json',
          mediaType: 'application/json',
          textIncludes: 'tree-eClass learner data export',
        },
      ],
    },
    async ({ canvasElement }) => {
      const data = await openSection(canvasElement, 'data-export');
      const link = data.getByRole('link', { name: 'Download learner data' });
      await expect(link).toBeVisible();
      await expect(link).toHaveAttribute('href', '/api/settings/export');
      await expect(link).toHaveAttribute('download', 'tree-eclass-learner-data.json');
      await expect(data.getByText(/Download a portable copy of annotations/)).toBeVisible();
      await checkpoint('export-ready');
      await userEvent.click(link);
      await expect(link).toBeVisible();
      await checkpoint('export-downloaded');
    },
  ),
};

export const SettingsLoadRecovery = {
  name: 'Synthetic Settings route load failure and recovery',
  ...reviewed(
    {
      path: '/settings',
      fixture: settingsFixture('load-error'),
      checkpoints: ['load-error', 'loaded'],
      expectedHttpErrors: [{ method: 'GET', path: '/api/v1/settings', status: 503, count: 1 }],
    },
    async ({ canvasElement }) => {
      const canvas = within(canvasElement);
      await expect(await canvas.findByRole('heading', { name: 'This page could not load' })).toBeVisible();
      await expect(
        canvas.getByText('tree-eClass could not load this page. Your saved data has not changed.'),
      ).toBeVisible();
      await checkpoint('load-error');
      await userEvent.click(canvas.getByRole('link', { name: 'Settings' }));
      await expect(await canvas.findByRole('heading', { name: 'Settings' })).toBeVisible();
      await expect(await canvas.findByRole('searchbox', { name: 'Filter registered courses' })).toBeVisible();
      await checkpoint('loaded');
    },
  ),
};
