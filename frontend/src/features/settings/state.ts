import { isServer } from '@/lib/display';

export const DIRTY_FORMS: Set<string> = new Set();
export const ACTION_TO_SECTION = {
  '/api/settings/ai': 'ai',
  '/settings/webhook': 'webhook',
  '/settings/credentials': 'credentials',
  '/settings/preferences': 'preferences',
  '/settings/discord-exporter': 'discord-exporter',
  '/settings/discord-course-map': 'discord-course-mapping',
} satisfies Record<string, string>;
if (!isServer()) {
  window.addEventListener('beforeunload', (event) => {
    if (DIRTY_FORMS.size) event.preventDefault();
  });
}
