import { isServer } from '@/lib/display';

export const DIRTY_FORMS: Set<string> = new Set();
export const ACTION_TO_SECTION = {
  '/api/v1/settings/ai': 'ai',
  '/api/v1/settings/webhook': 'webhook',
  '/api/v1/settings/credentials': 'credentials',
  '/api/v1/settings/preferences': 'preferences',
  '/api/v1/settings/discord-exporter': 'discord-exporter',
  '/api/v1/settings/discord-course-map': 'discord-course-mapping',
} satisfies Record<string, string>;
if (!isServer()) {
  window.addEventListener('beforeunload', (event) => {
    if (DIRTY_FORMS.size) event.preventDefault();
  });
}
