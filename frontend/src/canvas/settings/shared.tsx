import { createContext, useContext, type ReactNode } from 'react';
import type { SettingsPayload } from '@/features/settings/types';
import type { CourseSummary } from '@/lib/types';
import { colors } from '../ui/palette';
import type { UIStyle } from '../ui/types';
import { Text, View } from '../ui/primitives';

export type { SettingsPayload };
export type SettingsCourse = CourseSummary & { storage_prefix?: string | null };
export const sections = [
  ['appearance', 'Appearance'],
  ['add-course', 'Add course'],
  ['hidden-courses', 'Hidden courses'],
  ['knowledge', 'Knowledge maintenance'],
  ['ai', 'AI backends and models'],
  ['discord-course-mapping', 'Discord course mapping'],
  ['discord-exporter', 'Discord exporter'],
  ['webhook', 'Webhook'],
  ['credentials', 'eClass credentials'],
  ['storage', 'Document storage'],
  ['preferences', 'Application preferences'],
  ['data-export', 'Learner data'],
] as const;
export type SectionID = (typeof sections)[number][0];
export const styles = {
  page: { width: '100%', maxWidth: 1180, marginLeft: 'auto', marginRight: 'auto', padding: 24, gap: 20 },
  stack: { width: '100%', minWidth: 0, gap: 12 },
  row: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 12 },
  title: { fontSize: 24, fontWeight: 650, color: colors.text },
  heading: { fontSize: 18, fontWeight: 600, color: colors.text },
  text: { fontSize: 14, lineHeight: 1.4, color: colors.text },
  muted: { fontSize: 13, lineHeight: 1.4, color: colors.muted },
  card: { padding: 16, borderWidth: 1, borderRadius: 16, borderColor: colors.border, gap: 12 },
} satisfies Record<string, UIStyle>;

interface SettingsContextValue {
  opened: Set<string>;
  dirty: Set<string>;
  setOpen(id: string, open: boolean): void;
  setDirty(id: string, dirty: boolean): void;
  refresh(): void;
  observe(id: string, rect: { y: number; height: number }): void;
}
export const SettingsContext = createContext<SettingsContextValue | null>(null);
export function useSettingsContext(): SettingsContextValue {
  const context = useContext(SettingsContext);
  if (!context) throw new Error('Settings controls require the Settings route');
  return context;
}
export function Hint({ children }: { children: ReactNode }) {
  return <Text style={styles.muted}>{children}</Text>;
}
export function Group({ title, children }: { title: string; children: ReactNode }) {
  return (
    <View style={styles.stack}>
      <Text style={styles.heading}>{title}</Text>
      {children}
    </View>
  );
}
export function message(cause: unknown, fallback: string): string {
  return cause instanceof Error ? cause.message : fallback;
}
