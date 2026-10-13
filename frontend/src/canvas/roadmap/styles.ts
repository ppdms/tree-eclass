import { colors } from '../ui/palette';
import type { UIStyle } from '../ui/types';

export const styles = {
  stack: { flexDirection: 'column', gap: 14 },
  row: { flexDirection: 'row', flexWrap: 'wrap', gap: 10, alignItems: 'center' },
  title: { fontSize: 19, fontWeight: 650, color: colors.text },
  heading: { fontSize: 15, fontWeight: 650, color: colors.text },
  body: { fontSize: 14, lineHeight: 1.5, color: colors.text },
  meta: { fontSize: 12, lineHeight: 1.45, color: colors.muted },
  card: { padding: 16, gap: 12, flexDirection: 'column' },
  secondary: {
    display: 'grid',
    gridTemplateColumns: ['1fr', '1fr'],
    gap: 14,
    mobile: { gridTemplateColumns: ['1fr'] },
  },
  action: {
    padding: 14,
    gap: 10,
    flexDirection: 'column',
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 12,
  },
  disclosure: { width: '100%', justifyContent: 'space-between' },
} satisfies Record<string, UIStyle>;

export function semanticId(...parts: (string | number | undefined)[]): string {
  return parts.map((part) => encodeURIComponent(String(part ?? ''))).join(':');
}

export function intentKey(prefix: string): string {
  const random =
    globalThis.crypto?.randomUUID?.().replaceAll('-', '') ||
    `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
  return `${prefix}-${random}`.slice(0, 128);
}
