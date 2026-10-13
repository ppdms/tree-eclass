import { colors } from '../ui/palette';
import type { UIStyle } from '../ui/types';

export const styles = {
  panel: { gap: 18, minWidth: 0 },
  heading: { color: colors.text, fontSize: 21, fontWeight: 650, lineHeight: 1.3 },
  title: { color: colors.text, fontSize: 16, fontWeight: 600, lineHeight: 1.35 },
  description: { color: colors.muted, fontSize: 13, lineHeight: 1.5 },
  category: { color: colors.muted, fontSize: 11, fontWeight: 650, lineHeight: 1.4 },
  grid: {
    display: 'grid',
    gridTemplateColumns: ['1fr', '1fr'],
    gap: 14,
    minWidth: 0,
    mobile: { gridTemplateColumns: ['1fr'] },
  },
  card: {
    padding: 18,
    gap: 16,
    minWidth: 0,
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 16,
    backgroundColor: colors.surface,
  },
  profile: { gridRowSpan: 2 },
  danger: { borderColor: colors.danger },
  copy: { gap: 4, minWidth: 0 },
  actions: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 10, minWidth: 0 },
  source: { gap: 12, minWidth: 0, paddingTop: 10, borderBottomWidth: 1, borderColor: colors.border },
  sourceRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, minWidth: 0 },
  label: { color: colors.muted, fontSize: 12, lineHeight: 1.4, width: 80, flexShrink: 0 },
  value: { color: colors.text, fontSize: 12, lineHeight: 1.45, minWidth: 0 },
  folder: { color: colors.text, fontSize: 12, lineHeight: 1.45, mono: true, minWidth: 0 },
  link: { color: colors.link, fontSize: 12, lineHeight: 1.45 },
} satisfies Record<string, UIStyle>;
