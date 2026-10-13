import { colors } from '../ui/palette';
import type { UIStyle } from '../ui/types';

export const styles = {
  frame: { flexDirection: 'column', gap: 10, width: '100%', padding: 12, backgroundColor: colors.page },
  row: { flexDirection: 'row', alignItems: 'center', flexWrap: 'wrap', gap: 8 },
  readerRow: { flexDirection: 'row', alignItems: 'stretch', gap: 10, minWidth: 0 },
  column: { flexDirection: 'column', gap: 12, minWidth: 0 },
  heading: { fontSize: 20, fontWeight: 700, color: colors.text },
  title: { fontSize: 15, fontWeight: 600, color: colors.text },
  body: { fontSize: 14, color: colors.text, lineHeight: 1.5 },
  muted: { fontSize: 12, color: colors.muted, lineHeight: 1.5 },
  source: { fontSize: 14, color: colors.text, maxLines: 1 },
  reader: { flexGrow: 1, flexShrink: 1, minWidth: 0 },
  drawer: {
    width: 360,
    flexShrink: 0,
    gap: 10,
    padding: 12,
    backgroundColor: colors.surface,
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.border,
  },
  quote: { fontSize: 13, color: colors.muted, lineHeight: 19 / 13 },
  mark: { padding: 12, gap: 8, borderRadius: 12, borderWidth: 1, borderColor: colors.border },
  selected: { backgroundColor: colors.accent, color: colors.onAccent },
  panel: { padding: 12, gap: 10, backgroundColor: colors.surface, borderRadius: 16 },
  danger: { color: colors.danger, fontSize: 13 },
  success: { color: colors.success, fontSize: 13 },
  pageField: { width: 100 },
} satisfies Record<string, UIStyle>;
