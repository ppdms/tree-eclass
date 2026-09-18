import * as stylex from '@stylexjs/stylex';
import { Highlighter, Trash2, X } from 'lucide-react';
import { lookup } from '@/lib/display';
import { colors, typography } from '@/styles/tokens.stylex';
import { HIGHLIGHT_COLORS, type Annotation } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from './api';
import type { HighlightRect } from './PdfReaderMarks';

export { HIGHLIGHT_COLORS };
const HIGHLIGHT_COLOR_KEYS = Object.keys(HIGHLIGHT_COLORS);

const styles = stylex.create({
  toolbar: {
    borderColor: colors.borderLight,
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.35rem',
    paddingBlock: '0.3rem',
    paddingInline: '0.35rem',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    display: 'flex',
    position: 'absolute',
    zIndex: 6,
    maxWidth: 'calc(100% - 1rem)',
    width: 'max-content',
  },
  toolbarLeft: { transform: 'translate(-.65rem,-.25rem)' },
  toolbarRight: { transform: 'translate(.65rem,-.25rem)' },
  notePosition: (top: string, left: string | null, right: string | null) => ({ left, right, top }),
  label: {
    gap: '0.25rem',
    paddingInline: '0.25rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.size2xs,
    fontWeight: typography.weightBold,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
  },
  iconWarning: {
    color: colors.warning,
  },
  colorsWrapper: {
    gap: '0.2rem',
    paddingInline: '0.2rem',
    alignItems: 'center',
    display: 'flex',
  },
  swatch: {
    borderColor: 'transparent',
    borderRadius: '999px',
    borderStyle: 'solid',
    borderWidth: 2,
    boxShadow: 'inset 0 0 0 1px rgb(0 0 0 / .18)',
    cursor: 'pointer',
    minBlockSize: 32,
    minInlineSize: 32,
    transform: { default: null, ':hover': 'scale(1.12)' },
    transitionProperty: 'transform',
  },
  swatchSelected: {
    borderColor: colors.textPrimary,
    outlineColor: colors.focusRing,
    outlineStyle: 'solid',
    outlineWidth: 1,
  },
  swatchBg: (backgroundColor: string) => ({ backgroundColor }),
  iconBtn: {
    padding: 0,
    borderRadius: '0.375rem',
    borderWidth: 0,
    placeItems: 'center',
    backgroundColor: 'transparent',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'grid',
    height: '2.75rem',
    minHeight: '2.75rem',
    minWidth: '2.75rem',
    width: '2.75rem',
  },
  deleteBtn: {
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceDanger },
    color: { default: colors.textSecondary, ':hover': colors.danger },
  },
  closeBtn: {
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
  },
});

function notePlacement(rect: HighlightRect | undefined) {
  if (!rect) return { top: '1.1rem', left: '4%', right: null, side: 'right' as const };
  const top = `${Math.max(1, rect.y * 100)}%`;
  if (rect.x + rect.w > 0.72) return { top, left: null, right: '4%', side: 'left' as const };
  return {
    left: `${Math.min(72, (rect.x + rect.w) * 100)}%`,
    top,
    right: null,
    side: 'right' as const,
  };
}

export interface HighlightToolbarProps {
  item: Annotation;
  rect: HighlightRect | undefined;
  onClose: () => void;
  onDelete: (id: string | number) => Promise<boolean>;
  onUpdate: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
  deleting: boolean;
}

function HighlightColorSwatches({ item, onUpdate }: { item: Annotation; onUpdate: HighlightToolbarProps['onUpdate'] }) {
  return (
    <div {...stylex.props(styles.colorsWrapper)} aria-label="Highlight color">
      {HIGHLIGHT_COLOR_KEYS.map((color) => (
        <button
          key={color}
          type="button"
          {...stylex.props(
            styles.swatch,
            item.color === color && styles.swatchSelected,
            styles.swatchBg(lookup(HIGHLIGHT_COLORS, color, HIGHLIGHT_COLORS.yellow)),
          )}
          aria-label={`Change highlight color to ${color}`}
          aria-pressed={item.color === color}
          title={`Change highlight color to ${color}`}
          onClick={() => onUpdate?.(item.id, { color })}
        />
      ))}
    </div>
  );
}

export function HighlightToolbar({ item, rect, onClose, onDelete, onUpdate, deleting }: HighlightToolbarProps) {
  const placement = notePlacement(rect);
  return (
    <div
      {...stylex.props(
        styles.toolbar,
        placement.side === 'left' ? styles.toolbarLeft : styles.toolbarRight,
        styles.notePosition(placement.top, placement.left, placement.right),
      )}
      role="group"
      aria-label="Highlight toolbar"
      onPointerDown={(event) => event.stopPropagation()}
    >
      <span {...stylex.props(styles.label)}>
        <Highlighter aria-hidden="true" {...stylex.props(styles.iconWarning)} /> Highlight
      </span>
      <HighlightColorSwatches item={item} onUpdate={onUpdate} />
      <button
        type="button"
        {...stylex.props(styles.iconBtn, styles.deleteBtn)}
        disabled={deleting}
        aria-label="Delete highlight"
        title={deleting ? 'Deleting highlight' : 'Delete highlight'}
        onClick={() => onDelete(item.id)}
      >
        <Trash2 aria-hidden="true" />
      </button>
      <button
        type="button"
        {...stylex.props(styles.iconBtn, styles.closeBtn)}
        aria-label="Close highlight toolbar"
        title="Close highlight toolbar"
        onClick={onClose}
      >
        <X aria-hidden="true" />
      </button>
    </div>
  );
}
