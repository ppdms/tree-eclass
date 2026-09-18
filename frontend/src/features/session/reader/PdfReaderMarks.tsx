import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { FilePenLine, MessageSquareText, Trash2, X } from 'lucide-react';
import { lookup } from '@/lib/display';
import { colors, typography } from '@/styles/tokens.stylex';
import type { Annotation } from '@/features/session/reader/types';
import { HIGHLIGHT_COLORS, HighlightToolbar } from './HighlightToolbar';
export { HighlightToolbar };

export interface HighlightRect {
  x: number;
  y: number;
  w: number;
  h: number;
}

const styles = stylex.create({
  notePosition: (top: string, left: string | null, right: string | null) => ({ left, right, top }),
  annotationMarkPosition: (left: string, top: string, width: string, height: string, backgroundColor: string) => ({
    backgroundColor,
    height,
    left,
    top,
    width,
  }),
  highlightTogglePosition: (left: string, top: string, width: string, height: string) => ({
    height,
    left,
    minHeight: '2rem',
    minWidth: '2rem',
    top,
    width,
  }),
  marksOverlay: {
    inset: 0,
    pointerEvents: 'none',
    position: 'absolute',
  },
  bookmarkMarker: {
    backgroundColor: colors.danger,
    clipPath: 'polygon(0% 0%, 100% 0%, 100% 100%, 50% 82%, 0% 100%)',
    display: 'block',
    filter: 'drop-shadow(0 2px 3px rgb(0 0 0 / 0.22))',
    position: 'absolute',
    zIndex: 2,
    height: '3.1rem',
    right: '1rem',
    top: '1.1rem',
    width: '2.15rem',
  },
  noteMarker: {
    padding: 0,
    borderColor: colors.surface,
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 2,
    alignItems: 'center',
    backgroundColor: {
      default: colors.info,
      ':hover': colors.infoStrong,
    },
    color: colors.background,
    cursor: 'pointer',
    display: 'grid',
    fontFamily: 'inherit',
    position: 'absolute',
    transform: { default: 'translate(-50%, -75%)', ':hover': 'translate(-50%, -85%) scale(1.08)' },
    transitionDuration: '120ms',
    transitionProperty: 'all',
    zIndex: 3,
  },
  pageMarker: {
    transform: 'translate(0, 0)',
  },
  noteMarkerActive: {
    opacity: 0,
    pointerEvents: 'none',
    visibility: 'hidden',
  },
  markerIcon: {
    height: '1rem',
    width: '1rem',
  },
  popover: {
    borderColor: colors.borderLight,
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 1,
    paddingInline: '0.85rem',
    backgroundColor: colors.surfaceRaised,
    boxShadow: '0 8px 24px rgb(0 0 0 / .18)',
    color: colors.textPrimary,
    fontSize: '0.78rem',
    lineHeight: 1.5,
    paddingBlockEnd: '0.85rem',
    paddingBlockStart: '0.8rem',
    position: 'absolute',
    zIndex: 5,
    width: 'min(18rem, calc(100% - 1.5rem))',
  },
  popoverLeft: { transform: 'translate(-.75rem,-.45rem)' },
  popoverRight: { transform: 'translate(.75rem,-.45rem)' },
  arrow: {
    borderBlockStyle: 'solid',
    blockSize: 0,
    borderBlockColor: 'transparent',
    borderBlockWidth: '.35rem',
    inlineSize: 0,
    position: 'absolute',
    top: '.15rem',
  },
  arrowLeft: {
    borderInlineStartColor: colors.surfaceRaised,
    filter: 'drop-shadow(1px 1px 1px rgb(25 22 12 / .16))',
    insetInlineEnd: '-.58rem',
  },
  arrowRight: {
    borderInlineEndColor: colors.surfaceRaised,
    filter: 'drop-shadow(-1px 1px 1px rgb(25 22 12 / .16))',
    insetInlineStart: '-.58rem',
  },
  popoverHeader: {
    gap: '0.5rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: typography.size2xs,
    fontWeight: typography.weightBold,
    justifyContent: 'space-between',
    letterSpacing: '0.08em',
    textTransform: 'uppercase',
    marginBottom: '0.55rem',
  },
  popoverHeaderTitle: {
    gap: '0.25rem',
    alignItems: 'center',
    display: 'inline-flex',
  },
  headerIcon: {
    height: '0.75rem',
    width: '0.75rem',
  },
  headerActions: {
    gap: '0.15rem',
    alignItems: 'center',
    display: 'flex',
  },
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
  annotationMark: {
    borderRadius: '0.5rem',
    position: 'absolute',
  },
  annotationMarkNote: {
    borderColor: colors.borderLight,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: 'transparent',
    borderBottomColor: colors.info,
    borderBottomStyle: 'solid',
    borderBottomWidth: 2,
  },
  highlightToggle: {
    padding: 0,
    borderWidth: 0,
    backgroundColor: { default: 'transparent', ':hover': 'rgb(245 158 11 / .12)' },
    cursor: 'pointer',
    outlineColor: { default: 'transparent', ':hover': 'rgb(217 119 6 / .55)' },
    outlineOffset: { default: 0, ':hover': -2 },
    outlineStyle: { default: 'none', ':hover': 'solid' },
    outlineWidth: { default: 0, ':hover': 2 },
    pointerEvents: 'auto',
    position: 'absolute',
    zIndex: 2,
  },
  highlightToggleOpen: {
    backgroundColor: 'rgb(245 158 11 / .12)',
  },
});

function hasBookmark(annotations: Annotation[]): boolean {
  return annotations.some((item) => item.kind === 'bookmark' && item.status !== 'deleted');
}

function BookmarkMarker() {
  return (
    <span {...stylex.props(styles.bookmarkMarker)} role="img" aria-label="Bookmarked page" title="Bookmarked page" />
  );
}

interface NotePlacement {
  top: string;
  left: string | null;
  right: string | null;
  side: 'left' | 'right';
}

function notePlacement(rect: HighlightRect | undefined): NotePlacement {
  if (!rect) return { top: '1.1rem', left: '4%', right: null, side: 'right' };
  const top = `${Math.max(1, rect.y * 100)}%`;
  if (rect.x + rect.w > 0.72) return { top, left: null, right: '4%', side: 'left' };
  return { left: `${Math.min(72, (rect.x + rect.w) * 100)}%`, top, right: null, side: 'right' };
}

export function NoteMarker({
  item,
  rect,
  active,
  onClick,
}: {
  item: Annotation;
  rect: HighlightRect | undefined;
  active: boolean;
  onClick: (event: React.MouseEvent<HTMLButtonElement>) => void;
}) {
  const placement = notePlacement(rect);
  return (
    <button
      type="button"
      {...stylex.props(
        styles.noteMarker,
        !rect && styles.pageMarker,
        active && styles.noteMarkerActive,
        styles.notePosition(placement.top, placement.left, placement.right),
      )}
      aria-label={item.body ? `Show note: ${item.body}` : 'Show note attached to selected text'}
      aria-expanded={active}
      aria-hidden={active}
      tabIndex={active ? -1 : 0}
      title={item.body || 'Open note attached to selected text'}
      onClick={(event) => {
        event.stopPropagation();
        event.currentTarget.blur();
        onClick(event);
      }}
    >
      <MessageSquareText aria-hidden="true" {...stylex.props(styles.markerIcon)} />
    </button>
  );
}

interface AnnotationPopoverProps {
  item: Annotation;
  rect: HighlightRect | undefined;
  onClose: () => void;
  onDelete: (id: string | number) => Promise<boolean>;
  deleting: boolean;
}

function AnnotationPopoverArrow({ side }: { side: 'left' | 'right' }) {
  return (
    <span aria-hidden="true" {...stylex.props(styles.arrow, side === 'left' ? styles.arrowLeft : styles.arrowRight)} />
  );
}

function AnnotationPopoverHeader({
  item,
  onClose,
  onDelete,
  deleting,
}: Pick<AnnotationPopoverProps, 'item' | 'onClose' | 'onDelete' | 'deleting'>) {
  return (
    <div {...stylex.props(styles.popoverHeader)}>
      <span {...stylex.props(styles.popoverHeaderTitle)}>
        <FilePenLine aria-hidden="true" {...stylex.props(styles.headerIcon)} /> Note
      </span>
      <div {...stylex.props(styles.headerActions)}>
        <button
          type="button"
          {...stylex.props(styles.iconBtn, styles.deleteBtn)}
          disabled={deleting}
          aria-label="Delete note"
          title={deleting ? 'Deleting note' : 'Delete note'}
          onClick={() => onDelete(item.id)}
        >
          <Trash2 aria-hidden="true" />
        </button>
        <button
          type="button"
          {...stylex.props(styles.iconBtn, styles.closeBtn)}
          aria-label="Close note"
          title="Close note"
          onClick={onClose}
        >
          <X aria-hidden="true" />
        </button>
      </div>
    </div>
  );
}

export function AnnotationPopover({ item, rect, onClose, onDelete, deleting }: AnnotationPopoverProps) {
  const placement = notePlacement(rect);
  return (
    <aside
      {...stylex.props(
        styles.popover,
        placement.side === 'left' ? styles.popoverLeft : styles.popoverRight,
        styles.notePosition(placement.top, placement.left, placement.right),
      )}
      role="dialog"
      aria-label="Note attached to selected text"
      onPointerDown={(event) => event.stopPropagation()}
    >
      <AnnotationPopoverArrow side={placement.side} />
      <AnnotationPopoverHeader item={item} onClose={onClose} onDelete={onDelete} deleting={deleting} />
      <p>{item.body || 'No note text yet.'}</p>
    </aside>
  );
}

function AnnotationMarks({ item }: { item: Annotation }) {
  const rects = item.rects || [];
  return (
    <>
      {rects.map((rect, index) => (
        <mark
          key={`${item.id}-${index}`}
          title={item.body || item.quote}
          {...stylex.props(
            styles.annotationMark,
            item.kind === 'note' && styles.annotationMarkNote,
            styles.annotationMarkPosition(
              `${rect.x * 100}%`,
              `${rect.y * 100}%`,
              `${rect.w * 100}%`,
              `${rect.h * 100}%`,
              item.status === 'orphaned'
                ? 'rgb(148 163 184 / .3)'
                : lookup(HIGHLIGHT_COLORS, item.color || 'yellow', HIGHLIGHT_COLORS.yellow),
            ),
          )}
        />
      ))}
    </>
  );
}

export function HighlightMarks({ annotations = [] }: { annotations?: Annotation[] }) {
  return (
    <div {...stylex.props(styles.marksOverlay)}>
      {hasBookmark(annotations) ? <BookmarkMarker /> : null}
      {annotations.map((item) => (
        <AnnotationMarks key={item.id} item={item} />
      ))}
    </div>
  );
}

export function HighlightToggle({
  item,
  rect,
  active,
  onClick,
}: {
  item: Annotation;
  rect: HighlightRect;
  active: boolean;
  onClick: (event: React.MouseEvent<HTMLButtonElement>) => void;
}) {
  const preview = (item.quote || item.body || 'selected text').slice(0, 80);
  return (
    <button
      type="button"
      {...stylex.props(
        styles.highlightToggle,
        active && styles.highlightToggleOpen,
        styles.highlightTogglePosition(`${rect.x * 100}%`, `${rect.y * 100}%`, `${rect.w * 100}%`, `${rect.h * 100}%`),
      )}
      aria-label={`${item.kind === 'note' ? 'Note' : 'Highlight'}: ${preview}`}
      aria-expanded={active}
      title={item.quote || item.body || 'Open annotation actions'}
      onClick={(event) => {
        event.stopPropagation();
        onClick(event);
      }}
    />
  );
}
