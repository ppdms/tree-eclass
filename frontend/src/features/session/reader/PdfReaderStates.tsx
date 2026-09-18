import * as stylex from '@stylexjs/stylex';
import { useEffect, useRef, useState } from 'react';
import { colors } from '@/styles/tokens.stylex';
import type { Annotation } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from './api';
import { AnnotationPopover, HighlightToolbar, HighlightToggle, NoteMarker } from './PdfReaderMarks';

export { DocumentLoadError, EmptyDocument, UnrenderableDocument } from './DocumentStates';
export { HighlightMarks } from './PdfReaderMarks';

const styles = stylex.create({
  skeleton: {
    inset: 0,
    borderColor: colors.border,
    borderWidth: '1px',
    placeItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'grid',
    fontSize: '0.75rem',
    position: 'absolute',
  },
  textLayer: {
    inset: 0,
    overflow: 'clip',
    backgroundColor: 'transparent',
    caretColor: 'CanvasText',
    color: 'transparent',
    colorScheme: 'light dark',
    forcedColorAdjust: 'none',
    letterSpacing: 'normal',
    lineHeight: 1,
    opacity: 1,
    position: 'absolute',
    textAlign: 'initial',
    textSizeAdjust: 'none',
    transformOrigin: '0 0',
    wordSpacing: 'normal',
    zIndex: 0,
  },
});

export function PageSkeleton({ pageNumber }: { pageNumber: number }) {
  return (
    <div {...stylex.props(styles.skeleton)} aria-hidden="true">
      <span>Page {pageNumber}</span>
    </div>
  );
}

export function PageTextLayer({
  textLayerRef,
  size,
  scale,
  onMouseUp,
}: {
  textLayerRef: React.RefObject<HTMLDivElement | null>;
  size: { width: number; height: number };
  scale: number | null;
  onMouseUp: () => void;
}) {
  // SAFETY: Custom CSS property for PDF.js textlayer scale calculation;
  // cast to CSSProperties allows custom CSS variables on inline style objects.
  const style = {
    width: size.width,
    height: size.height,
    '--total-scale-factor': scale ?? 1,
  } as React.CSSProperties;
  return (
    <div
      ref={textLayerRef}
      onMouseUp={onMouseUp}
      onKeyUp={onMouseUp}
      className={stylex.props(styles.textLayer).className}
      style={style}
    />
  );
}

function useAnnotationDismissal(closeAnnotation: () => void): void {
  useEffect(() => {
    const close = (event: PointerEvent): void => {
      const target = event.target;
      if (
        !(target instanceof Element) ||
        !target.closest?.(
          '.reader-note-marker, .reader-highlight-toggle, .reader-annotation-popover, .reader-highlight-toolbar',
        )
      )
        closeAnnotation();
    };
    window.addEventListener('pointerdown', close);
    return () => window.removeEventListener('pointerdown', close);
  }, [closeAnnotation]);
}

interface AnnotationDeletion {
  deletingId: string | number | null;
  handleDelete: (id: string | number) => Promise<boolean>;
}

function useAnnotationDeletion(
  onDelete: (id: string | number) => Promise<boolean>,
  closeAnnotation: () => void,
): AnnotationDeletion {
  const [deletingId, setDeletingId] = useState<string | number | null>(null);
  const handleDelete = async (id: string | number): Promise<boolean> => {
    setDeletingId(id);
    try {
      const deleted = await onDelete?.(id);
      if (deleted !== false) closeAnnotation();
      return deleted !== false;
    } finally {
      setDeletingId(null);
    }
  };
  return { deletingId, handleDelete };
}

function AnnotationMarkers({
  highlights,
  notes,
  openId,
  onToggle,
}: {
  highlights: Annotation[];
  notes: Annotation[];
  openId: string | number | null;
  onToggle: (id: string | number, event: React.MouseEvent<HTMLButtonElement>) => void;
}) {
  return (
    <>
      {highlights.map((item) => {
        const rect = (item.rects || [])[0];
        return rect ? (
          <HighlightToggle
            key={`${item.id}-toggle`}
            item={item}
            rect={rect}
            active={item.id === openId}
            onClick={(event) => onToggle(item.id, event)}
          />
        ) : null;
      })}
      {notes.map((item) => (
        <NoteMarker
          key={`${item.id}-marker`}
          item={item}
          rect={(item.rects || [])[0]}
          active={item.id === openId}
          onClick={(event) => onToggle(item.id, event)}
        />
      ))}
    </>
  );
}

function OpenAnnotation({
  item,
  deleting,
  onClose,
  onDelete,
  onUpdate,
}: {
  item: Annotation | undefined;
  deleting: boolean;
  onClose: () => void;
  onDelete: (id: string | number) => Promise<boolean>;
  onUpdate: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
}) {
  if (item?.kind === 'note') {
    return (
      <AnnotationPopover
        item={item}
        rect={(item.rects || [])[0]}
        onClose={onClose}
        onDelete={onDelete}
        deleting={deleting}
      />
    );
  }
  if (item?.kind === 'highlight') {
    return (
      <HighlightToolbar
        item={item}
        rect={(item.rects || [])[0]}
        onClose={onClose}
        onDelete={onDelete}
        onUpdate={onUpdate}
        deleting={deleting}
      />
    );
  }
  return null;
}

export interface AnnotationActionsProps {
  annotations?: Annotation[];
  onDelete: (id: string | number) => Promise<boolean>;
  onUpdate: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
}

interface AnnotationOpenState {
  openId: string | number | null;
  openAnnotation: Annotation | undefined;
  closeAnnotation: () => void;
  onToggle: (id: string | number, event: React.MouseEvent<HTMLButtonElement>) => void;
}

function useAnnotationOpenState(textMarks: Annotation[]): AnnotationOpenState {
  const [openId, setOpenId] = useState<string | number | null>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const closeAnnotation = (): void => {
    setOpenId(null);
    requestAnimationFrame(() => returnFocusRef.current?.focus());
  };
  useEffect(() => {
    if (!textMarks.some((item) => item.id === openId)) closeAnnotation();
  }, [textMarks, openId, closeAnnotation]);
  useEffect(() => {
    if (!openId) return undefined;
    const closeOnEscape = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') closeAnnotation();
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [openId, closeAnnotation]);
  const openAnnotation = textMarks.find((item) => item.id === openId);
  const onToggle = (id: string | number, event: React.MouseEvent<HTMLButtonElement>): void => {
    if (openId === id) return closeAnnotation();
    returnFocusRef.current = event?.currentTarget;
    setOpenId(id);
  };
  return { openId, openAnnotation, closeAnnotation, onToggle };
}

export function AnnotationActions({ annotations = [], onDelete, onUpdate }: AnnotationActionsProps) {
  const textMarks = annotations.filter((item) => item.kind === 'note' || item.kind === 'highlight');
  const notes = textMarks.filter((item) => item.kind === 'note');
  const highlights = textMarks.filter((item) => item.kind === 'highlight');
  const { openId, openAnnotation, closeAnnotation, onToggle } = useAnnotationOpenState(textMarks);
  useAnnotationDismissal(closeAnnotation);
  const { deletingId, handleDelete } = useAnnotationDeletion(onDelete, closeAnnotation);
  return (
    <>
      <AnnotationMarkers highlights={highlights} notes={notes} openId={openId} onToggle={onToggle} />
      <OpenAnnotation
        item={openAnnotation}
        deleting={deletingId === openAnnotation?.id}
        onClose={closeAnnotation}
        onDelete={handleDelete}
        onUpdate={onUpdate}
      />
    </>
  );
}
