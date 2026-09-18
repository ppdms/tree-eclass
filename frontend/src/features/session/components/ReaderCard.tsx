import * as stylex from '@stylexjs/stylex';
import { forwardRef, type Ref } from 'react';
import PdfReader, { type PdfReaderHandle } from '@/features/session/reader/PdfReader';
import type { Annotation, SessionDocument } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from '@/features/session/reader/api';
import type { SelectionPayload } from '@/features/session/reader/PdfReaderHelpers';

export interface ReaderCardProps {
  readerRef: React.RefObject<HTMLDivElement | null>;
  activeDocument: SessionDocument | null;
  courseId: string | number;
  annotations: Annotation[];
  onPageChange: (page: number) => void;
  onScaleChange: (scale: number | null, mode: string) => void;
  onSelect: (payload: SelectionPayload) => void;
  onContextMenu: (event: React.MouseEvent) => void;
  onDeleteAnnotation: (id: string | number) => Promise<boolean>;
  onUpdateAnnotation: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
}

const styles = stylex.create({
  readerPane: {
    overflow: 'hidden',
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    height: '100%',
    minWidth: 0,
  },
});

const ReaderCard = forwardRef<PdfReaderHandle, ReaderCardProps>(function ReaderCard(
  {
    readerRef,
    activeDocument,
    courseId,
    annotations,
    onPageChange,
    onScaleChange,
    onSelect,
    onContextMenu,
    onDeleteAnnotation,
    onUpdateAnnotation,
  },
  ref: Ref<PdfReaderHandle>,
) {
  return (
    <div ref={readerRef} {...stylex.props(styles.readerPane)} onContextMenu={onContextMenu}>
      <PdfReader
        ref={ref}
        document={activeDocument}
        courseId={courseId}
        annotations={annotations}
        onPageChange={onPageChange}
        onScaleChange={onScaleChange}
        onSelect={onSelect}
        onDeleteAnnotation={onDeleteAnnotation}
        onUpdateAnnotation={onUpdateAnnotation}
        onOrphaned={() => {
          /* The server has already recorded orphaning; the panel
             shows it on the next load rather than mutating here. */
        }}
      />
    </div>
  );
});

export default ReaderCard;
