import * as stylex from '@stylexjs/stylex';
import type { RefObject } from 'react';
import { effects } from '@/styles/tokens.stylex';
import type { Annotation } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from './api';
import { AnnotationActions, HighlightMarks, PageSkeleton, PageTextLayer } from './PdfReaderStates';

interface PdfReaderPageProps {
  holder: RefObject<HTMLDivElement | null>;
  canvas: RefObject<HTMLCanvasElement | null>;
  textLayer: RefObject<HTMLDivElement | null>;
  pageNumber: number;
  size: { width: number; height: number };
  scale: number | null;
  rendered: boolean;
  annotations: Annotation[];
  onMouseUp: () => void;
  onDeleteAnnotation: (id: string | number) => Promise<boolean>;
  onUpdateAnnotation: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
}

const styles = stylex.create({
  page: {
    borderRadius: '0.375rem',
    marginInline: 'auto',
    backgroundColor: 'transparent',
    boxShadow: effects.shadowLarge,
    position: 'relative',
    marginBottom: '1.75rem',
  },
  pageSize: (width: number | null, height: number | null) => ({ height, width }),
  canvas: {
    borderRadius: '0.375rem',
    backgroundColor: '#ffffff',
    display: 'block',
  },
});

export function PdfReaderPage({
  holder,
  canvas,
  textLayer,
  pageNumber,
  size,
  scale,
  rendered,
  annotations,
  onMouseUp,
  onDeleteAnnotation,
  onUpdateAnnotation,
}: PdfReaderPageProps) {
  return (
    <div
      ref={holder}
      data-page={pageNumber}
      {...stylex.props(styles.page, styles.pageSize(size.width || null, size.height || null))}
    >
      <canvas ref={canvas} {...stylex.props(styles.canvas)} />
      {!rendered && <PageSkeleton pageNumber={pageNumber} />}
      <PageTextLayer textLayerRef={textLayer} size={size} scale={scale} onMouseUp={onMouseUp} />
      <HighlightMarks annotations={annotations} />
      <AnnotationActions annotations={annotations} onDelete={onDeleteAnnotation} onUpdate={onUpdateAnnotation} />
    </div>
  );
}
