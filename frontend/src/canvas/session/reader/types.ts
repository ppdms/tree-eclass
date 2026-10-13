import type {
  Annotation as SourceAnnotation,
  AnnotationRect,
  SessionDocument as SourceDocument,
} from '@/features/session/reader/types';

export type { AnnotationRect };
export type SessionDocument = SourceDocument & { source_hash?: string; mime_type?: string };
export type Annotation = SourceAnnotation & { document_id?: string | number; source_hash?: string };

export function annotationAnchorId(annotation: Annotation): string {
  const documentId = encodeURIComponent(String(annotation.document_id ?? ''));
  return `reader:annotation:${documentId}:${encodeURIComponent(String(annotation.id))}`;
}

export interface ReaderState {
  ready: boolean;
  pageCount: number;
  zoom: number;
  mode: 'width' | 'page' | 'manual';
  error: string | null;
}

export interface ReaderSelection {
  documentId: string;
  page: number;
  quote: string;
  prefix: string;
  suffix: string;
  char_start: number;
  char_end: number;
  rects: AnnotationRect[];
  bounds: { x: number; y: number; width: number; height: number };
}

export interface ReaderProps {
  document: SessionDocument;
  initialPage: number;
  height: number;
  width: number;
  annotations: Annotation[];
  onPageChange(page: number): void;
  onSelectionChange(selection: ReaderSelection | null): void;
  onAnnotationClick(annotation: Annotation): void;
  onStateChange(state: ReaderState): void;
  onContextMenu?(event: { x: number; y: number; page: number; selection: ReaderSelection | null }): void;
}

export interface ReaderHandle {
  goToPage(page: number): void;
  fitWidth(): void;
  fitPage(): void;
  zoomIn(): void;
  zoomOut(): void;
  clearSelection(): void;
  focusAnnotation(annotation: Annotation): void;
}

export interface Bounds {
  x: number;
  y: number;
  width: number;
  height: number;
}
