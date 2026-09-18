import * as stylex from '@stylexjs/stylex';
/**
 * The reader pane.
 *
 * Pages are rendered lazily and only near the viewport: a lecture PDF here runs
 * to several hundred pages, and rendering all of them would freeze the tab for
 * the sake of pages nobody is looking at. Zoom and "current page" are owned
 * here (they need the scroller and the loaded pdf.js document, both local to
 * this component) and exposed to the surrounding toolbar via `onPageChange` /
 * `onScaleChange` callbacks plus an imperative `zoomIn`/`zoomOut` ref, rather
 * than being lifted wholesale — the toolbar is a sibling, not a parent.
 */

import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
  type Ref,
  type RefObject,
} from 'react';
import { isServer } from '@/lib/display';
import * as pdfjs from 'pdfjs-dist';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import { colors } from '@/styles/tokens.stylex';
import { DocumentLoadError, EmptyDocument, UnrenderableDocument } from './PdfReaderStates';
import type { Annotation, SessionDocument } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from './api';
import {
  friendlyDocumentError,
  selectionPayload,
  type SelectionPayload,
  useAnchorReconciliation,
  useAnnotationsByPage,
  useLazyRender,
  usePageSize,
  usePdfLoader,
  useScaleManagement,
} from './PdfReaderHelpers';
import { usePageTracking } from './usePageTracking';
import { PdfReaderPage } from './PdfReaderPage';
import { PdfReaderLoading } from './PdfReaderLoading';
export { friendlyDocumentError };

export interface PdfReaderHandle {
  zoomIn: () => void;
  zoomOut: () => void;
  fitToWidth: () => void;
  fitPage: () => void;
}

export interface PdfReaderProps {
  document: SessionDocument | null;
  courseId: string | number;
  onPageChange?: (page: number) => void;
  onScaleChange?: (scale: number | null, mode: string) => void;
  annotations: Annotation[];
  onSelect: (payload: SelectionPayload) => void;
  onOrphaned: (ids: (string | number)[]) => void;
  onDeleteAnnotation: (id: string | number) => Promise<boolean>;
  onUpdateAnnotation: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
}

pdfjs.GlobalWorkerOptions.workerSrc = new URL('pdfjs-dist/build/pdf.worker.min.mjs', import.meta.url).toString();

const styles = stylex.create({
  sessionReader: {
    overflow: 'auto',
    backgroundColor: colors.background,
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    paddingBlockEnd: 'clamp(1rem, 2vw, 2rem)',
    paddingBlockStart: 'clamp(1rem, 2vw, 2rem)',
    paddingInlineEnd: 'clamp(1rem, 2vw, 2rem)',
    paddingInlineStart: 'clamp(1rem, 2vw, 2rem)',
    minWidth: 0,
  },
});

async function renderPageToCanvas(
  pdf: PDFDocumentProxy,
  pageNumber: number,
  scale: number,
  target: HTMLCanvasElement,
  textLayer: HTMLElement,
  onRendered: (rendered: boolean) => void,
): Promise<{ cancel: () => void }> {
  const page = await pdf.getPage(pageNumber);
  const viewport = page.getViewport({ scale });
  const ratio = !isServer() ? window.devicePixelRatio || 1 : 1;
  target.width = Math.floor(viewport.width * ratio);
  target.height = Math.floor(viewport.height * ratio);
  const rootFontSize = Number.parseFloat(getComputedStyle(document.documentElement).fontSize) || 16;
  target.style.width = `${viewport.width / rootFontSize}rem`;
  target.style.height = `${viewport.height / rootFontSize}rem`;

  const context = target.getContext('2d');
  if (!context) return { cancel: () => {} };
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  const task = page.render({ canvasContext: context, canvas: target, viewport });
  try {
    await task.promise;
  } catch (error) {
    if (error instanceof Error && error.name !== 'RenderingCancelledException') throw error;
    return { cancel: () => {} };
  }

  textLayer.replaceChildren();
  const layer = new pdfjs.TextLayer({
    textContentSource: await page.getTextContent(),
    container: textLayer,
    viewport,
  });
  await layer.render();
  onRendered(true);
  return { cancel: () => task.cancel() };
}

function usePageRender(
  pdf: PDFDocumentProxy | null,
  pageNumber: number,
  scale: number | null,
  shouldRender: boolean,
  canvasRef: RefObject<HTMLCanvasElement | null>,
  textLayerRef: RefObject<HTMLElement | null>,
  onRendered: (rendered: boolean) => void,
): void {
  useEffect(() => {
    if (!shouldRender || !pdf || scale === null) return undefined;
    const target = canvasRef.current;
    const textLayer = textLayerRef.current;
    if (!target || !textLayer) return undefined;
    let cancelled = false;
    let task: { cancel: () => void } | null = null;

    (async () => {
      const result = await renderPageToCanvas(pdf, pageNumber, scale, target, textLayer, onRendered);
      if (cancelled) return;
      task = result;
    })();

    return () => {
      cancelled = true;
      task?.cancel();
    };
  }, [pdf, pageNumber, scale, shouldRender, onRendered, canvasRef, textLayerRef]);
}

interface PageProps {
  pdf: PDFDocumentProxy | null;
  pageNumber: number;
  scale: number | null;
  annotations: Annotation[];
  onSelect: PdfReaderProps['onSelect'];
  onAnchorsResolved: PdfReaderProps['onOrphaned'];
  onDeleteAnnotation: PdfReaderProps['onDeleteAnnotation'];
  onUpdateAnnotation: PdfReaderProps['onUpdateAnnotation'];
}

function Page({
  pdf,
  pageNumber,
  scale,
  annotations,
  onSelect,
  onAnchorsResolved,
  onDeleteAnnotation,
  onUpdateAnnotation,
}: PageProps) {
  const holder = useRef<HTMLDivElement>(null);
  const canvas = useRef<HTMLCanvasElement>(null);
  const textLayer = useRef<HTMLDivElement>(null);
  const size = usePageSize(pdf, pageNumber, scale);
  const [shouldRender, setShouldRender] = useState(false);
  const [rendered, setRendered] = useState(false);

  useLazyRender(holder, setShouldRender);
  usePageRender(pdf, pageNumber, scale, shouldRender, canvas, textLayer, setRendered);
  useAnchorReconciliation(rendered, textLayer, annotations, onAnchorsResolved);

  const handleMouseUp = useCallback(() => {
    requestAnimationFrame(() => {
      const payload = selectionPayload(window.getSelection(), pageNumber, textLayer.current);
      if (payload) onSelect(payload);
    });
  }, [onSelect, pageNumber]);

  return (
    <PdfReaderPage
      holder={holder}
      canvas={canvas}
      textLayer={textLayer}
      pageNumber={pageNumber}
      size={size}
      scale={scale}
      rendered={rendered}
      annotations={annotations}
      onMouseUp={handleMouseUp}
      onDeleteAnnotation={onDeleteAnnotation}
      onUpdateAnnotation={onUpdateAnnotation}
    />
  );
}

interface ReaderBodyProps {
  scrollerRef: RefObject<HTMLDivElement | null>;
  error: string | null;
  pdf: PDFDocumentProxy | null;
  scale: number | null;
  byPage: Map<number, Annotation[]>;
  onSelect: PdfReaderProps['onSelect'];
  onOrphaned: PdfReaderProps['onOrphaned'];
  onRetry: () => void;
  doc: SessionDocument | null;
  onDeleteAnnotation: PdfReaderProps['onDeleteAnnotation'];
  onUpdateAnnotation: PdfReaderProps['onUpdateAnnotation'];
}

function ReaderPages({
  pdf,
  scale,
  byPage,
  onSelect,
  onOrphaned,
  onDeleteAnnotation,
  onUpdateAnnotation,
}: {
  pdf: PDFDocumentProxy;
  scale: number;
  byPage: Map<number, Annotation[]>;
  onSelect: PdfReaderProps['onSelect'];
  onOrphaned: PdfReaderProps['onOrphaned'];
  onDeleteAnnotation: PdfReaderProps['onDeleteAnnotation'];
  onUpdateAnnotation: PdfReaderProps['onUpdateAnnotation'];
}) {
  return Array.from({ length: pdf.numPages }, (_, index) => (
    <Page
      key={index + 1}
      pdf={pdf}
      pageNumber={index + 1}
      scale={scale}
      annotations={byPage.get(index + 1) || []}
      onSelect={onSelect}
      onAnchorsResolved={onOrphaned}
      onDeleteAnnotation={onDeleteAnnotation}
      onUpdateAnnotation={onUpdateAnnotation}
    />
  ));
}

function ReaderBodyContent({
  error,
  pdf,
  scale,
  byPage,
  onSelect,
  onOrphaned,
  onRetry,
  doc,
  onDeleteAnnotation,
  onUpdateAnnotation,
}: ReaderBodyProps) {
  if (error) {
    return (
      <DocumentLoadError message={friendlyDocumentError(error)} onRetry={onRetry} downloadUrl={doc?.download_url} />
    );
  }
  if (!pdf || scale === null) {
    return <PdfReaderLoading pdf={pdf} doc={doc} />;
  }
  return (
    <ReaderPages
      pdf={pdf}
      scale={scale}
      byPage={byPage}
      onSelect={onSelect}
      onOrphaned={onOrphaned}
      onDeleteAnnotation={onDeleteAnnotation}
      onUpdateAnnotation={onUpdateAnnotation}
    />
  );
}

function ReaderBody(props: ReaderBodyProps) {
  return (
    <div ref={props.scrollerRef} {...stylex.props(styles.sessionReader)}>
      <ReaderBodyContent {...props} />
    </div>
  );
}

function useReaderScaleReport(
  scale: number | null,
  mode: string,
  onScaleChange: PdfReaderProps['onScaleChange'],
): void {
  useEffect(() => {
    onScaleChange?.(scale, mode);
  }, [scale, mode, onScaleChange]);
}

function usePdfReaderApi(
  ref: Ref<PdfReaderHandle> | null,
  setManualScale: (updater: (value: number | null) => number | null) => void,
  fitToWidth: () => void,
  fitPage: () => void,
): void {
  useImperativeHandle(
    ref,
    () => ({
      zoomIn: () => setManualScale((value) => Math.min(3, (value ?? 1) + 0.15)),
      zoomOut: () => setManualScale((value) => Math.max(0.3, (value ?? 1) - 0.15)),
      fitToWidth,
      fitPage,
    }),
    [fitPage, fitToWidth, setManualScale],
  );
}

const PdfReader = forwardRef<PdfReaderHandle, PdfReaderProps>(function PdfReader(
  {
    document: doc,
    courseId,
    onPageChange,
    onScaleChange,
    annotations,
    onSelect,
    onOrphaned,
    onDeleteAnnotation,
    onUpdateAnnotation,
  },
  ref,
) {
  const scroller = useRef<HTMLDivElement>(null);
  const { pdf, error, retry } = usePdfLoader(doc, courseId);
  const [scale, setScale] = useState<number | null>(null);
  const { setManualScale, fitToWidth, fitPage, mode } = useScaleManagement(doc, pdf, scroller, setScale, scale);
  const byPage = useAnnotationsByPage(annotations);

  usePageTracking(scroller, pdf?.numPages || 0, onPageChange);
  useReaderScaleReport(scale, mode, onScaleChange);
  usePdfReaderApi(ref, setManualScale, fitToWidth, fitPage);

  if (!doc) return <EmptyDocument />;
  if (!doc.renderable) return <UnrenderableDocument doc={doc} />;

  return (
    <ReaderBody
      scrollerRef={scroller}
      error={error}
      pdf={pdf}
      scale={scale}
      byPage={byPage}
      onSelect={onSelect}
      onOrphaned={onOrphaned}
      onRetry={retry}
      doc={doc}
      onDeleteAnnotation={onDeleteAnnotation}
      onUpdateAnnotation={onUpdateAnnotation}
    />
  );
});

export default PdfReader;
