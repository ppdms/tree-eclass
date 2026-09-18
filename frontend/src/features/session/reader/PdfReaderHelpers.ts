/**
 * Self-contained helpers extracted from PdfReader.tsx.
 *
 * Pure logic and hooks that do not depend on the reader's JSX components;
 * kept out of the component file so PdfReader.tsx stays focused on
 * rendering. Plain TS: this module contains no JSX.
 */

import { useEffect, useLayoutEffect, useMemo, useState, type RefObject } from 'react';
import * as pdfjs from 'pdfjs-dist';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import { z } from 'zod/v4';
import { errorLikeSchema, type ErrorLike } from '@/lib/errors';

import type { Annotation, SessionDocument } from '@/features/session/reader/types';

import type { Anchor } from './anchoring';
import { resolveAnchor, selectionToAnchor } from './anchoring';
import type { PageSize } from './PdfReaderScale';

export {
  fitScale,
  pageScale,
  readReaderPreferences,
  readerPreferenceKey,
  useFitToWidth,
  useFitToPage,
  scaleForMode,
  useNaturalPageSize,
  useResponsiveAutoFit,
  useScaleManagement,
} from './PdfReaderScale';
export interface SelectionScreenRect {
  top: number;
  bottom: number;
  left: number;
  width: number;
}

export type SelectionPayload = Anchor & {
  pageNumber: number;
  screenRect: SelectionScreenRect;
};

export interface UsePdfLoaderResult {
  pdf: PDFDocumentProxy | null;
  error: string | null;
  retry: () => void;
}

/** How far outside the viewport a page starts rendering, in viewport heights. */
const RENDER_MARGIN = 2;

function errorMessage(loadError: ErrorLike): string {
  const message = z.string().safeParse(loadError.message);
  return message.success ? message.data : '';
}

// pdf.js reports load failures with the whole request URL embedded in the
// message. The reader is inside the app, so the user needs to know what went
// wrong, not which internal endpoint did. Map the failure to a plain line and
// keep anything unrecognised as a short, URL-free summary.
export function friendlyDocumentError(message: string): string {
  const text = message;
  const lower = text.toLowerCase();
  if (lower.includes('404'))
    return 'This document is missing from the course files. It may have been moved or removed.';
  if (lower.includes('webdav') || lower.includes('storage'))
    return 'The course storage could not provide this document. Check Settings → Storage, then run a course check.';
  if (lower.includes('not indexed'))
    return 'This document has not been indexed yet. Run a course check and try again once the source is available.';
  if (lower.includes('superseded'))
    return 'This document is an older indexed version. Return to the course files and open the current source.';
  if (lower.includes('belongs to another course'))
    return (
      'This source is linked to a different course. Return to the course files and ' +
      'choose a document from this course.'
    );
  if (lower.includes('503')) return 'The course storage service is temporarily unavailable. Try again in a moment.';
  if (lower.includes('413')) return 'This document is too large for the reader. Open it outside the app instead.';
  if (/network|failed to fetch|connection/i.test(lower))
    return 'The document could not be downloaded. Check the connection and try again.';
  const stripped = text
    .replace(/https?:\/\/[^\s'"]+/g, '')
    .replace(/['"`][^'"`]*['"`]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  return stripped || 'The document could not be opened.';
}

export function usePageSize(pdf: PDFDocumentProxy | null, pageNumber: number, scale: number | null): PageSize {
  const [size, setSize] = useState<PageSize>({ width: 0, height: 0 });

  // Reserve the page's real height before rendering it, so lazy rendering
  // does not make the scrollbar jump around as pages appear.
  useEffect(() => {
    if (!pdf) {
      setSize({ width: 0, height: 0 });
      return undefined;
    }
    let cancelled = false;
    pdf.getPage(pageNumber).then((page) => {
      if (cancelled) return;
      const viewport = page.getViewport({ scale: scale ?? 1 });
      setSize({ width: viewport.width, height: viewport.height });
    });
    return () => {
      cancelled = true;
    };
  }, [pdf, pageNumber, scale]);

  return size;
}

// A page starts rendering once it's within RENDER_MARGIN viewport-heights of
// the visible area. "Which page is current" is a separate concern, handled
// once for the whole document by usePageTracking rather than per page here.
export function useLazyRender(
  holderRef: RefObject<HTMLElement | null>,
  onShouldRender: (shouldRender: boolean) => void,
): void {
  useEffect(() => {
    const node = holderRef.current;
    if (!node) return undefined;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) onShouldRender(true);
      },
      { rootMargin: `${RENDER_MARGIN * 100}% 0%`, threshold: 0 },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [holderRef, onShouldRender]);
}

// Re-anchor stored highlights against the page as actually rendered, and
// report which ones no longer match anything.
export function useAnchorReconciliation(
  rendered: boolean,
  textLayerRef: RefObject<HTMLElement | null>,
  annotations: Annotation[],
  onAnchorsResolved?: (unresolvedIds: (string | number)[]) => void,
): void {
  useLayoutEffect(() => {
    if (!rendered || !textLayerRef.current) return;
    const unresolved = annotations
      .filter((item) => item.quote && !(item.rects || []).length)
      .filter((item) => !resolveAnchor(item, textLayerRef.current))
      .map((item) => item.id);
    if (unresolved.length) onAnchorsResolved?.(unresolved);
  }, [rendered, annotations, onAnchorsResolved, textLayerRef]);
}

// A second, transient screen-space rect (not persisted, not the page-relative
// one in `anchor.rects`) purely so the floating selection toolbar can
// position itself at the selection.
export function selectionPayload(
  selection: Selection | null,
  pageNumber: number,
  textLayer: HTMLElement | null,
): SelectionPayload | null {
  if (!selection || selection.isCollapsed) return null;
  const anchor = selectionToAnchor(selection, textLayer);
  if (!anchor) return null;
  const screenRect = selection.getRangeAt(0).getBoundingClientRect();
  return {
    ...anchor,
    pageNumber,
    screenRect: {
      top: screenRect.top,
      bottom: screenRect.bottom,
      left: screenRect.left,
      width: screenRect.width,
    },
  };
}

export function usePdfLoader(doc: SessionDocument | null, courseId: string | number): UsePdfLoaderResult {
  const [pdf, setPdf] = useState<PDFDocumentProxy | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const documentId = doc?.document_id;
    if (!documentId) return undefined;
    let cancelled = false;
    setPdf(null);
    setError(null);

    // Native content URLs already carry course scope; replace it without duplicating the query.
    const url = new URL(doc.content_url || '', window.location.origin);
    url.searchParams.set('course_id', String(courseId));
    const task = pdfjs.getDocument({
      url: url.href,
      // Ranged fetching is why the endpoint exists: without it the whole
      // lecture is downloaded before the first page can be shown.
      disableRange: false,
      disableStream: false,
    });
    task.promise.then(
      (loaded) => {
        if (!cancelled) setPdf(loaded);
      },
      (loadError) => {
        if (!cancelled) {
          const parsed = errorLikeSchema.safeParse(loadError);
          const errorLike = parsed.success ? parsed.data : { message: String(loadError) };
          setError(errorMessage(errorLike) || 'The document could not be opened.');
        }
      },
    );
    return () => {
      cancelled = true;
      task.destroy();
    };
  }, [doc?.document_id, doc?.content_url, courseId, attempt]);

  return { pdf, error, retry: () => setAttempt((value) => value + 1) };
}

export function useAnnotationsByPage(annotations: Annotation[]): Map<number, Annotation[]> {
  return useMemo(() => {
    const grouped = new Map<number, Annotation[]>();
    (annotations || []).forEach((item) => {
      if (item.status === 'deleted') return;
      const list = grouped.get(item.page_number) || [];
      list.push(item);
      grouped.set(item.page_number, list);
    });
    return grouped;
  }, [annotations]);
}
