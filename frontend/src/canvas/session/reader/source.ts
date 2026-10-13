import { request } from '../../app/api';
import { decodeImage } from '../../app/browser';
import { PdfPages } from '../../pdf';
import type { PageSize } from '../../pdf';
import { pdfGlyphPositions } from './pdfPositions';
import { pageGeometry, type PageGeometry } from './geometry';
import type { SessionDocument } from './types';
import { settledPageWork } from './work';

export interface ReaderSource {
  pageCount: number;
  firstSize: PageSize;
  size(page: number): Promise<PageSize>;
  raster(page: number, width: number, signal: AbortSignal): Promise<ImageBitmap>;
  geometry(page: number, signal: AbortSignal): Promise<PageGeometry>;
  releasePage(page: number): void;
  destroy(): void;
}

export function sourceIdentity(document: SessionDocument): string {
  return JSON.stringify([
    String(document.document_id),
    document.source_hash ?? '',
    document.content_url ?? '',
    document.mime_type ?? '',
    document.document_kind ?? '',
    document.renderable,
  ]);
}

export function sourceFormat(document: SessionDocument): 'pdf' | 'image' | 'unsupported' {
  if (document.renderable === false) return 'unsupported';
  const kind = document.document_kind?.toLowerCase();
  const mime = document.mime_type?.toLowerCase();
  if (kind === 'image' || mime?.startsWith('image/')) return 'image';
  if (kind === 'pdf' || mime === 'application/pdf' || (!kind && !mime && document.renderable)) return 'pdf';
  return 'unsupported';
}

/** Terminal-fallback classification for the session shell; does not assert pixel readiness. */
export function readerSupportsSource(document: SessionDocument): boolean {
  return sourceFormat(document) !== 'unsupported';
}

function pdfSource(pages: PdfPages, firstSize: PageSize, operations: Record<string, number>): ReaderSource {
  return {
    pageCount: pages.pageCount,
    firstSize,
    size: (page) => pages.size(page - 1),
    raster: async (page, width, signal) => (await pages.render(page - 1, width, signal)).bitmap,
    geometry: async (page, signal) => {
      const [text, operators, size] = await settledPageWork([
        pages.text(page - 1, signal),
        pages.operators(page - 1, signal),
        pages.size(page - 1),
      ] as const);
      signal.throwIfAborted();
      const fonts = await pages.fontMetrics(page - 1, operators, signal);
      signal.throwIfAborted();
      return pageGeometry(
        text.content,
        text.transform,
        size,
        pdfGlyphPositions(operators, operations, fonts, text.transform),
      );
    },
    releasePage: (page) => {
      void pages.releasePage(page - 1).catch(() => {});
    },
    destroy: () => pages.destroy(),
  };
}

async function imageSource(url: string, signal: AbortSignal, headers?: Record<string, string>): Promise<ReaderSource> {
  const response = await request(url, { signal, headers });
  if (!response.ok) throw new Error(`Document request failed (${response.status})`);
  let blobReference: Blob | null = await response.blob();
  const bitmap = await decodeImage(blobReference, { signal });
  if (signal.aborted) {
    bitmap.close();
    signal.throwIfAborted();
  }
  const width = Math.min(612, bitmap.width);
  const size = { width, height: (width * bitmap.height) / bitmap.width };
  let firstBitmap: ImageBitmap | null = bitmap;
  return {
    pageCount: 1,
    firstSize: size,
    size: async () => size,
    raster: async (_page, width, abort) => {
      abort.throwIfAborted();
      const blob = blobReference;
      if (!blob) throw new DOMException('Reader source was released', 'AbortError');
      const targetWidth = Math.max(1, Math.round(width));
      const targetHeight = Math.max(1, Math.round((width * size.height) / size.width));
      const original = firstBitmap;
      firstBitmap = null;
      const resized = original
        ? await resizeFirstImage(original, targetWidth, targetHeight)
        : await decodeImage(blob, { width: targetWidth, height: targetHeight, signal: abort });
      if (abort.aborted) {
        resized.close();
        abort.throwIfAborted();
      }
      return resized;
    },
    geometry: async () => ({ text: '', spans: [], size }),
    releasePage: () => {},
    destroy: () => {
      firstBitmap?.close();
      firstBitmap = null;
      blobReference = null;
    },
  };
}

async function resizeFirstImage(bitmap: ImageBitmap, width: number, height: number): Promise<ImageBitmap> {
  if (bitmap.width === width && bitmap.height === height) return bitmap;
  try {
    return await createImageBitmap(bitmap, { resizeWidth: width, resizeHeight: height, resizeQuality: 'high' });
  } finally {
    bitmap.close();
  }
}

export async function openSource(document: SessionDocument, page: number, signal: AbortSignal): Promise<ReaderSource> {
  if (!document.content_url) throw new Error('This document has no indexed source URL.');
  if (!document.source_hash) {
    throw new Error('The workspace did not provide a source revision. Open current course files.');
  }
  const headers = { 'If-Match': `"sha256-${document.source_hash}"` };
  if (sourceFormat(document) === 'image') return imageSource(document.content_url, signal, headers);
  signal.throwIfAborted();
  const pages = await PdfPages.openUrl(document.content_url, document.display_name ?? '', page, signal, headers);
  if (signal.aborted) {
    pages.destroy();
    signal.throwIfAborted();
  }
  const requested = Math.max(1, Math.min(pages.pageCount, page));
  return pdfSource(pages, await pages.size(requested - 1), PdfPages.operations);
}

export function friendlyDocumentError(cause: unknown): string {
  const message = cause instanceof Error ? cause.message : String(cause);
  const lower = message.toLowerCase();
  if (lower.includes('412'))
    return 'This source changed since the workspace opened. Return to the current course files.';
  if (lower.includes('404'))
    return 'This document is missing from the course files. It may have moved or been removed.';
  if (/webdav|storage/.test(lower)) return 'Storage could not provide this document. Check Settings / Storage.';
  if (lower.includes('not indexed')) return 'This document has not been indexed yet. Run a course check and try again.';
  if (lower.includes('superseded'))
    return 'This is an older indexed version. Open the current source from course files.';
  if (lower.includes('another course')) return 'Choose a document belonging to this course from its course files.';
  if (lower.includes('503')) return 'The course storage service is temporarily unavailable. Try again in a moment.';
  if (lower.includes('413')) return 'This document is too large for the reader. Open the original source instead.';
  if (/network|failed to fetch|connection/.test(lower))
    return 'The document could not be downloaded. Check the connection.';
  if (/password/.test(lower)) return 'This PDF requires a password. Open the original source in a PDF viewer.';
  return (
    message
      .replace(/https?:\/\/[^\s'"]+/g, '')
      .replace(/['"`][^'"`]*['"`]/g, '')
      .replace(/\s+/g, ' ')
      .trim() || 'The document could not be opened.'
  );
}
