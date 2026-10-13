// pdf.js running inside the render worker: pages rasterize into OffscreenCanvas
// and reach the Canvas2D painter as ImageBitmaps. Parsing stays in pdf.js's own nested worker.
// Every 2D context pdf.js draws with is instrumented for transfer functions (transfer.ts).
import * as pdfjs from 'pdfjs-dist';
import type { PDFDocumentProxy, PageViewport, RenderTask } from 'pdfjs-dist';
import pdfWorkerUrl from 'pdfjs-dist/build/pdf.worker.min.mjs?url';
import type { getTextContentParameters, PDFOperatorList, TextContent } from 'pdfjs-dist/types/src/display/api';
import { instrument, TransferFilters } from './transfer';
import { PdfWorkLifetime } from './pdfLifetime';
import { workerFonts } from './ui/fonts';

declare module 'pdfjs-dist/types/src/display/api' {
  // pdf.js draws into any canvas exposing getContext('2d'), width and height; its
  // declarations only name HTMLCanvasElement. The worker passes an OffscreenCanvas.
  interface PDFPageProxy {
    render(params: { canvas: OffscreenCanvas; viewport: PageViewport }): RenderTask;
    // GetTextContent's parser stream emits the same chunks drained by getTextContent().
    streamTextContent(params?: getTextContentParameters): ReadableStream<TextContent>;
  }
}

export type FontMode = 'FontFace in worker' | 'glyph paths';

export interface PageSize {
  width: number;
  height: number;
}

export interface RenderedPage {
  bitmap: ImageBitmap;
  ms: number;
}

/** A page's pdf.js text content and its scale-1 viewport transform (PDF points, y down). */
export interface PageTextSource {
  content: TextContent;
  transform: number[];
}

export interface PdfFontMetrics {
  matrix: number[];
  vertical: boolean;
  type3: boolean;
  defaultVMetrics: number[] | null;
}

interface CanvasSlot {
  canvas: OffscreenCanvas | null;
  context: OffscreenCanvasRenderingContext2D | null;
}

/** Scratch canvases for pdf.js (patterns, masks, groups) without a document. */
class OffscreenCanvasFactory {
  create(width: number, height: number): CanvasSlot {
    const canvas = new OffscreenCanvas(width, height);
    const context = canvas.getContext('2d');
    if (context) instrument(context);
    return { canvas, context };
  }

  reset(slot: CanvasSlot, width: number, height: number): void {
    if (!slot.canvas) throw new Error('Canvas is not specified');
    slot.canvas.width = width;
    slot.canvas.height = height;
  }

  destroy(slot: CanvasSlot): void {
    if (slot.canvas) {
      slot.canvas.width = 0;
      slot.canvas.height = 0;
    }
    slot.canvas = null;
    slot.context = null;
  }
}

let parser: Worker | null = null;
let parserHandle: pdfjs.PDFWorker | null = null;
let parserRelease: Promise<void> | null = null;
const documentReleases = new WeakMap<PDFDocumentProxy['loadingTask'], Promise<void>>();

/** The shared parser must finish Terminate before a replacement source starts work. */
function releaseDocument(task: PDFDocumentProxy['loadingTask']): Promise<void> {
  const existing = documentReleases.get(task);
  if (existing) return existing;
  const release = task.destroy();
  documentReleases.set(task, release);
  parserRelease = parserRelease ? Promise.all([parserRelease, release]).then(() => {}) : release;
  void parserRelease.catch(() => {});
  return release;
}

function documentOptions() {
  parser ??= new Worker(pdfWorkerUrl, { type: 'module' });
  pdfjs.GlobalWorkerOptions.workerPort = parser;
  // Explicit ownership keeps one document's abort from destroying the shared port during the next open.
  parserHandle ??= pdfjs.PDFWorker.create({ port: parser });
  const fonts = workerFonts();
  // SAFETY: workerFonts checked the native FontFaceSet and FontFace APIs. With
  // both document-backed factories replaced, pdf.js's FontLoader is the only
  // ownerDocument consumer, and its native font path only uses this fonts set.
  const ownerDocument = fonts ? ({ fonts } as Document) : undefined;
  return {
    CanvasFactory: OffscreenCanvasFactory,
    worker: parserHandle,
    FilterFactory: TransferFilters,
    disableFontFace: fonts === null,
    ownerDocument,
  };
}

export class PdfPages {
  static readonly operations = pdfjs.OPS;
  readonly name: string;
  readonly mode: FontMode;
  readonly sizes: PageSize[];
  readonly #doc: PDFDocumentProxy;
  readonly #lifetime = new PdfWorkLifetime();
  #destroyed = false;

  get pageCount(): number {
    return this.#doc.numPages;
  }

  constructor(doc: PDFDocumentProxy, name: string, mode: FontMode, sizes: PageSize[]) {
    this.#doc = doc;
    this.name = name;
    this.mode = mode;
    this.sizes = sizes;
  }

  static async open(data: ArrayBuffer, name: string): Promise<PdfPages> {
    await parserRelease;
    const task = pdfjs.getDocument({
      data,
      ...documentOptions(),
    });
    try {
      const doc = await task.promise;
      const sizes = await Promise.all(
        Array.from({ length: doc.numPages }, async (_, index) => {
          const viewport = (await doc.getPage(index + 1)).getViewport({ scale: 1 });
          return { width: viewport.width, height: viewport.height };
        }),
      );
      return new PdfPages(doc, name, workerFonts() ? 'FontFace in worker' : 'glyph paths', sizes);
    } catch (error) {
      await releaseDocument(task);
      throw error;
    }
  }

  /** Starts ranged/streamed loading, resolving after only the requested page's size. */
  static async openUrl(
    url: string,
    name: string,
    requestedPage: number,
    signal: AbortSignal,
    headers?: Record<string, string>,
  ): Promise<PdfPages> {
    signal.throwIfAborted();
    await parserRelease;
    signal.throwIfAborted();
    const task = pdfjs.getDocument({
      // PDF.js resolves string URLs through window.location; a URL object is worker-safe.
      url: new URL(url, self.location.origin),
      httpHeaders: headers,
      disableRange: false,
      disableStream: false,
      ...documentOptions(),
    });
    let opened: PdfPages | null = null;
    const abort = () => {
      if (opened) opened.destroy();
      else void releaseDocument(task).catch(() => {});
    };
    signal.addEventListener('abort', abort, { once: true });
    try {
      const doc = await task.promise;
      signal.throwIfAborted();
      const pages = new PdfPages(doc, name, workerFonts() ? 'FontFace in worker' : 'glyph paths', []);
      opened = pages;
      await pages.size(Math.max(0, Math.min(doc.numPages - 1, requestedPage - 1)));
      signal.throwIfAborted();
      return pages;
    } catch (error) {
      opened?.destroy();
      await releaseDocument(task);
      throw error;
    } finally {
      signal.removeEventListener('abort', abort);
    }
  }

  async size(index: number): Promise<PageSize> {
    if (this.sizes[index]) return this.sizes[index];
    const viewport = (await this.#page(index)).getViewport({ scale: 1 });
    return (this.sizes[index] = { width: viewport.width, height: viewport.height });
  }

  /** Rasterizes one page at `deviceWidth` pixels wide. */
  async render(index: number, deviceWidth: number, signal?: AbortSignal): Promise<RenderedPage> {
    signal?.throwIfAborted();
    const started = performance.now();
    const page = await this.#page(index);
    signal?.throwIfAborted();
    const unit = page.getViewport({ scale: 1 });
    const viewport = page.getViewport({ scale: deviceWidth / unit.width });
    const canvas = new OffscreenCanvas(Math.round(viewport.width), Math.round(viewport.height));
    // pdf.js asks for this exact context; getContext returns the same, now instrumented, instance.
    const context = canvas.getContext('2d', { alpha: false, willReadFrequently: true });
    if (!context) throw new Error('No 2D context for the page');
    const transfer = instrument(context);
    const task = page.render({ canvas, viewport });
    try {
      // RenderTask.cancel() rejects before PDF.js's delayed operator-stream abort.
      // Account for the whole public render promise instead of releasing a page
      // slot while that parser job is still running. Evicted pixels are discarded.
      await this.#lifetime.run(task.promise);
      signal?.throwIfAborted();
      transfer.flush();
      if (!signal) page.cleanup();
      return { bitmap: canvas.transferToImageBitmap(), ms: performance.now() - started };
    } finally {
      canvas.width = canvas.height = 0;
    }
  }

  async text(index: number, signal?: AbortSignal): Promise<PageTextSource> {
    signal?.throwIfAborted();
    const page = await this.#page(index);
    signal?.throwIfAborted();
    let content: TextContent;
    if (!signal || page.isPureXfa) {
      content = await this.#lifetime.run(page.getTextContent());
    } else {
      const reader = page.streamTextContent().getReader();
      content = { items: [], styles: Object.create(null), lang: null };
      try {
        // PDF.js 6.2.108's stream cancel ACK does not await its parser WorkerTask.
        // Drain instead: discarded text retains no new chunks, and its cache slot
        // remains occupied until parsing finishes (or document Terminate ACKs).
        await this.#lifetime.run(
          (async () => {
            while (true) {
              const { value, done } = await reader.read();
              if (done) break;
              if (signal.aborted) continue;
              content.lang ??= value.lang;
              Object.assign(content.styles, value.styles);
              content.items.push(...value.items);
            }
          })(),
        );
      } finally {
        reader.releaseLock();
      }
    }
    signal?.throwIfAborted();
    return { content, transform: page.getViewport({ scale: 1 }).transform };
  }

  async operators(index: number, signal?: AbortSignal): Promise<PDFOperatorList> {
    signal?.throwIfAborted();
    const page = await this.#page(index);
    signal?.throwIfAborted();
    // No public cancellable operator stream exists. Do not reject early and leave
    // an unaccounted parser job behind; eviction waits for this public promise.
    const operators = await this.#lifetime.run(page.getOperatorList());
    signal?.throwIfAborted();
    return operators;
  }

  async fontMetrics(
    index: number,
    operators: PDFOperatorList,
    signal?: AbortSignal,
  ): Promise<Record<string, PdfFontMetrics>> {
    signal?.throwIfAborted();
    const page = await this.#page(index);
    signal?.throwIfAborted();
    const names = new Set<string>();
    for (let i = 0; i < operators.fnArray.length; i++) {
      if (operators.fnArray[i] === pdfjs.OPS.setFont) names.add(String(operators.argsArray[i][0]));
      else if (operators.fnArray[i] === pdfjs.OPS.setGState) {
        for (const [key, value] of operators.argsArray[i][0]) {
          if (key === 'Font') names.add(String(value[0]));
        }
      }
    }
    const result: Record<string, PdfFontMetrics> = {};
    await this.#lifetime.run(
      Promise.all(
        [...names].map(async (name) => {
          // PDFObjects resolves dependencies through this callback without copying embedded font bytes.
          const font = await new Promise<{
            fontMatrix?: number[];
            vertical?: boolean;
            isType3Font?: boolean;
            defaultVMetrics?: number[];
          }>((resolve) => page.commonObjs.get(name, resolve));
          result[name] = {
            matrix: font.fontMatrix ?? [0.001, 0, 0, 0.001, 0, 0],
            vertical: font.vertical ?? false,
            type3: font.isType3Font ?? false,
            defaultVMetrics: font.defaultVMetrics ?? null,
          };
        }),
      ),
    );
    signal?.throwIfAborted();
    return result;
  }

  #page(index: number) {
    if (this.#destroyed) return Promise.reject(new DOMException('PDF source was released', 'AbortError'));
    return this.#lifetime.run(this.#doc.getPage(index + 1));
  }

  async releasePage(index: number): Promise<void> {
    if (this.#destroyed) return;
    const page = await this.#page(index);
    if (!this.#destroyed) page.cleanup();
  }

  destroy(): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    this.#lifetime.close(releaseDocument(this.#doc.loadingTask));
  }
}
