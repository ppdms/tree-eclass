import { publishImage, releaseImage } from '../../ui/resources';
import type { PageSize } from '../../pdf';
import type { PageGeometry } from './geometry';
import { friendlyDocumentError, type ReaderSource } from './source';
import { settledPageWork } from './work';

export interface CachedPage {
  page: number;
  size: PageSize | null;
  geometry: PageGeometry | null;
  image: string | null;
  width: number;
  error: string | null;
  loading: boolean;
  abort: AbortController;
  geometryTask: Promise<PageGeometry> | null;
}
interface RenderedPageWork {
  bitmap: ImageBitmap | null;
}
let generation = 0;

export function rasterWidth(size: PageSize, zoom: number, dpr: number): number {
  const requested = size.width * zoom * dpr;
  // A single huge sheet must not exhaust the GPU; CSS geometry remains at the exact requested zoom.
  const pixelBound = Math.sqrt((16_777_216 * size.width) / size.height);
  const heightBound = (8192 * size.width) / size.height;
  return Math.max(1, Math.floor(Math.min(requested, pixelBound, heightBound, 8192)));
}

/**
 * Only requested pages retain text/GPU resources. Evicted in-flight pages retain
 * their scheduler slots until all public parser/render promises settle.
 */
export class PageCache {
  readonly pages = new Map<number, CachedPage>();
  readonly sizes = new Map<number, PageSize>();
  sizeVersion = 0;
  readonly #source: ReaderSource;
  readonly #changed: () => void;
  readonly #generation = ++generation;
  #disposed = false;
  #sequence = 0;
  #requested = new Set<number>();
  #active = 0;
  #zoom = 1;
  #dpr = 1;

  constructor(source: ReaderSource, changed: () => void) {
    this.#source = source;
    this.#changed = changed;
  }

  get identity(): number {
    return this.#generation;
  }

  update(pages: number[], zoom: number, dpr: number): void {
    if (this.#disposed) return;
    this.#zoom = zoom;
    this.#dpr = dpr;
    this.#requested = new Set(pages);
    for (const [number, entry] of this.pages) if (!this.#requested.has(number)) this.#evict(entry);
    for (const number of pages) {
      let entry = this.pages.get(number);
      if (!entry) {
        entry = {
          page: number,
          size: null,
          geometry: null,
          image: null,
          width: 0,
          error: null,
          loading: false,
          abort: new AbortController(),
          geometryTask: null,
        };
        this.pages.set(number, entry);
      }
    }
    this.#pump();
  }

  #pump(): void {
    if (this.#disposed) return;
    for (const number of this.#requested) {
      if (this.#active >= 2) return;
      const entry = this.pages.get(number);
      if (!entry) continue;
      const size = entry.size ?? this.sizes.get(number) ?? this.#source.firstSize;
      const width = rasterWidth(size, this.#zoom, this.#dpr);
      if (entry.loading || entry.error || (entry.image && entry.width === width)) continue;
      this.#active++;
      void this.#load(entry, this.#zoom, this.#dpr);
    }
  }

  async #load(entry: CachedPage, zoom: number, dpr: number): Promise<void> {
    entry.loading = true;
    try {
      entry.size ??= await this.#source.size(entry.page);
      if (!this.#live(entry)) return;
      if (this.sizes.get(entry.page) !== entry.size) {
        this.sizes.set(entry.page, entry.size);
        this.sizeVersion++;
        this.#changed();
      }
      const width = rasterWidth(entry.size, zoom, dpr);
      const [bitmap, geometry] = await this.#renderAndGeometry(entry, width);
      if (!this.#live(entry)) {
        bitmap.close();
        return;
      }
      const image = `reader:page:${this.#generation}:${entry.page}:${++this.#sequence}`;
      publishImage(image, bitmap);
      if (entry.image) releaseImage(entry.image);
      entry.image = image;
      entry.width = width;
      entry.geometry = geometry;
    } catch (error) {
      if (this.#live(entry)) entry.error = friendlyDocumentError(error);
    } finally {
      entry.loading = false;
      this.#active--;
      if (this.#live(entry)) this.#changed();
      else this.#source.releasePage(entry.page);
      this.#pump();
    }
  }

  async #renderAndGeometry(entry: CachedPage, width: number): Promise<[ImageBitmap, PageGeometry]> {
    const rendered: RenderedPageWork = { bitmap: null };
    const abort = new AbortController();
    try {
      const signal = AbortSignal.any([abort.signal, entry.abort.signal]);
      const fail = (cause: unknown): never => {
        abort.abort(cause);
        throw cause;
      };
      entry.geometryTask ??= this.#source.geometry(entry.page, signal).catch(fail);
      const raster = this.#source
        .raster(entry.page, width, signal)
        .then((value) => {
          if (signal.aborted) {
            value.close();
            signal.throwIfAborted();
          }
          rendered.bitmap = value;
          return value;
        })
        .catch(fail);
      return await settledPageWork([raster, entry.geometryTask] as const);
    } catch (error) {
      abort.abort();
      rendered.bitmap?.close();
      throw error;
    }
  }

  retry(page: number): void {
    const entry = this.pages.get(page);
    if (entry) this.#evict(entry);
    this.#changed();
  }

  #live(entry: CachedPage): boolean {
    return !this.#disposed && !entry.abort.signal.aborted && this.pages.get(entry.page) === entry;
  }

  #evict(entry: CachedPage): void {
    entry.abort.abort();
    if (entry.image) releaseImage(entry.image);
    this.pages.delete(entry.page);
    if (!entry.loading) this.#source.releasePage(entry.page);
  }

  destroy(): void {
    this.#disposed = true;
    for (const entry of this.pages.values()) this.#evict(entry);
    this.sizes.clear();
    this.#source.destroy();
  }
}
