interface LoadingImage {
  image: HTMLImageElement;
  promise: Promise<HTMLImageElement>;
  users: number;
}
const maxDimension = 8192;
const maxPixels = 16_777_216;

function isImageUrl(source: string | Blob): source is string {
  return typeof source === 'string';
}

export class BrowserImageHost {
  readonly #loading = new Map<string, LoadingImage>();

  async decode(source: string | Blob, width?: number, height?: number, signal?: AbortSignal): Promise<ImageBitmap> {
    if ([width, height].some((value) => value !== undefined && (!Number.isFinite(value) || value <= 0))) {
      throw new Error('Invalid native image dimensions');
    }
    const objectUrl = isImageUrl(source) ? null : URL.createObjectURL(source);
    const url = new URL(objectUrl ?? String(source), location.href);
    if (!['http:', 'https:', 'blob:'].includes(url.protocol)) throw new Error('Unsupported native image protocol');
    const entry = this.#acquire(url);
    try {
      const image = await abortable(entry.promise, signal);
      const [outputWidth, outputHeight] = dimensions(image.naturalWidth, image.naturalHeight, width, height);
      const canvas = document.createElement('canvas');
      canvas.width = outputWidth;
      canvas.height = outputHeight;
      const context = canvas.getContext('2d');
      if (!context) throw new Error('Native image decoder is unavailable');
      context.drawImage(image, 0, 0, outputWidth, outputHeight);
      try {
        const bitmap = await createImageBitmap(canvas);
        if (signal?.aborted) {
          bitmap.close();
          throw new DOMException('Image decoding aborted', 'AbortError');
        }
        return bitmap;
      } finally {
        canvas.width = 0;
        canvas.height = 0;
      }
    } catch {
      throw new Error('Native image decoding failed');
    } finally {
      entry.users--;
      if (!entry.users) {
        this.#loading.delete(url.href);
        entry.image.removeAttribute('src');
      }
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    }
  }
  #acquire(url: URL): LoadingImage {
    const cached = this.#loading.get(url.href);
    if (cached) {
      cached.users++;
      return cached;
    }
    const image = new Image();
    if (url.origin !== location.origin && url.protocol !== 'blob:') image.crossOrigin = 'anonymous';
    const { promise, resolve, reject } = Promise.withResolvers<HTMLImageElement>();
    image.onload = () => {
      image.onload = null;
      image.onerror = null;
      if (image.naturalWidth > 0 && image.naturalHeight > 0) resolve(image);
      else reject(new Error('Native image has no intrinsic dimensions'));
    };
    image.onerror = () => {
      image.onload = null;
      image.onerror = null;
      reject(new Error('Native image could not be loaded'));
    };
    const entry = { image, promise, users: 1 };
    this.#loading.set(url.href, entry);
    image.src = url.href;
    return entry;
  }
}
async function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) throw new DOMException('Image decoding aborted', 'AbortError');
  const gate = Promise.withResolvers<T>();
  const abort = () => gate.reject(new DOMException('Image decoding aborted', 'AbortError'));
  signal.addEventListener('abort', abort, { once: true });
  void promise.then(gate.resolve, gate.reject);
  try {
    return await gate.promise;
  } finally {
    signal.removeEventListener('abort', abort);
  }
}
function dimensions(
  intrinsicWidth: number,
  intrinsicHeight: number,
  width?: number,
  height?: number,
): [number, number] {
  if (
    !Number.isFinite(intrinsicWidth) ||
    !Number.isFinite(intrinsicHeight) ||
    intrinsicWidth <= 0 ||
    intrinsicHeight <= 0
  )
    throw new Error('Invalid intrinsic native image dimensions');
  let scale =
    width === undefined
      ? height === undefined
        ? 1
        : height / intrinsicHeight
      : height === undefined
        ? width / intrinsicWidth
        : Math.min(width / intrinsicWidth, height / intrinsicHeight);
  scale = Math.min(
    scale,
    maxDimension / intrinsicWidth,
    maxDimension / intrinsicHeight,
    Math.sqrt(maxPixels / (intrinsicWidth * intrinsicHeight)),
  );
  return [Math.max(1, Math.floor(intrinsicWidth * scale)), Math.max(1, Math.floor(intrinsicHeight * scale))];
}
