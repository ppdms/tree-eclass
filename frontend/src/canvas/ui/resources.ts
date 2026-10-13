export interface RasterResource {
  image: ImageBitmap;
  width: number;
  height: number;
  revision: number;
}

const images = new Map<string, RasterResource>();
let invalidate: (() => void) | undefined;
let revision = 0;

export function configureImages(changed: () => void): void {
  if (invalidate) throw new Error('Canvas image ownership was already initialized');
  invalidate = changed;
}

/** Called by committed resource owners; the registry owns and eventually closes the bitmap. */
export function publishImage(id: string, bitmap: ImageBitmap): void {
  if (!invalidate) {
    bitmap.close();
    throw new Error('Canvas image ownership has not been initialized');
  }
  images.get(id)?.image.close();
  images.set(id, { image: bitmap, width: bitmap.width, height: bitmap.height, revision: ++revision });
  invalidate();
}

export function releaseImage(id: string): void {
  const resource = images.get(id);
  if (!resource) return;
  images.delete(id);
  resource.image.close();
  invalidate?.();
}

export function rasterImage(id: string): RasterResource | undefined {
  return images.get(id);
}
