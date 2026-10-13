// PDF transfer functions (ExtGState /TR, /TR2) for pdf.js inside the render worker.
// pdf.js applies FilterFactory.addFilter's result as the canvas `filter` drawing state;
// its DOM factory builds an SVG feComponentTransfer (out = map[in] on unpremultiplied
// sRGB bytes, alpha untouched), which needs a document. Here addFilter returns a
// sentinel, and instrumented contexts draw while it is active into a transparent
// layer that is mapped through the tables and composited before anything that could
// observe or reorder those pixels (clip changes, state pops, pixel reads, use as a
// source). Other factory methods return 'none': pdf.js 6 has JS fallbacks for soft
// masks and knockout groups, and high-contrast/selection filters are unused here.

type Ctx = OffscreenCanvasRenderingContext2D;
type Maps = readonly (Uint8Array | null)[];
type PathOrRule = Path2D | CanvasFillRule | undefined;
type ImageArgs =
  | [image: CanvasImageSource, dx: number, dy: number]
  | [image: CanvasImageSource, dx: number, dy: number, dw: number, dh: number]
  | [
      image: CanvasImageSource,
      sx: number,
      sy: number,
      sw: number,
      sh: number,
      dx: number,
      dy: number,
      dw: number,
      dh: number,
    ];
/** Drawing calls that a transfer filter affects; both the native methods and a layer context satisfy it. */
type Drawer = Pick<
  Ctx,
  'fill' | 'clip' | 'stroke' | 'fillRect' | 'strokeRect' | 'fillText' | 'strokeText' | 'drawImage'
>;
type Natives = Drawer &
  Pick<Ctx, 'save' | 'restore' | 'setTransform' | 'putImageData' | 'clearRect' | 'getImageData' | 'createPattern'>;

interface Lut {
  red: Uint8Array;
  green: Uint8Array;
  blue: Uint8Array;
}

/** Device-pixel bounds. */
interface Bounds {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

interface Frame {
  filter: string;
  clipped: boolean;
}

const PREFIX = 'url(#tree-transfer-';
const IDENTITY = Uint8Array.from({ length: 256 }, (_, index) => index);
const nativeFilter = Object.getOwnPropertyDescriptor(OffscreenCanvasRenderingContext2D.prototype, 'filter');
/** Sentinel filter → tables, shared by every document's factory (pdf.js creates factories itself). */
const tables = new Map<string, Lut>();
const contexts = new WeakMap<OffscreenCanvas, TransferContext>();
let nextId = 1;

/** FilterFactory for pdf.js getDocument; one sentinel per distinct transfer function. */
export class TransferFilters {
  readonly #ids = new Map<string, string>();

  addFilter(maps: Maps | null): string {
    if (!maps) return 'none';
    // One function applies to all channels; four are C, M, Y, K, of which pdf.js maps the first three to R, G, B.
    const channel = (index: number) => (maps.length === 1 ? maps[0] : maps[index]) ?? IDENTITY;
    const lut = { red: channel(0), green: channel(1), blue: channel(2) };
    const key = [lut.red, lut.green, lut.blue].map((table) => table.join(',')).join(';');
    let id = this.#ids.get(key);
    if (!id) {
      id = `${PREFIX}${nextId++})`;
      this.#ids.set(key, id);
      tables.set(id, lut);
    }
    return id;
  }

  addHCMFilter = none;
  addAlphaFilter = none;
  addLuminosityFilter = none;
  addKnockoutFilter = none;
  addHighlightHCMFilter = none;
  addSelectionHCMFilter = none;
  addSelectionFilter = none;

  createSelectionStyle(): null {
    return null;
  }

  destroy(): void {
    for (const id of this.#ids.values()) tables.delete(id);
    this.#ids.clear();
  }
}

function none(): string {
  return 'none';
}

function transformed(ctx: Ctx, x: number, y: number, width: number, height: number): Bounds {
  const matrix = ctx.getTransform();
  const points = [
    [x, y],
    [x + width, y],
    [x, y + height],
    [x + width, y + height],
  ].map(([px = 0, py = 0]) => matrix.transformPoint({ x: px, y: py }));
  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  return { left: Math.min(...xs), top: Math.min(...ys), right: Math.max(...xs), bottom: Math.max(...ys) };
}

function textBounds(ctx: Ctx, text: string, x: number, y: number): Bounds {
  const metrics = ctx.measureText(text);
  const left = x - metrics.actualBoundingBoxLeft - 2;
  const top = y - metrics.actualBoundingBoxAscent - 2;
  const width = metrics.actualBoundingBoxLeft + metrics.actualBoundingBoxRight + 4;
  return transformed(ctx, left, top, width, metrics.actualBoundingBoxAscent + metrics.actualBoundingBoxDescent + 4);
}

function imageBounds(ctx: Ctx, args: ImageArgs): Bounds | null {
  if (args.length === 3) return null;
  if (args.length === 5) return transformed(ctx, args[1], args[2], args[3], args[4]);
  return transformed(ctx, args[5], args[6], args[7], args[8]);
}

// Methods are called on their receiver: the layer's are unbound, only the natives are bound.
function drawImageOn(target: Drawer, args: ImageArgs): void {
  if (args.length === 3) target.drawImage(args[0], args[1], args[2]);
  else if (args.length === 5) target.drawImage(args[0], args[1], args[2], args[3], args[4]);
  else target.drawImage(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
}

function fillOn(target: Drawer, clip: boolean, pathOrRule: PathOrRule, rule?: CanvasFillRule): void {
  if (pathOrRule instanceof Path2D) {
    if (clip) target.clip(pathOrRule, rule);
    else target.fill(pathOrRule, rule);
  } else if (clip) target.clip(pathOrRule ?? rule);
  else target.fill(pathOrRule ?? rule);
}

/** Copies the drawing state pdf.js sets (the layer has no clip; the composite applies the target's). */
function copyState(from: Ctx, to: Ctx): void {
  to.setTransform(from.getTransform());
  to.globalAlpha = from.globalAlpha;
  to.fillStyle = from.fillStyle;
  to.strokeStyle = from.strokeStyle;
  to.lineWidth = from.lineWidth;
  to.lineCap = from.lineCap;
  to.lineJoin = from.lineJoin;
  to.miterLimit = from.miterLimit;
  to.setLineDash(from.getLineDash());
  to.lineDashOffset = from.lineDashOffset;
  to.font = from.font;
  to.textAlign = from.textAlign;
  to.textBaseline = from.textBaseline;
  to.imageSmoothingEnabled = from.imageSmoothingEnabled;
}

/** A canvas used as an image source must first show its own pending transfer-mapped pixels. */
function settleSource(source: CanvasImageSource): void {
  if (source instanceof OffscreenCanvas) contexts.get(source)?.flush();
}

/** Installs transfer-filter emulation on a context (instance overrides; prototypes stay untouched). */
export function instrument(ctx: Ctx): TransferContext {
  return contexts.get(ctx.canvas) ?? new TransferContext(ctx);
}

export class TransferContext {
  readonly #ctx: Ctx;
  readonly #native: Natives;
  readonly #frames: Frame[] = [];
  #filter = 'none';
  #lut: Lut | null = null;
  #layer: Ctx | null = null;
  #dirty: Bounds | null = null;

  constructor(ctx: Ctx) {
    this.#ctx = ctx;
    // Bound originals keep their overloads; the instance overrides below shadow the prototype.
    this.#native = {
      fill: ctx.fill.bind(ctx),
      stroke: ctx.stroke.bind(ctx),
      fillRect: ctx.fillRect.bind(ctx),
      strokeRect: ctx.strokeRect.bind(ctx),
      fillText: ctx.fillText.bind(ctx),
      strokeText: ctx.strokeText.bind(ctx),
      drawImage: ctx.drawImage.bind(ctx),
      save: ctx.save.bind(ctx),
      restore: ctx.restore.bind(ctx),
      clip: ctx.clip.bind(ctx),
      setTransform: ctx.setTransform.bind(ctx),
      putImageData: ctx.putImageData.bind(ctx),
      clearRect: ctx.clearRect.bind(ctx),
      getImageData: ctx.getImageData.bind(ctx),
      createPattern: ctx.createPattern.bind(ctx),
    };
    contexts.set(ctx.canvas, this);
    this.#installDrawing();
    this.#installState();
  }

  #installDrawing(): void {
    const ctx = this.#ctx;
    ctx.fill = (pathOrRule?: PathOrRule, rule?: CanvasFillRule) =>
      this.#draw((target) => fillOn(target, false, pathOrRule, rule), null);
    ctx.stroke = (path?: Path2D) => this.#draw((target) => (path ? target.stroke(path) : target.stroke()), null);
    ctx.fillRect = (x: number, y: number, width: number, height: number) =>
      this.#draw((target) => target.fillRect(x, y, width, height), transformed(ctx, x, y, width, height));
    ctx.strokeRect = (x: number, y: number, width: number, height: number) => {
      const pad = ctx.lineWidth / 2 + 1;
      const bounds = transformed(ctx, x - pad, y - pad, width + 2 * pad, height + 2 * pad);
      this.#draw((target) => target.strokeRect(x, y, width, height), bounds);
    };
    ctx.fillText = (text: string, x: number, y: number, maxWidth?: number) =>
      this.#draw((target) => target.fillText(text, x, y, maxWidth), textBounds(ctx, text, x, y));
    ctx.strokeText = (text: string, x: number, y: number, maxWidth?: number) =>
      this.#draw((target) => target.strokeText(text, x, y, maxWidth), textBounds(ctx, text, x, y));
    ctx.drawImage = (...args: ImageArgs) => {
      settleSource(args[0]);
      this.#draw((target) => drawImageOn(target, args), imageBounds(ctx, args));
    };
    ctx.createPattern = (image: CanvasImageSource, repetition: string | null) => {
      settleSource(image);
      return this.#native.createPattern(image, repetition);
    };
  }

  #installState(): void {
    const ctx = this.#ctx;
    ctx.save = () => {
      this.#frames.push({ filter: this.#filter, clipped: false });
      this.#native.save();
    };
    ctx.restore = () => {
      const frame = this.#frames.pop();
      // Restoring a clip or a different filter changes how later draws relate to pending pixels.
      if (frame && (frame.clipped || frame.filter !== this.#filter)) this.flush();
      if (frame) this.#setFilterState(frame.filter);
      this.#native.restore();
    };
    ctx.clip = (pathOrRule?: PathOrRule, rule?: CanvasFillRule) => {
      this.flush();
      const frame = this.#frames[this.#frames.length - 1];
      if (frame) frame.clipped = true;
      fillOn(this.#native, true, pathOrRule, rule);
    };
    ctx.putImageData = (data: ImageData, dx: number, dy: number) => {
      this.flush();
      this.#native.putImageData(data, dx, dy);
    };
    ctx.clearRect = (x: number, y: number, width: number, height: number) => {
      this.flush();
      this.#native.clearRect(x, y, width, height);
    };
    ctx.getImageData = (sx: number, sy: number, sw: number, sh: number) => {
      this.flush();
      return this.#native.getImageData(sx, sy, sw, sh);
    };
    Object.defineProperty(ctx, 'filter', {
      configurable: true,
      get: () => this.#filter,
      set: (value: string) => {
        if (value === this.#filter) return;
        this.flush();
        this.#setFilterState(value);
        nativeFilter?.set?.call(ctx, this.#lut ? 'none' : value);
      },
    });
  }

  #setFilterState(filter: string): void {
    this.#filter = filter;
    this.#lut = tables.get(filter) ?? null;
  }

  /** Draws natively, or into the layer while a transfer filter is active. */
  #draw(paint: (target: Drawer) => void, bounds: Bounds | null): void {
    const ctx = this.#ctx;
    if (!this.#lut) {
      paint(this.#native);
      return;
    }
    const layer = this.#ensureLayer();
    // Non-default compositing reads the destination: give each such draw its own layer.
    const composite = ctx.globalCompositeOperation;
    if (composite !== 'source-over') this.flush();
    copyState(ctx, layer);
    paint(layer);
    const area = bounds ?? { left: 0, top: 0, right: ctx.canvas.width, bottom: ctx.canvas.height };
    const dirty = this.#dirty ?? area;
    this.#dirty = {
      left: Math.min(dirty.left, area.left),
      top: Math.min(dirty.top, area.top),
      right: Math.max(dirty.right, area.right),
      bottom: Math.max(dirty.bottom, area.bottom),
    };
    if (composite !== 'source-over') this.flush(composite);
  }

  #ensureLayer(): Ctx {
    const { width, height } = this.#ctx.canvas;
    if (this.#layer?.canvas.width !== width || this.#layer.canvas.height !== height) {
      const layer = new OffscreenCanvas(width, height).getContext('2d', { willReadFrequently: true });
      if (!layer) throw new Error('No 2D context for the transfer-function layer');
      this.#layer = layer;
      this.#dirty = null;
    }
    return this.#layer;
  }

  /** Maps pending layer pixels through the active tables and composites them onto the target. */
  flush(composite: GlobalCompositeOperation = 'source-over'): void {
    const layer = this.#layer;
    const lut = this.#lut;
    const dirty = this.#dirty;
    this.#dirty = null;
    if (!layer || !lut || !dirty) return;
    const left = Math.max(0, Math.floor(dirty.left));
    const top = Math.max(0, Math.floor(dirty.top));
    const width = Math.min(layer.canvas.width, Math.ceil(dirty.right)) - left;
    const height = Math.min(layer.canvas.height, Math.ceil(dirty.bottom)) - top;
    if (width <= 0 || height <= 0) return;
    const pixels = layer.getImageData(left, top, width, height);
    const data = pixels.data;
    // getImageData returns unpremultiplied bytes, which is what feComponentTransfer maps.
    for (let i = 0; i < data.length; i += 4) {
      if (data[i + 3] === 0) continue;
      data[i] = lut.red[data[i] ?? 0] ?? 0;
      data[i + 1] = lut.green[data[i + 1] ?? 0] ?? 0;
      data[i + 2] = lut.blue[data[i + 2] ?? 0] ?? 0;
    }
    layer.putImageData(pixels, left, top);
    const ctx = this.#ctx;
    this.#native.save();
    // Natives, not instance methods: pdf.js may be mirroring this context's transforms onto another.
    this.#native.setTransform(1, 0, 0, 1, 0, 0);
    ctx.globalAlpha = 1;
    ctx.globalCompositeOperation = composite;
    nativeFilter?.set?.call(ctx, 'none');
    // The target's clip still applies, exactly as it did for the original draws.
    this.#native.drawImage(layer.canvas, left, top, width, height, left, top, width, height);
    this.#native.restore();
    layer.clearRect(left, top, width, height);
  }
}
