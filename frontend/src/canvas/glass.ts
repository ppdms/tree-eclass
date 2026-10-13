// WebGL2 compositor for the navigation layer, drawn offscreen and copied onto the
// Canvas2D output. Crop into bounded Canvas2D staging surfaces before uploading:
// DOM-source subrect uploads can still make the driver process the full source.
import type { ChromeOp, GlassGlow, GlassStyle } from './chrome';
import {
  bindTarget,
  compile,
  contains,
  copyScreen,
  deviceRect,
  intersects,
  surfaceHeight,
  target,
  texture,
  type DeviceRect,
  type Program,
  type Target,
} from './gl';
import {
  blurShader,
  composeShader,
  copyShader,
  downShader,
  fillShader,
  glassShader,
  scrollEdgeShader,
} from './glassShaders';
import { parseColor, type Palette } from './theme';
import type { Rect } from './ui/types';

// Backdrop margin around a surface: refraction depth plus three blur sigmas.
const BACKDROP_MARGIN = 60;
// The material's drop shadow reaches 28px below and 22px around each surface.
const DRAW_REACH = 40;
const FROST_SIGMA = 11; // CSS pixels; Liquid Glass faces show no readable backdrop detail
const QUAD = ['quad', 'target', 'flip'] as const;
const COPY = [...QUAD, 'source', 'region', 'alpha'] as const;
const FILL = [...QUAD, 'color'] as const;
const COMPOSE = [...QUAD, 'content', 'under', 'size'] as const;
const DOWN = [...QUAD, 'source', 'texel'] as const;
const BLUR = [...QUAD, 'source', 'texel', 'direction', 'sigma'] as const;
const SAMPLE = ['frosted', 'dpr', 'region', 'frostSize'] as const;
const GLASS = [
  ...QUAD,
  ...SAMPLE,
  'sharp',
  'rect',
  'radius',
  'bevel',
  'depth',
  'frost',
  'tint',
  'adaptive',
  'lift',
  'rim',
  'shade',
  'shadow',
  'dark',
  'glow',
] as const;
const EDGE = [...QUAD, ...SAMPLE, 'top', 'bottom', 'page', 'strength'] as const;

type Uniforms<Names extends readonly string[]> = Record<Names[number], WebGLUniformLocation>;
type Lazy<Names extends readonly string[]> = () => Program<Names[number]>;
interface Programs {
  copy: Lazy<typeof COPY>;
  fill: Lazy<typeof FILL>;
  compose: Lazy<typeof COMPOSE>;
  down: Lazy<typeof DOWN>;
  blur: Lazy<typeof BLUR>;
  glass: Lazy<typeof GLASS>;
  edge: Lazy<typeof EDGE>;
}

interface Region {
  rect: DeviceRect;
  layered: boolean;
  content: WebGLTexture;
  under: Target;
  composite: Target;
  frost: [Target, Target];
  sharp: WebGLTexture;
}

function programs(gl: WebGL2RenderingContext): Programs {
  return {
    copy: compile(gl, copyShader, COPY),
    fill: compile(gl, fillShader, FILL),
    compose: compile(gl, composeShader, COMPOSE),
    down: compile(gl, downShader, DOWN),
    blur: compile(gl, blurShader, BLUR),
    glass: compile(gl, glassShader, GLASS),
    edge: compile(gl, scrollEdgeShader, EDGE),
  };
}

export class GlassCompositor {
  // Driver introspection synchronizes the GPU process; it is not needed to draw.
  readonly label = 'WebGL2';
  readonly #gl: WebGL2RenderingContext;
  readonly #programs: Programs;
  readonly #staging = new Map<string, OffscreenCanvasRenderingContext2D>();
  readonly #layers: WebGLTexture[] = [];
  readonly #regions: Region[] = [];
  #layerCount = 0;
  #regionCount = 0;
  #drawn = false;
  #dpr = 1;
  #width = 1;
  #height = 1;
  constructor(readonly canvas: OffscreenCanvas) {
    const options = { alpha: true, premultipliedAlpha: true, antialias: false, depth: false, stencil: false };
    const gl = canvas.getContext('webgl2', options);
    if (!gl) throw new Error('WebGL2 is unavailable in the render worker');
    this.#gl = gl;
    gl.getExtension('KHR_parallel_shader_compile');
    this.#programs = programs(gl);
    gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, true);
    gl.pixelStorei(gl.UNPACK_COLORSPACE_CONVERSION_WEBGL, gl.NONE);
    gl.bindVertexArray(gl.createVertexArray());
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    gl.flush(); // Dispatch shader compilation now, not at the first blocking link-status query.
  }

  resize(width: number, height: number, dpr: number): void {
    this.#width = width;
    this.#height = height;
    this.#dpr = dpr;
    this.#drawn = true;
  }

  /** Avoid a full-viewport GL→2D readback when navigation only occupies the top strip. */
  #size(ops: readonly ChromeOp[]): void {
    let bottom = 1;
    for (const op of ops) {
      if (op.kind === 'layer') bottom = Math.max(bottom, op.bounds.y + op.bounds.height);
      else if (op.kind === 'edge') bottom = Math.max(bottom, op.bottom * this.#dpr);
      else {
        const margin = op.kind === 'glass' ? DRAW_REACH : 0;
        bottom = Math.max(bottom, (op.rect.y + op.rect.height + margin) * this.#dpr);
      }
    }
    const height = surfaceHeight(this.#height, this.#dpr, bottom);
    if (this.canvas.width !== this.#width) this.canvas.width = this.#width;
    if (this.canvas.height !== height) this.canvas.height = height;
  }

  beginFrame(): void {
    this.#layerCount = 0;
    this.#regionCount = 0;
  }

  /** Uploads a chrome segment; its slot stays valid until the next frame begins. */
  capture(source: OffscreenCanvas, bounds: DeviceRect): number {
    const slot = this.#layerCount++;
    const layer = (this.#layers[slot] ??= texture(this.#gl));
    this.#upload(source, bounds, layer);
    return slot;
  }

  run(ops: readonly ChromeOp[], base: OffscreenCanvas, palette: Palette | null): void {
    if (!ops.length && !this.#drawn) return;
    this.#size(ops);
    const gl = this.#gl;
    this.#screen();
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    this.#drawn = ops.length > 0;
    const covered: DeviceRect[] = [];
    for (const op of ops) {
      if (op.kind === 'layer') {
        this.#copy(this.#layers[op.slot]!, op.bounds, op.alpha);
        covered.push(op.bounds);
      } else if (op.kind === 'fill') {
        covered.push(this.#fill(op.rect, op.color));
      } else if (palette && op.kind === 'edge') {
        const area = { x: 0, y: 0, width: this.canvas.width / this.#dpr, height: op.bottom };
        this.#edge(op.top, op.bottom, this.#backdrop(base, area, covered, false), palette);
      } else if (palette && op.kind === 'glass') {
        covered.push(
          this.#glass(op.rect, op.radius, op.style, op.glow, this.#backdrop(base, op.rect, covered), palette),
        );
      }
    }
  }

  #screen(): void {
    const gl = this.#gl;
    gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    gl.viewport(0, 0, this.canvas.width, this.canvas.height);
    gl.enable(gl.BLEND);
  }

  #upload(source: OffscreenCanvas, rect: DeviceRect, into: WebGLTexture): void {
    const key = `${rect.width}x${rect.height}`;
    let staging = this.#staging.get(key);
    if (!staging) {
      if (this.#staging.size >= 8) this.#staging.clear();
      const created = new OffscreenCanvas(rect.width, rect.height).getContext('2d');
      if (!created) throw new Error('Canvas2D is unavailable in the render worker');
      created.globalCompositeOperation = 'copy';
      staging = created;
      this.#staging.set(key, staging);
    }
    staging.drawImage(source, rect.x, rect.y, rect.width, rect.height, 0, 0, rect.width, rect.height);
    const gl = this.#gl;
    gl.bindTexture(gl.TEXTURE_2D, into);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, staging.canvas);
  }

  /** Sharp and frosted backdrop under `area`, including overlay layers already drawn above the content. */
  #backdrop(base: OffscreenCanvas, area: Rect, covered: readonly DeviceRect[], allowLayers = true): Region {
    const rect = deviceRect(area, BACKDROP_MARGIN, this.#dpr, base);
    const layered = allowLayers && covered.some((other) => intersects(other, rect));
    if (!layered) {
      for (let index = 0; index < this.#regionCount; index++) {
        const region = this.#regions[index]!;
        if (!region.layered && contains(region.rect, rect)) return region;
      }
    }
    const region = this.#region(rect, layered);
    this.#upload(base, rect, region.content);
    if (layered) this.#compose(region);
    this.#frost(region);
    this.#screen();
    return region;
  }

  #region(rect: DeviceRect, layered: boolean): Region {
    const gl = this.#gl;
    const slot = this.#regionCount++;
    let region = this.#regions[slot];
    if (!region) {
      const content = texture(gl);
      region = {
        rect,
        layered,
        content,
        sharp: content,
        under: target(gl),
        composite: target(gl),
        frost: [target(gl), target(gl)],
      };
      this.#regions[slot] = region;
    }
    region.rect = rect;
    region.layered = layered;
    region.sharp = region.content;
    return region;
  }

  #compose(region: Region): void {
    const gl = this.#gl;
    const { width, height } = region.rect;
    copyScreen(gl, region.under, region.rect, this.canvas.height);
    bindTarget(gl, region.composite, width, height);
    gl.disable(gl.BLEND);
    const program = this.#programs.compose();
    gl.useProgram(program.program);
    this.#quad(program.at, { x: 0, y: 0, width, height }, width, height, -1);
    this.#bind(program.at.content, 0, region.content);
    this.#bind(program.at.under, 1, region.under.texture);
    gl.uniform2f(program.at.size, width, height);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    region.sharp = region.composite.texture;
  }

  #frost(region: Region): void {
    const gl = this.#gl;
    const { width, height } = region.rect;
    const small = { x: 0, y: 0, width: Math.ceil(width / 4), height: Math.ceil(height / 4) };
    const [a, b] = region.frost;
    gl.disable(gl.BLEND);
    const down = this.#programs.down();
    gl.useProgram(down.program);
    bindTarget(gl, a, small.width, small.height);
    this.#quad(down.at, small, small.width, small.height, -1);
    this.#bind(down.at.source, 0, region.sharp);
    gl.uniform2f(down.at.texel, 1 / width, 1 / height);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    const blur = this.#programs.blur();
    gl.useProgram(blur.program);
    gl.uniform2f(blur.at.texel, 1 / small.width, 1 / small.height);
    gl.uniform1f(blur.at.sigma, (FROST_SIGMA * this.#dpr) / 4);
    for (const [from, into, direction] of [
      [a, b, [1, 0]],
      [b, a, [0, 1]],
    ] as const) {
      bindTarget(gl, into, small.width, small.height);
      this.#quad(blur.at, small, small.width, small.height, -1);
      this.#bind(blur.at.source, 0, from.texture);
      gl.uniform2f(blur.at.direction, direction[0], direction[1]);
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    }
  }

  #glass(
    rect: Rect,
    radius: number,
    style: GlassStyle,
    glow: GlassGlow | null,
    region: Region,
    palette: Palette,
  ): DeviceRect {
    const gl = this.#gl;
    const program = this.#programs.glass();
    const at = program.at;
    const quad = deviceRect(rect, DRAW_REACH, this.#dpr, this.canvas);
    const half = Math.min(rect.width, rect.height) / 2;
    const material = palette.glass;
    gl.useProgram(program.program);
    this.#quad(at, quad, this.canvas.width, this.canvas.height, 1);
    this.#sample(at, region);
    this.#bind(at.sharp, 0, region.sharp);
    gl.uniform4f(at.rect, rect.x, rect.y, rect.width, rect.height);
    gl.uniform1f(at.radius, Math.min(radius, half));
    gl.uniform1f(at.bevel, Math.max(1, Math.min(style.bevel, half)));
    gl.uniform1f(at.depth, style.depth);
    gl.uniform1f(at.frost, style.frost);
    const [r, g, b] = material.tint;
    gl.uniform4f(at.tint, r, g, b, Math.min(0.96, material.tintAlpha + style.density));
    gl.uniform1f(at.adaptive, material.adaptive);
    gl.uniform1f(at.lift, material.lift);
    gl.uniform1f(at.rim, material.rim);
    gl.uniform1f(at.shade, material.shade);
    gl.uniform1f(at.shadow, material.shadow);
    gl.uniform1f(at.dark, palette.dark ? 1 : 0);
    gl.uniform3f(at.glow, glow?.x ?? 0, glow?.y ?? 0, glow?.energy ?? 0);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    return quad;
  }

  #edge(top: number, bottom: number, region: Region, palette: Palette): void {
    const gl = this.#gl;
    const program = this.#programs.edge();
    const at = program.at;
    gl.useProgram(program.program);
    const quad = { x: 0, y: 0, width: this.canvas.width, height: Math.ceil(bottom * this.#dpr) };
    this.#quad(at, quad, this.canvas.width, this.canvas.height, 1);
    this.#sample(at, region);
    gl.uniform1f(at.top, top);
    gl.uniform1f(at.bottom, bottom);
    const [r, g, b] = palette.page;
    gl.uniform4f(at.page, r, g, b, palette.glass.edgeFade);
    gl.uniform1f(at.strength, palette.glass.edgeStrength);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
  }

  #sample(at: Uniforms<typeof SAMPLE>, region: Region): void {
    const gl = this.#gl;
    const frost = region.frost[0];
    this.#bind(at.frosted, 1, frost.texture);
    gl.uniform1f(at.dpr, this.#dpr);
    const rect = region.rect;
    gl.uniform4f(at.region, rect.x, rect.y, rect.width, rect.height);
    gl.uniform2f(at.frostSize, frost.width, frost.height);
  }

  #fill(rect: Rect, value: string): DeviceRect {
    const gl = this.#gl;
    const program = this.#programs.fill();
    const quad = deviceRect(rect, 0, this.#dpr, this.canvas);
    const [r, g, b, a] = parseColor(value);
    gl.useProgram(program.program);
    this.#quad(program.at, quad, this.canvas.width, this.canvas.height, 1);
    gl.uniform4f(program.at.color, r * a, g * a, b * a, a);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    return quad;
  }

  #copy(source: WebGLTexture, bounds: DeviceRect, alpha: number): void {
    const gl = this.#gl;
    const program = this.#programs.copy();
    gl.useProgram(program.program);
    this.#quad(program.at, bounds, this.canvas.width, this.canvas.height, 1);
    this.#bind(program.at.source, 0, source);
    gl.uniform4f(program.at.region, bounds.x, bounds.y, bounds.width, bounds.height);
    gl.uniform1f(program.at.alpha, alpha);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
  }

  #quad(at: Uniforms<typeof QUAD>, quad: DeviceRect, width: number, height: number, flip: number): void {
    const gl = this.#gl;
    gl.uniform4f(at.quad, quad.x, quad.y, quad.width, quad.height);
    gl.uniform2f(at.target, width, height);
    gl.uniform1f(at.flip, flip);
  }

  #bind(location: WebGLUniformLocation, unit: number, source: WebGLTexture): void {
    const gl = this.#gl;
    gl.activeTexture(gl.TEXTURE0 + unit);
    gl.bindTexture(gl.TEXTURE_2D, source);
    gl.uniform1i(location, unit);
  }
}
