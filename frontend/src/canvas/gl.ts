// Minimal WebGL2 plumbing for the glass compositor: programs with resolved uniforms
// and resizable RGBA8 render targets. Textures are premultiplied, top row first.
import { quadVertex } from './glassShaders';

export interface Program<Name extends string> {
  program: WebGLProgram;
  at: Record<Name, WebGLUniformLocation>;
}
export interface Target {
  texture: WebGLTexture;
  framebuffer: WebGLFramebuffer;
  width: number;
  height: number;
}
export interface DeviceRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

/** Allocate glass output in CSS-pixel blocks, not on every animation-frame extent change. */
export function surfaceHeight(limit: number, dpr: number, bottom = 0): number {
  const block = Math.ceil(64 * dpr);
  return Math.min(limit, Math.max(2 * block, Math.ceil(bottom / block) * block));
}

function shader(gl: WebGL2RenderingContext, type: number, source: string): WebGLShader {
  const compiled = gl.createShader(type);
  if (!compiled) throw new Error('WebGL could not allocate a shader');
  gl.shaderSource(compiled, source);
  gl.compileShader(compiled);
  return compiled;
}

/** Compiles without waiting; `link` checks status on first use so drivers can compile in parallel. */
export function compile<Name extends string>(
  gl: WebGL2RenderingContext,
  fragment: string,
  names: readonly Name[],
): () => Program<Name> {
  const program = gl.createProgram();
  const stages = [shader(gl, gl.VERTEX_SHADER, quadVertex), shader(gl, gl.FRAGMENT_SHADER, fragment)];
  for (const stage of stages) gl.attachShader(program, stage);
  gl.linkProgram(program);
  let linked: Program<Name> | null = null;
  return () => {
    if (linked) return linked;
    if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
      const log = stages.map((stage) => gl.getShaderInfoLog(stage)).join('\n');
      throw new Error(`Glass shader failed to link: ${gl.getProgramInfoLog(program)}\n${log}`);
    }
    // SAFETY: the loop below assigns every requested name or throws before the record escapes.
    const at = {} as Record<Name, WebGLUniformLocation>;
    for (const name of names) {
      const location = gl.getUniformLocation(program, name);
      if (!location) throw new Error(`Glass shader has no uniform ${name}`);
      at[name] = location;
    }
    for (const stage of stages) gl.deleteShader(stage);
    linked = { program, at };
    return linked;
  };
}

export function texture(gl: WebGL2RenderingContext): WebGLTexture {
  const created = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, created);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  return created;
}

export function target(gl: WebGL2RenderingContext): Target {
  return { texture: texture(gl), framebuffer: gl.createFramebuffer(), width: 0, height: 0 };
}

/** Reallocates `into` when its size changes and binds its framebuffer and viewport. */
export function bindTarget(gl: WebGL2RenderingContext, into: Target, width: number, height: number): void {
  if (into.width !== width || into.height !== height) {
    gl.bindTexture(gl.TEXTURE_2D, into.texture);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null);
    gl.bindFramebuffer(gl.FRAMEBUFFER, into.framebuffer);
    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, into.texture, 0);
    into.width = width;
    into.height = height;
  }
  gl.bindFramebuffer(gl.FRAMEBUFFER, into.framebuffer);
  gl.viewport(0, 0, width, height);
}

/** Copies the default framebuffer, padding backdrop samples below the drawn navigation with transparency. */
export function copyScreen(gl: WebGL2RenderingContext, into: Target, rect: DeviceRect, screenHeight: number): void {
  bindTarget(gl, into, rect.width, rect.height);
  const height = Math.max(0, Math.min(rect.height, screenHeight - rect.y));
  if (height < rect.height) {
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
  }
  gl.bindFramebuffer(gl.FRAMEBUFFER, null);
  gl.bindTexture(gl.TEXTURE_2D, into.texture);
  if (height) {
    const y = screenHeight - rect.y - height;
    gl.copyTexSubImage2D(gl.TEXTURE_2D, 0, 0, rect.height - height, rect.x, y, rect.width, height);
  }
}

export function intersects(a: DeviceRect, b: DeviceRect): boolean {
  return a.x < b.x + b.width && b.x < a.x + a.width && a.y < b.y + b.height && b.y < a.y + a.height;
}

export function contains(outer: DeviceRect, inner: DeviceRect): boolean {
  return (
    inner.x >= outer.x &&
    inner.y >= outer.y &&
    inner.x + inner.width <= outer.x + outer.width &&
    inner.y + inner.height <= outer.y + outer.height
  );
}

/** A CSS-pixel rectangle grown by `margin`, snapped outward to device pixels and clipped to the target. */
export function deviceRect(
  rect: DeviceRect,
  margin: number,
  dpr: number,
  bounds: { width: number; height: number },
): DeviceRect {
  const left = Math.max(0, Math.floor((rect.x - margin) * dpr));
  const top = Math.max(0, Math.floor((rect.y - margin) * dpr));
  const right = Math.min(bounds.width, Math.ceil((rect.x + rect.width + margin) * dpr));
  const bottom = Math.min(bounds.height, Math.ceil((rect.y + rect.height + margin) * dpr));
  return { x: left, y: top, width: Math.max(0, right - left), height: Math.max(0, bottom - top) };
}
