// Group opacity for Canvas2D: a subtree draws into a scratch canvas sized to its
// visible extent, then composites once at the group's opacity.
import { deviceRect, type DeviceRect } from '../gl';
import type { UINode } from './tree';
import type { Rect } from './types';

type Context = OffscreenCanvasRenderingContext2D;
interface Scratch {
  canvas: OffscreenCanvas;
  context: Context;
}
export interface OpacityLayer extends Scratch {
  parent: Context;
  bounds: DeviceRect;
}

/** Visible area a subtree can paint, including focus rings, limited to its clip. */
export function subtreeExtent(node: UINode): Rect {
  let left = node.screen.x;
  let top = node.screen.y;
  let right = left + node.screen.width;
  let bottom = top + node.screen.height;
  const visit = (child: UINode) => {
    if (!child.shown || child.kind === 'raw' || child.props.fixed) return;
    left = Math.min(left, child.screen.x);
    top = Math.min(top, child.screen.y);
    right = Math.max(right, child.screen.x + child.screen.width);
    bottom = Math.max(bottom, child.screen.y + child.screen.height);
    for (const grandchild of child.children) visit(grandchild);
  };
  for (const child of node.children) visit(child);
  const clip = node.clip;
  left = Math.max(clip.x, left - 4);
  top = Math.max(clip.y, top - 4);
  right = Math.min(clip.x + clip.width, right + 4);
  bottom = Math.min(clip.y + clip.height, bottom + 4);
  return { x: left, y: top, width: Math.max(0, right - left), height: Math.max(0, bottom - top) };
}

export class OpacityLayers {
  readonly #free: Scratch[] = [];

  /** Starts a layer over `extent`; null when nothing of it can reach the parent canvas. */
  begin(parent: Context, extent: Rect, dpr: number): OpacityLayer | null {
    // Nested scratch contexts retain world coordinates but have a cropped origin.
    // Bounds and the final copy are in the immediate parent's device coordinates.
    const transform = parent.getTransform();
    const local = { ...extent, x: extent.x + transform.e / dpr, y: extent.y + transform.f / dpr };
    const bounds = deviceRect(local, 0, dpr, parent.canvas);
    if (!bounds.width || !bounds.height) return null;
    const scratch = this.#free.pop() ?? this.#create();
    const { canvas, context } = scratch;
    if (canvas.width < bounds.width || canvas.height < bounds.height) {
      canvas.width = Math.max(canvas.width, bounds.width);
      canvas.height = Math.max(canvas.height, bounds.height);
    }
    context.setTransform(1, 0, 0, 1, 0, 0);
    context.clearRect(0, 0, bounds.width, bounds.height);
    context.setTransform(dpr, 0, 0, dpr, transform.e - bounds.x, transform.f - bounds.y);
    return { canvas, context, parent, bounds };
  }

  /** Composites the layer into its parent and returns the parent context. */
  end(layer: OpacityLayer, opacity: number): Context {
    const { parent, bounds } = layer;
    parent.save();
    parent.setTransform(1, 0, 0, 1, 0, 0);
    parent.globalAlpha = opacity;
    parent.drawImage(layer.canvas, 0, 0, bounds.width, bounds.height, bounds.x, bounds.y, bounds.width, bounds.height);
    parent.restore();
    this.#free.push({ canvas: layer.canvas, context: layer.context });
    return parent;
  }

  #create(): Scratch {
    const canvas = new OffscreenCanvas(1, 1);
    const context = canvas.getContext('2d');
    if (!context) throw new Error('Canvas2D is unavailable in the render worker');
    return { canvas, context };
  }
}
