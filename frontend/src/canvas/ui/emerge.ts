// Overlay glass emerges out of the control that opened it (`morphFrom`), the way Liquid
// Glass menus grow from their button: the shape follows a slightly underdamped spring
// from the anchor's capsule to its final rect, and its contents fade in once it has
// mostly formed. Renderer-owned motion: React only mounts the final surface.
import { byId } from './interaction-targets';
import type { UINode, UITree } from './tree';
import type { Rect } from './types';

const OMEGA = 17; // rad/s
const ZETA = 0.78; // ~2% overshoot
const SETTLE = 0.42; // seconds
const DAMPED = OMEGA * Math.sqrt(1 - ZETA * ZETA);

interface Entry {
  from: Rect;
  radius: number;
  time: number;
}

export interface MorphFrame {
  rect: Rect;
  radius: number;
  alpha: number;
}

function spring(t: number): number {
  const decay = Math.exp(-ZETA * OMEGA * t);
  return 1 - decay * (Math.cos(DAMPED * t) + ((ZETA * OMEGA) / DAMPED) * Math.sin(DAMPED * t));
}

function mix(from: number, to: number, amount: number): number {
  return from + (to - from) * amount;
}

export class Emergence {
  readonly #entries = new Map<UINode, Entry>();
  readonly #frame: MorphFrame = { rect: { x: 0, y: 0, width: 0, height: 0 }, radius: 0, alpha: 1 };

  constructor(readonly tree: UITree) {}

  /** True while a surface drawn this frame still needs another frame. */
  get active(): boolean {
    for (const [node, entry] of this.#entries) if (node.shown && entry.time < SETTLE) return true;
    return false;
  }

  step(dt: number, animate: boolean): boolean {
    let moving = false;
    for (const [node, entry] of this.#entries) {
      if (!node.shown) {
        this.#entries.delete(node);
        continue;
      }
      entry.time = animate ? Math.min(SETTLE, entry.time + dt) : SETTLE;
      moving ||= entry.time < SETTLE;
    }
    return moving;
  }

  /** This frame's shape for a surface (registering it on first sight), or null once settled. */
  frame(node: UINode, radius: number, animate: boolean): MorphFrame | null {
    let entry = this.#entries.get(node);
    if (!entry) {
      const anchor = node.props.morphFrom ? byId(this.tree, node.props.morphFrom) : null;
      const from = anchor ? { ...anchor.screen } : node.screen;
      const time = animate && anchor && anchor.screen.width > 0 ? 0 : SETTLE;
      entry = { from, radius: Math.min(anchor?.style.borderRadius ?? radius, from.height / 2), time };
      this.#entries.set(node, entry);
    }
    if (entry.time >= SETTLE) return null;
    const p = spring(entry.time);
    const to = node.screen;
    const rect = this.#frame.rect;
    rect.x = mix(entry.from.x, to.x, p);
    rect.y = mix(entry.from.y, to.y, p);
    rect.width = Math.max(1, mix(entry.from.width, to.width, p));
    rect.height = Math.max(1, mix(entry.from.height, to.height, p));
    this.#frame.radius = Math.min(mix(entry.radius, radius, p), rect.width / 2, rect.height / 2);
    this.#frame.alpha = Math.min(1, Math.max(0, (entry.time - 0.08) / 0.2));
    return this.#frame;
  }
}
