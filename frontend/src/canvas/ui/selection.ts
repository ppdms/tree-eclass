import { visibleRect } from './geometry';
import type { CanvasPointer, InteractionEvents } from './interaction';
import { alive } from './interaction-targets';
import type { UIPainter } from './paint';
import type { Paragraph } from './paragraph';
import { hitText, selectableText } from './query';
import type { UINode, UITree } from './tree';
import type { Rect } from './types';

export type SelectionGranularity = 'char' | 'word' | 'line';
interface Position {
  node: UINode;
  offset: number;
}
type Range = [Position, Position];
interface TextEntry {
  node: UINode;
  text: string;
  paragraph: Paragraph;
}
interface CachedRects {
  paragraph: Paragraph;
  start: number;
  end: number;
  width: number;
  rects: Rect[];
}

interface ClickSequence {
  count: number;
  time: number;
  x: number;
  y: number;
  node: UINode | null;
}

/** Selection keeps UTF-16 offsets into committed paragraphs; input never builds or lays out text. */
export class CanvasSelection {
  #entries: TextEntry[] = [];
  #revision = -1;
  #width = -1;
  #scope: UINode | undefined;
  #anchor: Range | null = null;
  #range: Range | null = null;
  #granularity: SelectionGranularity = 'char';
  #rects = new Map<UINode, CachedRects>();
  #mirrored = '';
  #clicks: ClickSequence = { count: 0, time: 0, x: 0, y: 0, node: null };

  constructor(
    readonly tree: UITree,
    readonly painter: UIPainter,
    readonly events: InteractionEvents,
  ) {}

  get hasRange(): boolean {
    return !!this.#range && this.#compare(...this.#range) !== 0;
  }

  granularity(input: CanvasPointer, node: UINode): SelectionGranularity {
    const last = this.#clicks;
    const repeated =
      last.node === node && input.time - last.time < 450 && Math.hypot(input.x - last.x, input.y - last.y) < 5;
    const count = repeated ? (last.count % 3) + 1 : 1;
    this.#clicks = { count, node, time: input.time, x: input.x, y: input.y };
    return input.shift ? 'char' : count === 2 ? 'word' : count === 3 ? 'line' : 'char';
  }

  begin(node: UINode, x: number, y: number, granularity: SelectionGranularity, shift: boolean): boolean {
    this.#refresh();
    const at = this.#position(node, x, y);
    if (!at) return false;
    if (shift && this.#anchor) this.extend(x, y);
    else {
      this.#granularity = granularity;
      this.#anchor = this.#granule(at);
      this.#range = this.#anchor;
      this.synchronize();
    }
    return true;
  }

  extend(x: number, y: number): void {
    this.#refresh();
    const anchor = this.#anchor;
    const at = this.#locate(x, y);
    if (!anchor || !at) return;
    const [start, end] = this.#granule(at);
    this.#range = [
      this.#compare(start, anchor[0]) < 0 ? start : anchor[0],
      this.#compare(end, anchor[1]) > 0 ? end : anchor[1],
    ];
    this.synchronize();
  }

  selectAll(): void {
    this.#refresh();
    const first = this.#entries[0];
    const last = this.#entries[this.#entries.length - 1];
    if (!first || !last) {
      this.clear();
      return;
    }
    this.#granularity = 'char';
    this.#anchor = [
      { node: first.node, offset: 0 },
      { node: last.node, offset: last.text.length },
    ];
    this.#range = this.#anchor;
    this.synchronize();
    this.events.invalidate();
  }

  clear(mirror = true): void {
    this.#anchor = null;
    this.#range = null;
    this.#rects.clear();
    this.painter.selectionRects.clear();
    if (mirror) this.#mirror('');
    else this.#mirrored = '';
    this.events.invalidate();
  }

  synchronize(): void {
    this.#refresh();
    const range = this.#range;
    if (!range) return;
    const start = this.#entries.findIndex((entry) => entry.node === range[0].node);
    const end = this.#entries.findIndex((entry) => entry.node === range[1].node);
    if (start < 0 || end < start) {
      this.clear();
      return;
    }
    const selected = new Set<UINode>();
    const parts: string[] = [];
    for (let index = start; index <= end; index++) {
      const entry = this.#entries[index]!;
      const from = index === start ? Math.min(entry.text.length, range[0].offset) : 0;
      const to = index === end ? Math.min(entry.text.length, range[1].offset) : entry.text.length;
      parts.push(entry.text.slice(from, to));
      if (from < to) {
        selected.add(entry.node);
        this.#highlight(entry, from, to);
      }
    }
    for (const node of this.#rects.keys()) {
      if (!selected.has(node)) {
        this.#rects.delete(node);
        this.painter.selectionRects.delete(node);
      }
    }
    this.#mirror(this.hasRange ? parts.join('\n') : '');
  }

  #refresh(): void {
    const scope = this.tree.focusRoots[this.tree.focusRoots.length - 1];
    const width = this.tree.root.rect.width;
    if (this.#revision === this.tree.revision && scope === this.#scope && width === this.#width) return;
    this.#revision = this.tree.revision;
    this.#width = width;
    this.#scope = scope;
    this.#entries = [];
    for (const node of selectableText(this.tree)) {
      const paragraph = node.text.paragraph;
      const text = node.content;
      if (paragraph && text && alive(this.tree, node)) this.#entries.push({ node, text, paragraph });
    }
  }

  #position(node: UINode, x: number, y: number): Position | null {
    const entry = this.#entries.find((entry) => entry.node === node);
    if (!entry || !alive(this.tree, node)) return null;
    const pos = entry.paragraph.offsetAt(x - node.screen.x - node.inset.left, y - node.screen.y - node.inset.top);
    return { node, offset: Math.min(entry.text.length, Math.max(0, pos)) };
  }

  #locate(x: number, y: number): Position | null {
    const target = hitText(this.tree, x, y);
    if (target) {
      const at = this.#position(target, x, y);
      if (at) return at;
    }
    let nearest: UINode | null = null;
    let distance = Infinity;
    for (const { node } of this.#entries) {
      const rect = visibleRect(node);
      if (!rect || !alive(this.tree, node)) continue;
      const dx = Math.max(0, rect.x - x, x - rect.x - rect.width);
      const dy = Math.max(0, rect.y - y, y - rect.y - rect.height);
      const next = dx + dy * 4;
      if (next < distance) {
        nearest = node;
        distance = next;
      }
    }
    return nearest ? this.#position(nearest, x, y) : null;
  }

  #granule(at: Position): Range {
    const entry = this.#entries.find((entry) => entry.node === at.node)!;
    if (this.#granularity === 'char') return [at, at];
    const paragraph = entry.paragraph;
    const offset = Math.min(at.offset, Math.max(0, entry.text.length - 1));
    if (this.#granularity === 'word') {
      const { start, end } = paragraph.wordBoundary(offset);
      return [
        { node: at.node, offset: start },
        { node: at.node, offset: end },
      ];
    }
    const line = paragraph.lineRange(offset);
    return line
      ? [
          { node: at.node, offset: line.start },
          { node: at.node, offset: line.end },
        ]
      : [at, at];
  }

  #compare(a: Position, b: Position): number {
    if (a.node === b.node) return a.offset - b.offset;
    return (
      this.#entries.findIndex((entry) => entry.node === a.node) -
      this.#entries.findIndex((entry) => entry.node === b.node)
    );
  }

  #highlight(entry: TextEntry, start: number, end: number): void {
    const previous = this.#rects.get(entry.node);
    const width = entry.node.rect.width;
    if (
      previous?.paragraph === entry.paragraph &&
      previous.start === start &&
      previous.end === end &&
      previous.width === width
    )
      return;
    const rects = entry.paragraph.rectsForRange(start, end);
    this.#rects.set(entry.node, { paragraph: entry.paragraph, start, end, width, rects });
    this.painter.selectionRects.set(entry.node, rects);
  }

  #mirror(text: string): void {
    if (text === this.#mirrored) return;
    this.#mirrored = text;
    this.events.selection(text);
  }
}
