import type { NativeEditInput } from '../app/protocol';
import type { Paragraph } from './paragraph';
import type { UINode, UITree } from './tree';
import { color, colors } from './palette';
import { MeasuredText } from './text';

type NativeRect = NonNullable<NonNullable<NativeEditInput['geometry']>['caret']>;

export class FieldPaint {
  readonly #drafts = new Map<UINode, MeasuredText>();
  state: NativeEditInput | null = null;

  retain(tree: UITree): void {
    if (this.state) {
      const node = tree.byId.get(this.state.field);
      if (!node || node.instance !== this.state.instance || node.disabled) this.state = null;
    }
    for (const [node, draft] of this.#drafts) {
      if (tree.byId.get(node.props.id!) === node) continue;
      draft.dispose();
      this.#drafts.delete(node);
    }
  }

  draw(context: OffscreenCanvasRenderingContext2D, node: UINode): void {
    const state = this.state?.instance === node.instance ? this.state : null;
    const geometry = state?.geometry;
    const x = geometry?.contentX ?? node.inset.left;
    const y = geometry?.contentY ?? node.inset.top;
    const width = geometry?.contentWidth ?? node.screen.width - node.inset.left - node.inset.right;
    const height = geometry?.contentHeight ?? node.screen.height;
    const paragraph = this.#paragraph(node, state, width);
    if (!paragraph) return;
    context.save();
    context.beginPath();
    context.rect(node.screen.x + x, node.screen.y, width, height);
    context.clip();
    context.translate(node.screen.x - (state?.scrollLeft ?? 0), node.screen.y - (state?.scrollTop ?? 0));
    if (state?.active && geometry) this.#range(context, geometry.selection, false);
    const align = node.style.textAlign;
    const factor = align === 'right' ? 1 : align === 'center' ? 0.5 : 0;
    const alignX = node.props.field!.multiline ? 0 : (width - paragraph.longestLine) * factor;
    paragraph.draw(context, x + alignX, y, color(node.style.color ?? colors.text), color(colors.link));
    if (state?.active) this.#editing(context, state);
    context.restore();
  }

  #paragraph(node: UINode, state: NativeEditInput | null, width: number): Paragraph | null {
    const field = node.props.field!;
    const originalWidth = node.screen.width - node.inset.left - node.inset.right;
    const aligned = !field.multiline && node.style.textAlign && node.style.textAlign !== 'left';
    if (!aligned && (!state || (state.value === field.value && (!field.multiline || width === originalWidth)))) {
      this.#drafts.get(node)?.dispose();
      this.#drafts.delete(node);
      return node.text.paragraph;
    }
    const masked = field.sensitive || field.type === 'password';
    const text = state?.value ?? field.value;
    const value = masked ? '•'.repeat(text.length) : text;
    const style = aligned ? { ...node.style, textAlign: 'left' as const } : node.style;
    let draft = this.#drafts.get(node);
    if (!draft) {
      draft = new MeasuredText();
      this.#drafts.set(node, draft);
    }
    draft.update(node.root.fonts, value || field.placeholder, style);
    draft.layout(field.multiline ? width : 100000);
    return draft.paragraph;
  }

  #range(context: OffscreenCanvasRenderingContext2D, rectangles: NativeRect[], underline: boolean): void {
    context.fillStyle = color(underline ? colors.link : colors.selection);
    for (const rect of rectangles) {
      context.fillRect(rect.x, underline ? rect.y + rect.height : rect.y, rect.width, underline ? 1 : rect.height);
    }
  }

  #editing(context: OffscreenCanvasRenderingContext2D, state: NativeEditInput): void {
    const geometry = state.geometry;
    if (!geometry) return;
    this.#range(context, geometry.composition, true);
    const caret = geometry.caret;
    if (!caret || (!state.composition && Math.floor(performance.now() / 500) % 2)) return;
    context.fillStyle = color(colors.text);
    context.fillRect(caret.x, caret.y, caret.width, caret.height);
  }
}
