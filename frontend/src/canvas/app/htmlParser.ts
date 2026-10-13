import type { RichBlock, TextRun } from '../ui/richTypes';

type InlineStyle = Omit<TextRun, 'text'>;

class RichDocument {
  readonly blocks: RichBlock[] = [];
  #runs: TextRun[] = [];
  #pre = false;
  readonly #lists: { ordered: boolean; next: number }[] = [];

  walk(node: Node, style: InlineStyle = {}): void {
    if (node.nodeType === Node.TEXT_NODE) {
      this.#text(node.textContent ?? '', style);
      return;
    }
    if (!(node instanceof Element)) return;
    this.#element(node, style);
  }

  finish(): RichBlock[] {
    this.#flush();
    return this.blocks;
  }

  #element(node: Element, style: InlineStyle): void {
    const tag = node.tagName;
    if (tag === 'OL' || tag === 'UL') {
      this.#flush();
      this.#lists.push({ ordered: tag === 'OL', next: 1 });
      for (const child of node.childNodes) this.walk(child, style);
      this.#flush();
      this.#lists.pop();
      return;
    }
    const block = tag === 'P' || tag === 'PRE' || tag === 'LI';
    if (block) this.#flush();
    const pre = this.#pre;
    if (tag === 'PRE') this.#pre = true;
    if (tag === 'LI') this.#listMarker(style);
    const next = this.#style(node, style);
    if (tag === 'BR') this.#append('\n', next);
    else for (const child of node.childNodes) this.walk(child, next);
    if (block) this.#flush();
    this.#pre = pre;
  }

  #style(node: Element, style: InlineStyle): InlineStyle {
    const next = { ...style };
    const tag = node.tagName;
    if (tag === 'B' || tag === 'STRONG') next.bold = true;
    if (tag === 'I' || tag === 'EM') next.italic = true;
    if (tag === 'CODE' || tag === 'PRE') next.mono = true;
    const href = node.getAttribute('href') ?? '';
    if (tag === 'A' && /^(https?:|mailto:|\/)/i.test(href)) next.href = href;
    return next;
  }

  #listMarker(style: InlineStyle): void {
    const list = this.#lists.at(-1);
    const marker = list?.ordered ? `${list.next++}. ` : '• ';
    this.#append('  '.repeat(Math.max(0, this.#lists.length - 1)) + marker, style);
  }

  #text(value: string, style: InlineStyle): void {
    if (!this.#pre) {
      value = value.replace(/\s+/g, ' ');
      const previous = this.#runs.at(-1)?.text;
      if (!previous || /[ \n]$/.test(previous)) value = value.replace(/^ /, '');
    }
    this.#append(value, style);
  }

  #append(text: string, style: InlineStyle): void {
    if (!text) return;
    const last = this.#runs.at(-1);
    if (
      last &&
      last.bold === style.bold &&
      last.italic === style.italic &&
      last.mono === style.mono &&
      last.href === style.href
    )
      last.text += text;
    else this.#runs.push({ text, ...style });
  }

  #flush(): void {
    if (!this.#runs.length) return;
    if (!this.#pre) {
      const last = this.#runs.at(-1)!;
      last.text = last.text.replace(/[ \t]+$/, '');
    }
    this.blocks.push({ runs: this.#runs, pre: this.#pre });
    this.#runs = [];
  }
}

/** Inert parsing preserves entities; only text and allowed inline attributes cross the worker boundary. */
export function parseRichHTML(value: string): RichBlock[] {
  const document = new DOMParser().parseFromString(value, 'text/html');
  const builder = new RichDocument();
  for (const child of document.body.childNodes) builder.walk(child);
  return builder.finish();
}
