import type { Definition, PhrasingContent } from 'mdast';
import type { TextRun } from './richTypes';
import { markdownFootnoteKey, type MarkdownInline } from './markdownTypes';

export interface MarkdownInlineContext {
  prefix: string;
  definitions: Map<string, Definition>;
  footnoteNumbers: Map<string, number>;
}

interface InlineStyle {
  bold?: boolean;
  italic?: boolean;
  strike?: boolean;
  mono?: boolean;
  href?: string;
  title?: string;
}

// Match ReactMarkdown's default URL transform, including relative URLs.
export function markdownSafeUrl(value: string): string {
  const colon = value.indexOf(':');
  if (colon === -1) return value;
  for (const separator of ['/', '?', '#']) {
    const index = value.indexOf(separator);
    if (index !== -1 && index < colon) return value;
  }
  return /^(https?|ircs?|mailto|xmpp)$/i.test(value.slice(0, colon)) ? value : '';
}

export function markdownNodeKey(node: { type: string; position?: { start: { offset?: number } } }): string {
  return `${node.type}-${node.position?.start.offset ?? 0}`;
}

function appendRun(parts: MarkdownInline[], key: string, text: string, style: InlineStyle): void {
  if (!text) return;
  let part = parts.at(-1);
  if (!part || part.type !== 'runs') {
    part = { type: 'runs', key, runs: [] };
    parts.push(part);
  }
  const previous = part.runs.at(-1);
  if (previous && equalStyle(previous, style)) previous.text += text;
  else part.runs.push({ ...style, text });
}

function equalStyle(run: TextRun & InlineStyle, style: InlineStyle): boolean {
  return (
    run.bold === style.bold &&
    run.italic === style.italic &&
    run.strike === style.strike &&
    run.mono === style.mono &&
    run.href === style.href &&
    run.title === style.title
  );
}

function appendImage(
  node: Extract<PhrasingContent, { type: 'image' | 'imageReference' }>,
  parts: MarkdownInline[],
  context: MarkdownInlineContext,
  style: InlineStyle,
): void {
  const resource = node.type === 'image' ? node : context.definitions.get(node.identifier);
  parts.push({
    type: 'image',
    key: markdownNodeKey(node),
    src: markdownSafeUrl(resource?.url ?? ''),
    alt: node.alt ?? '',
    title: resource?.title ?? style.title,
    href: style.href,
  });
}

function appendFootnote(
  identifier: string,
  key: string,
  parts: MarkdownInline[],
  context: MarkdownInlineContext,
  style: InlineStyle,
): void {
  const number = context.footnoteNumbers.get(identifier);
  if (number !== undefined)
    appendRun(parts, key, `[${number}]`, {
      ...style,
      href: `#${context.prefix}-${markdownFootnoteKey(identifier)}`,
    });
}

function appendInline(
  node: PhrasingContent,
  parts: MarkdownInline[],
  context: MarkdownInlineContext,
  style: InlineStyle,
): void {
  const key = markdownNodeKey(node);
  switch (node.type) {
    case 'text':
    case 'html':
      appendRun(parts, key, node.value.replace(/[ \t\r\n\f]+/g, ' '), style);
      return;
    case 'break':
      appendRun(parts, key, '\n', style);
      return;
    case 'inlineCode':
      appendRun(parts, key, node.value, { ...style, mono: true });
      return;
    case 'image':
    case 'imageReference':
      appendImage(node, parts, context, style);
      return;
    case 'footnoteReference':
      appendFootnote(node.identifier, key, parts, context, style);
      return;
    case 'emphasis':
      style = { ...style, italic: true };
      break;
    case 'strong':
      style = { ...style, bold: true };
      break;
    case 'delete':
      style = { ...style, strike: true };
      break;
    case 'link':
      style = { ...style, href: markdownSafeUrl(node.url) || undefined, title: node.title ?? undefined };
      break;
    case 'linkReference': {
      const definition = context.definitions.get(node.identifier);
      style = {
        ...style,
        href: markdownSafeUrl(definition?.url ?? '') || undefined,
        title: definition?.title ?? undefined,
      };
      break;
    }
  }
  if ('children' in node) for (const child of node.children) appendInline(child, parts, context, style);
}

export function markdownInline(nodes: PhrasingContent[], context: MarkdownInlineContext): MarkdownInline[] {
  const parts: MarkdownInline[] = [];
  for (const node of nodes) appendInline(node, parts, context, {});
  return parts;
}
