import { fromMarkdown } from 'mdast-util-from-markdown';
import { gfmFromMarkdown } from 'mdast-util-gfm';
import { gfm } from 'micromark-extension-gfm';
import type { FootnoteDefinition, Root, RootContent } from 'mdast';
import { markdownInline, markdownNodeKey, type MarkdownInlineContext } from './markdownInline';
import { markdownFootnoteKey, type MarkdownBlock, type MarkdownDocument } from './markdownTypes';

interface ParseContext extends MarkdownInlineContext {
  footnotes: Map<string, FootnoteDefinition>;
  targets: Map<string, Array<{ key: string; target: string }>>;
}

interface CachedBlock {
  source: string;
  dependencies: string;
  block: MarkdownBlock | null;
}

const options = { extensions: [gfm()], mdastExtensions: [gfmFromMarkdown()] };

function visit(node: Root | RootContent, callback: (node: RootContent) => void): void {
  if (node.type !== 'root') callback(node);
  if ('children' in node) for (const child of node.children) visit(child, callback);
}

function readDefinitions(root: Root, prefix: string): ParseContext {
  const context: ParseContext = {
    prefix,
    definitions: new Map(),
    footnotes: new Map(),
    footnoteNumbers: new Map(),
    targets: new Map(),
  };
  visit(root, (node) => {
    if (node.type === 'definition' && !context.definitions.has(node.identifier)) {
      context.definitions.set(node.identifier, node);
    }
    if (node.type === 'footnoteDefinition' && !context.footnotes.has(node.identifier)) {
      context.footnotes.set(node.identifier, node);
    }
  });
  for (const node of root.children) recordReferences(node, markdownNodeKey(node), context);
  // A referenced footnote may itself reference another footnote. Map iteration includes newly discovered entries.
  for (const identifier of context.footnoteNumbers.keys()) {
    const definition = context.footnotes.get(identifier);
    if (definition)
      for (const child of definition.children) recordReferences(child, markdownFootnoteKey(identifier), context);
  }
  return context;
}

function recordReferences(node: RootContent, target: string, context: ParseContext): void {
  if (node.type === 'footnoteDefinition') return;
  if (node.type === 'footnoteReference' && context.footnotes.has(node.identifier)) {
    if (!context.footnoteNumbers.has(node.identifier)) {
      context.footnoteNumbers.set(node.identifier, context.footnoteNumbers.size + 1);
    }
    const targets = context.targets.get(node.identifier) ?? [];
    targets.push({ key: markdownNodeKey(node), target });
    context.targets.set(node.identifier, targets);
  }
  if ('children' in node) for (const child of node.children) recordReferences(child, target, context);
}

function blockDependencies(node: RootContent, context: ParseContext): string {
  const references: unknown[] = [];
  visit(node, (child) => {
    if (child.type === 'linkReference' || child.type === 'imageReference') {
      const definition = context.definitions.get(child.identifier);
      references.push([child.identifier, definition?.url, definition?.title]);
    }
    if (child.type === 'footnoteReference') {
      references.push([child.identifier, context.footnoteNumbers.get(child.identifier)]);
    }
  });
  return references.length ? JSON.stringify(references) : '';
}

function makeTable(
  node: Extract<RootContent, { type: 'table' }>,
  key: string,
  context: ParseContext,
): Extract<MarkdownBlock, { type: 'table' }> {
  return {
    type: 'table',
    key,
    align: node.align ?? [],
    rows: node.children.map((row) => ({
      key: markdownNodeKey(row),
      cells: row.children.map((cell) => ({
        key: markdownNodeKey(cell),
        parts: markdownInline(cell.children, context),
      })),
    })),
  };
}

function makeBlock(
  node: RootContent,
  context: ParseContext,
  children: (nodes: RootContent[]) => MarkdownBlock[],
): MarkdownBlock | null {
  const key = markdownNodeKey(node);
  switch (node.type) {
    case 'paragraph':
      return { type: 'text', key, parts: markdownInline(node.children, context) };
    case 'heading':
      return { type: 'text', key, parts: markdownInline(node.children, context), depth: node.depth };
    case 'html':
      return {
        type: 'text',
        key,
        parts: [{ type: 'runs', key, runs: [{ text: node.value.replace(/[ \t\r\n\f]+/g, ' ') }] }],
      };
    case 'code':
      return { type: 'code', key, value: `${node.value}\n`, language: node.lang ?? null };
    case 'thematicBreak':
      return { type: 'rule', key };
    case 'blockquote':
      return { type: 'quote', key, blocks: children(node.children) };
    case 'list':
      return {
        type: 'list',
        key,
        ordered: Boolean(node.ordered),
        start: node.start ?? 1,
        spread: Boolean(node.spread),
        items: node.children.map((item) => ({
          key: markdownNodeKey(item),
          checked: item.checked ?? null,
          spread: Boolean(item.spread),
          blocks: children(item.children),
        })),
      };
    case 'table':
      return makeTable(node, key, context);
    default:
      return null; // Definitions contribute reference metadata, not visible blocks.
  }
}

/** Retain streamed block/run identities while references resolve against the complete document. */
export function createMarkdownParser(prefix: string): (value: string) => MarkdownDocument {
  let previous = new Map<string, CachedBlock>();
  let previousValue: string | null = null;
  let previousDocument: MarkdownDocument = { blocks: [], footnotes: [] };
  return (value) => {
    if (value === previousValue) return previousDocument;
    const root = fromMarkdown(value, options);
    const context = readDefinitions(root, prefix);
    const next = new Map<string, CachedBlock>();
    const convert = (nodes: RootContent[]): MarkdownBlock[] => {
      const blocks: MarkdownBlock[] = [];
      for (const node of nodes) {
        const key = markdownNodeKey(node);
        const source = value.slice(node.position?.start.offset, node.position?.end.offset);
        const dependencies = blockDependencies(node, context);
        const old = previous.get(key);
        if (old?.source === source && old.dependencies === dependencies) {
          visit(node, (child) => {
            const childKey = markdownNodeKey(child);
            const retained = previous.get(childKey);
            if (retained) next.set(childKey, retained);
          });
        }
        const cached =
          old?.source === source && old.dependencies === dependencies
            ? old
            : {
                source,
                dependencies,
                block: makeBlock(node, context, convert),
              };
        next.set(key, cached);
        if (cached.block) blocks.push(cached.block);
      }
      return blocks;
    };
    const blocks = convert(root.children);
    const footnotes = Array.from(context.footnoteNumbers, ([identifier, number]) => ({
      key: markdownFootnoteKey(identifier),
      number,
      blocks: convert(context.footnotes.get(identifier)!.children),
      targets: context.targets.get(identifier) ?? [],
    }));
    previous = next;
    previousValue = value;
    previousDocument = { blocks, footnotes };
    return previousDocument;
  };
}
