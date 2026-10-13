import type { TextRun } from './richTypes';

export interface MarkdownRuns {
  type: 'runs';
  key: string;
  runs: TextRun[];
}

export interface MarkdownImage {
  type: 'image';
  key: string;
  src: string;
  alt: string;
  title?: string;
  href?: string;
}

export type MarkdownInline = MarkdownRuns | MarkdownImage;

export interface MarkdownListItem {
  key: string;
  checked: boolean | null;
  spread: boolean;
  blocks: MarkdownBlock[];
}

export interface MarkdownCell {
  key: string;
  parts: MarkdownInline[];
}

export interface MarkdownRow {
  key: string;
  cells: MarkdownCell[];
}

export type MarkdownBlock =
  | { type: 'text'; key: string; parts: MarkdownInline[]; depth?: number }
  | { type: 'code'; key: string; value: string; language: string | null }
  | { type: 'quote'; key: string; blocks: MarkdownBlock[] }
  | { type: 'list'; key: string; ordered: boolean; start: number; spread: boolean; items: MarkdownListItem[] }
  | { type: 'rule'; key: string }
  | { type: 'table'; key: string; align: Array<'left' | 'center' | 'right' | null>; rows: MarkdownRow[] };

export interface MarkdownFootnote {
  key: string;
  number: number;
  blocks: MarkdownBlock[];
  targets: Array<{ key: string; target: string }>;
}

export interface MarkdownDocument {
  blocks: MarkdownBlock[];
  footnotes: MarkdownFootnote[];
}

export function markdownFootnoteKey(identifier: string): string {
  return `footnote-${encodeURIComponent(identifier)}`;
}
