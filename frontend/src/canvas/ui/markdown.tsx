import { useId, useMemo } from 'react';
import { MarkdownBlockView } from './markdownBlocks';
import { MarkdownFootnotes } from './markdownFootnotes';
import { createMarkdownParser } from './markdownParse';
import { colors } from './palette';
import { View } from './primitives';
import type { UIStyle } from './types';

interface MarkdownProps {
  id?: string;
  value: string;
  style?: UIStyle;
}

/** CommonMark plus GFM, parsed directly to canvas rich runs without HTML or a DOM render tree. */
export function Markdown({ id, value, style }: MarkdownProps) {
  const generatedId = useId();
  const prefix = id ?? `markdown-${generatedId}`;
  const parse = useMemo(() => createMarkdownParser(prefix), [prefix]);
  const document = useMemo(() => parse(value), [parse, value]);
  return (
    <View
      id={prefix}
      style={{ minWidth: 0, width: '100%', gap: 12, color: colors.text, fontSize: 16, lineHeight: 1.6, ...style }}
    >
      {document.blocks.map((block) => (
        <MarkdownBlockView key={block.key} prefix={prefix} block={block} />
      ))}
      <MarkdownFootnotes prefix={prefix} notes={document.footnotes} />
    </View>
  );
}
