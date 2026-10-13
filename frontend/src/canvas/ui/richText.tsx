import { useEffect, useState } from 'react';
import { requestHTML } from '../app/html';
import type { RichBlock } from './richTypes';
import type { UIStyle } from './types';
import { Text, View } from './primitives';

interface ParsedHTML {
  html: string;
  blocks: RichBlock[];
  error: string;
}
interface RichTextProps {
  html: string;
  id?: string;
  style?: UIStyle;
}

export function RichText({ html, id, style }: RichTextProps) {
  const [parsed, setParsed] = useState<ParsedHTML | null>(null);
  useEffect(() => requestHTML(html, (blocks, error) => setParsed({ html, blocks, error: error ?? '' })), [html]);
  if (parsed?.html !== html) return <Text style={style}>Opening description…</Text>;
  if (parsed.error) return <Text style={style}>Description could not render: {parsed.error}</Text>;
  return (
    <View id={id} style={{ gap: 8, minWidth: 0, ...style }}>
      {parsed.blocks.map((block, index) => (
        <Text
          key={index}
          id={id ? `${id}-paragraph-${index}` : undefined}
          runs={block.runs}
          style={{ minWidth: 0, mono: block.pre, lineHeight: 1.5 }}
        />
      ))}
    </View>
  );
}
