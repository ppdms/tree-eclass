import { memo } from 'react';
import { MarkdownInlines } from './markdownInlines';
import { MarkdownTable } from './markdownTable';
import type { MarkdownBlock, MarkdownListItem } from './markdownTypes';
import { colors } from './palette';
import { Text, Vector, View } from './primitives';

interface BlockProps {
  prefix: string;
  block: MarkdownBlock;
}

const headingSizes = new Map([
  [1, 30],
  [2, 26],
  [3, 22],
  [4, 20],
  [5, 18],
  [6, 16],
]);

function TaskMark({ id, checked }: { id: string; checked: boolean }) {
  return (
    <View
      id={id}
      disabled
      label={checked ? 'Completed task' : 'Incomplete task'}
      style={{
        width: 16,
        height: 16,
        marginTop: 4,
        borderWidth: 1,
        borderColor: colors.muted,
        borderRadius: 3,
        backgroundColor: checked ? colors.raised : colors.surface,
      }}
    >
      {checked && <Vector path="M 2 7 L 5 10 L 12 3" stroke={colors.muted} style={{ width: 14, height: 14 }} />}
    </View>
  );
}

function ListItem({ prefix, item, marker }: { prefix: string; item: MarkdownListItem; marker: string }) {
  const id = `${prefix}-${item.key}`;
  return (
    <View id={id} style={{ flexDirection: 'row', gap: 8, minWidth: 0, alignItems: 'start' }}>
      <Text id={`${id}-marker`} style={{ minWidth: 20, textAlign: 'right' }}>
        {marker}
      </Text>
      {item.checked !== null && <TaskMark id={`${id}-task`} checked={item.checked} />}
      <View style={{ flexGrow: 1, flexShrink: 1, minWidth: 0, gap: item.spread ? 12 : 6 }}>
        {item.blocks.map((block) => (
          <MarkdownBlockView key={block.key} prefix={prefix} block={block} />
        ))}
      </View>
    </View>
  );
}

function List({ prefix, block }: { prefix: string; block: Extract<MarkdownBlock, { type: 'list' }> }) {
  return (
    <View id={`${prefix}-${block.key}`} style={{ minWidth: 0, paddingLeft: 10, gap: block.spread ? 12 : 6 }}>
      {block.items.map((item, index) => (
        <ListItem key={item.key} prefix={prefix} item={item} marker={block.ordered ? `${block.start + index}.` : '•'} />
      ))}
    </View>
  );
}

function Quote({ prefix, block }: { prefix: string; block: Extract<MarkdownBlock, { type: 'quote' }> }) {
  return (
    <View id={`${prefix}-${block.key}`} style={{ flexDirection: 'row', minWidth: 0, gap: 12 }}>
      <View style={{ width: 3, backgroundColor: colors.border, borderRadius: 2 }} />
      <View style={{ minWidth: 0, flexGrow: 1, flexShrink: 1, gap: 12, color: colors.muted }}>
        {block.blocks.map((child) => (
          <MarkdownBlockView key={child.key} prefix={prefix} block={child} />
        ))}
      </View>
    </View>
  );
}

function Code({ prefix, block }: { prefix: string; block: Extract<MarkdownBlock, { type: 'code' }> }) {
  const id = `${prefix}-${block.key}`;
  return (
    <View
      id={id}
      focusable
      label={block.language ? `${block.language} code block` : 'Code block'}
      scroll={{ axis: 'both' }}
      style={{
        width: '100%',
        minWidth: 0,
        maxHeight: 360,
        padding: 12,
        backgroundColor: colors.raised,
        borderWidth: 1,
        borderColor: colors.border,
        borderRadius: 8,
        overflow: 'hidden',
      }}
    >
      <Text
        id={`${id}-source`}
        style={{ mono: true, fontSize: 14, lineHeight: 1.5, whiteSpace: 'pre', alignSelf: 'start', flexShrink: 0 }}
      >
        {block.value}
      </Text>
    </View>
  );
}

export const MarkdownBlockView = memo(function MarkdownBlockView({ prefix, block }: BlockProps) {
  switch (block.type) {
    case 'text':
      return (
        <MarkdownInlines
          id={`${prefix}-${block.key}`}
          parts={block.parts}
          style={
            block.depth
              ? { fontSize: headingSizes.get(block.depth), fontWeight: 700, lineHeight: 1.3, marginTop: 8 }
              : undefined
          }
        />
      );
    case 'code':
      return <Code prefix={prefix} block={block} />;
    case 'quote':
      return <Quote prefix={prefix} block={block} />;
    case 'list':
      return <List prefix={prefix} block={block} />;
    case 'table':
      return <MarkdownTable prefix={prefix} block={block} />;
    case 'rule':
      return (
        <View
          id={`${prefix}-${block.key}`}
          style={{ height: 1, marginTop: 8, marginBottom: 8, backgroundColor: colors.border }}
        />
      );
  }
});
