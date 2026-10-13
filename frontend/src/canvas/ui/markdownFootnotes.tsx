import { MarkdownBlockView } from './markdownBlocks';
import type { MarkdownFootnote } from './markdownTypes';
import { colors } from './palette';
import { Text, View } from './primitives';

export function MarkdownFootnotes({ prefix, notes }: { prefix: string; notes: MarkdownFootnote[] }) {
  if (!notes.length) return null;
  return (
    <View
      id={`${prefix}-footnotes`}
      style={{
        minWidth: 0,
        gap: 12,
        paddingTop: 12,
        borderWidth: 1,
        borderColor: colors.border,
        borderRadius: 8,
        padding: 12,
      }}
    >
      <Text style={{ fontWeight: 600, fontSize: 14 }}>Footnotes</Text>
      {notes.map((note) => (
        <View
          key={note.key}
          id={`${prefix}-${note.key}`}
          style={{ minWidth: 0, flexDirection: 'row', gap: 8, alignItems: 'start', fontSize: 14 }}
        >
          <Text style={{ minWidth: 20, textAlign: 'right' }}>{note.number}.</Text>
          <View style={{ flexGrow: 1, flexShrink: 1, minWidth: 0, gap: 8 }}>
            {note.blocks.map((block) => (
              <MarkdownBlockView key={block.key} prefix={prefix} block={block} />
            ))}
            <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 10, minWidth: 0 }}>
              {note.targets.map(({ key, target }, occurrence) => (
                <Text
                  key={key}
                  id={`${prefix}-${note.key}-back-${key}`}
                  label={`Return to footnote reference ${occurrence + 1}`}
                  runs={[{ text: occurrence ? `↩ ${occurrence + 1}` : '↩', href: `#${prefix}-${target}` }]}
                />
              ))}
            </View>
          </View>
        </View>
      ))}
    </View>
  );
}
