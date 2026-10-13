import { memo } from 'react';
import { SourceImage } from './images';
import { colors } from './palette';
import { Text, View } from './primitives';
import type { UIStyle } from './types';
import type { MarkdownImage, MarkdownInline } from './markdownTypes';

interface InlineProps {
  id: string;
  parts: MarkdownInline[];
  style?: UIStyle;
}

function InlineImage({ id, image }: { id: string; image: MarkdownImage }) {
  return (
    <View
      id={id}
      title={image.title}
      href={image.href}
      external
      focusable={Boolean(image.href)}
      style={{ minWidth: 0, maxWidth: '100%', alignItems: 'start' }}
    >
      {image.src ? (
        <SourceImage
          id={`${id}-source`}
          src={image.src}
          alt={image.alt}
          style={{ width: '100%', maxWidth: 640, maxHeight: 360, minWidth: 0 }}
        />
      ) : (
        <Text style={{ color: colors.muted, minWidth: 0 }}>{image.alt}</Text>
      )}
    </View>
  );
}

/** Text runs stay in one shaped paragraph; images divide only at their actual source position. */
export const MarkdownInlines = memo(function MarkdownInlines({ id, parts, style }: InlineProps) {
  const first = parts[0];
  if (parts.length === 1 && first?.type === 'runs') {
    return <Text id={id} runs={first.runs} style={{ minWidth: 0, ...style }} />;
  }
  return (
    <View id={id} style={{ minWidth: 0, gap: 6, ...style }}>
      {parts.map((part) =>
        part.type === 'runs' ? (
          <Text key={part.key} id={`${id}-${part.key}`} runs={part.runs} style={{ minWidth: 0, ...style }} />
        ) : (
          <InlineImage key={part.key} id={`${id}-${part.key}`} image={part} />
        ),
      )}
    </View>
  );
});
