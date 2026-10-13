import type { ToolUIPart } from 'ai';
import { AppLink } from '../app/links';
import { colors } from '../ui/palette';
import { Text, View } from '../ui/primitives';
import { lookupSource, toolFailed, toolLabel } from './model';
import { styles } from './styles';

export function ToolProgress({ part, messageId }: { part: ToolUIPart; messageId: string }) {
  const running = part.state !== 'output-available';
  const failed = toolFailed(part);
  return (
    <View id={`ask-${messageId}-tool-${part.toolCallId}`} style={styles.tool}>
      <Text style={{ ...styles.small, color: failed ? colors.danger : colors.muted }}>
        {toolLabel(part)}
        {failed ? ' — unavailable' : running ? ' — in progress' : ' — complete'}
      </Text>
    </View>
  );
}

function Source({ part, messageId }: { part: ToolUIPart; messageId: string }) {
  const source = lookupSource(part);
  const kindColor =
    source.kind === 'official'
      ? colors.success
      : source.kind === 'past exam'
        ? colors.warning
        : source.kind === 'community'
          ? colors.accent
          : colors.muted;
  const content = (
    <>
      <Text style={{ ...styles.small, color: kindColor }}>{source.kind}</Text>
      <Text style={styles.text}>{source.title}</Text>
      {source.page > 0 && <Text style={styles.small}>p. {source.page}</Text>}
    </>
  );
  const id = `ask-${messageId}-source-${part.toolCallId}`;
  return source.href ? (
    <AppLink id={id} href={source.href} block style={styles.source} title={source.title}>
      {content}
      <Text style={styles.link}>Open source</Text>
    </AppLink>
  ) : (
    <View id={id} style={styles.source}>
      {content}
    </View>
  );
}

export function CourseLookups({ parts, messageId }: { parts: ToolUIPart[]; messageId: string }) {
  const successful = parts.filter((part) => !toolFailed(part));
  return (
    <View id={`ask-${messageId}-lookups`} style={styles.sources} label="Course lookups">
      <Text style={{ ...styles.text, fontWeight: 600 }}>Course lookups</Text>
      {successful.length ? (
        successful.map((part) => <Source key={part.toolCallId} part={part} messageId={messageId} />)
      ) : (
        <Text style={styles.small}>No successful course lookup was recorded.</Text>
      )}
    </View>
  );
}
