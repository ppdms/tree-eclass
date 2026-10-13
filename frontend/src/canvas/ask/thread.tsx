import { isTextUIPart, isToolUIPart, type ChatStatus, type UIMessage } from 'ai';
import { memo, useCallback, useEffect, useRef, useState } from 'react';
import { scrollAreaTo } from '../app/browser';
import { Button, Notice } from '../ui/controls';
import { Markdown } from '../ui/markdown';
import { ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { friendlyError } from './model';
import { styles } from './styles';
import { CourseLookups, ToolProgress } from './tools';

export const MIN_TRANSCRIPT_HEIGHT = 140;

interface MessageProps {
  message: UIMessage;
  showRetry: boolean;
  busy: boolean;
  onRetry: () => void;
}

function AssistantMessage({ message, showRetry, busy, onRetry }: MessageProps) {
  const tools = message.parts.filter(isToolUIPart);
  let textAnchor = 'start';
  return (
    <View id={`ask-message-${message.id}`} style={styles.message} label="Answer">
      {message.parts.map((part) => {
        if (isTextUIPart(part)) {
          return (
            <Markdown
              key={`text-after-${textAnchor}`}
              id={`ask-${message.id}-text-after-${textAnchor}`}
              value={part.text}
            />
          );
        }
        if (isToolUIPart(part)) {
          // Native streams close each text section before a tool; the call ID anchors the next section.
          textAnchor = part.toolCallId;
          if (part.state !== 'output-available') {
            return <ToolProgress key={part.toolCallId} messageId={message.id} part={part} />;
          }
        }
        return null;
      })}
      <CourseLookups parts={tools} messageId={message.id} />
      <View style={styles.row}>
        <Text style={{ ...styles.small, flexGrow: 1, flexShrink: 1, minWidth: 0, maxWidth: '100%' }}>
          {tools.length
            ? 'Generated from course material. Check the lookup trail — answers can be wrong.'
            : 'Generated from course material. Answers can be wrong.'}
        </Text>
        {showRetry && (
          <Button id={`ask-retry-${message.id}`} onClick={onRetry} disabled={busy}>
            Try again
          </Button>
        )}
      </View>
    </View>
  );
}

const Message = memo(function Message(props: MessageProps) {
  if (props.message.role !== 'user') return <AssistantMessage {...props} />;
  const part = props.message.parts.find(isTextUIPart);
  return (
    <View id={`ask-message-${props.message.id}`} style={styles.message} label="Question">
      <Text style={styles.question}>{part?.text || ''}</Text>
    </View>
  );
});

function ThreadStatus({ status, messages }: { status: ChatStatus; messages: UIMessage[] }) {
  const parts = messages[messages.length - 1]?.parts || [];
  if (parts.some((part) => isToolUIPart(part) && part.state !== 'output-available')) return null;
  const hasText = parts.some((part) => isTextUIPart(part) && part.text);
  const copy =
    status === 'submitted' ? 'Sending the question…' : status === 'streaming' && !hasText ? 'Writing an answer…' : null;
  return copy ? (
    <Text id="ask-stream-status" style={styles.muted}>
      {copy}
    </Text>
  ) : null;
}

interface ScrollMetrics {
  offset: number;
  contentHeight: number;
  viewportHeight: number;
}

function useTranscriptScroll(messages: UIMessage[], status: ChatStatus) {
  const [pinned, setPinned] = useState(true);
  const previous = useRef<ScrollMetrics | null>(null);
  const onScrollMetrics = useCallback((metrics: ScrollMetrics) => {
    const last = previous.current;
    previous.current = metrics;
    setPinned((current) => {
      if (current && last && metrics.offset === last.offset && metrics.contentHeight >= last.contentHeight) {
        return true;
      }
      return metrics.contentHeight - metrics.offset - metrics.viewportHeight < 64;
    });
  }, []);
  useEffect(() => {
    if (pinned && messages.length) scrollAreaTo('ask-transcript', Infinity, { behavior: 'auto' });
  }, [messages, status, pinned]);
  const jump = () => {
    setPinned(true);
    scrollAreaTo('ask-transcript', Infinity, { behavior: 'smooth' });
  };
  return { pinned, onScrollMetrics, jump };
}

export interface AskThreadProps {
  messages: UIMessage[];
  status: ChatStatus;
  error: Error | undefined;
  busy: boolean;
  onRetry: () => void;
}

export function AskThread({ messages, status, error, busy, onRetry }: AskThreadProps) {
  const { pinned, onScrollMetrics, jump } = useTranscriptScroll(messages, status);
  if (!messages.length && !error) return null;
  let lastAssistant: UIMessage | undefined;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (messages[index]?.role === 'assistant') {
      lastAssistant = messages[index];
      break;
    }
  }
  return (
    <View style={{ ...styles.column, minHeight: MIN_TRANSCRIPT_HEIGHT, flexGrow: 1, flexShrink: 1 }}>
      <ScrollArea
        id="ask-transcript"
        height="100%"
        style={{ flexGrow: 1, flexShrink: 1 }}
        onScrollMetrics={onScrollMetrics}
      >
        <View style={styles.transcript} label="Conversation">
          {messages.map((message) => (
            <Message
              key={message.id}
              message={message}
              busy={busy}
              onRetry={onRetry}
              showRetry={message.id === lastAssistant?.id && status === 'ready' && !error}
            />
          ))}
          <ThreadStatus status={status} messages={messages} />
          {error && (
            <Notice id="ask-answer-error" tone="danger">
              <Text style={styles.text}>{friendlyError(error)}</Text>
              <Button id="ask-error-retry" onClick={onRetry} disabled={busy}>
                Try again
              </Button>
            </Notice>
          )}
          <View id="ask-transcript-end" style={{ height: 1 }} />
        </View>
      </ScrollArea>
      {!pinned && (
        <Button id="ask-jump-latest" onClick={jump} style={{ position: 'absolute', bottom: 16, right: 16 }}>
          Jump to latest answer
        </Button>
      )}
    </View>
  );
}
