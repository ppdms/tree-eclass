import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ArrowDown, TriangleAlert } from 'lucide-react';
import { isTextUIPart, isToolUIPart, type ChatStatus, type ToolUIPart, type UIMessage } from 'ai';
import { Button as ActionButton } from '@/components/ui/button';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';
import { SourceBlock, ToolCall } from './ToolParts';
export { Examples } from './AskExamples';
const styles = stylex.create({
  askMessage: {
    gap: spacing.sm,
    display: 'flex',
    flexDirection: 'column',
    width: '100%',
  },
  isUser: {
    alignItems: 'flex-end',
  },
  isAssistant: {
    alignItems: 'flex-start',
  },
  askMessageBubble: {
    padding: spacing.md,
    borderRadius: layout.radiusLarge,
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimary,
    maxWidth: '85%',
  },
  userText: { margin: 0, whiteSpace: 'pre-wrap' },
  askAnswer: {
    color: colors.textPrimary,
    lineHeight: 1.6,
    maxWidth: '100%',
    width: '100%',
  },
  askAnswerFooter: {
    gap: spacing.md,
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    width: '100%',
  },
  askLookupNote: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  askThreadStatus: {
    gap: spacing.sm,
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: typography.sizeSm,
  },
  askLoadingMark: {
    borderRadius: '9999px',
    backgroundColor: colors.primary,
    display: 'inline-block',
    height: '0.5rem',
    width: '0.5rem',
  },
  askThreadError: {
    padding: spacing.md,
    borderColor: colors.danger,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.md,
    alignItems: 'center',
    backgroundColor: colors.surfaceDanger,
    color: colors.danger,
    display: 'flex',
    justifyContent: 'space-between',
    width: '100%',
  },
  askThread: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    position: 'relative',
    minHeight: 0,
    overflowY: 'auto',
  },
  askThreadEmpty: {
    display: 'none',
  },
  askThreadList: {
    gap: spacing.xl,
    display: 'flex',
    flexDirection: 'column',
    paddingBottom: spacing.xl,
  },
  askJumpLatest: {
    position: 'absolute',
    transform: 'translateX(50%)',
    zIndex: 10,
    bottom: spacing.md,
    right: '50%',
  },
});

function UserMessage({ text }: { text: string }) {
  return (
    <article {...stylex.props(styles.askMessage, styles.isUser)} aria-label="Question">
      <div {...stylex.props(styles.askMessageBubble)}>
        <p {...stylex.props(styles.userText)}>{text}</p>
      </div>
    </article>
  );
}

function lookupNote(hasTools: boolean): string {
  return hasTools
    ? 'Generated from course material. Check the lookup trail — answers can be wrong.'
    : 'Generated from course material. Answers can be wrong.';
}

function AnswerFooter({
  hasTools,
  showRetry,
  onRetry,
  busy,
}: {
  hasTools: boolean;
  showRetry: boolean;
  onRetry: () => void;
  busy: boolean;
}) {
  return (
    <div {...stylex.props(styles.askAnswerFooter)}>
      <p {...stylex.props(styles.askLookupNote)}>{lookupNote(hasTools)}</p>
      {showRetry && (
        <ActionButton type="button" variant="outline" size="sm" onClick={onRetry} disabled={busy}>
          Try again
        </ActionButton>
      )}
    </div>
  );
}

function AssistantParts({ parts }: { parts: UIMessage['parts'] }) {
  return (
    <>
      {parts.map((part, index) => {
        if (isTextUIPart(part)) {
          return (
            <div key={index} {...stylex.props(styles.askAnswer)}>
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{part.text}</ReactMarkdown>
            </div>
          );
        }
        if (isToolUIPart(part)) {
          return part.state === 'output-available' ? null : <ToolCall key={part.toolCallId || index} part={part} />;
        }
        return null;
      })}
    </>
  );
}

interface MessageItemProps {
  message: UIMessage;
  showRetry: boolean;
  onRetry: () => void;
  busy: boolean;
}

function AssistantMessage({ message, showRetry, onRetry, busy }: MessageItemProps) {
  const parts = message.parts || [];
  const tools = parts.filter((part): part is ToolUIPart => isToolUIPart(part));
  return (
    <article {...stylex.props(styles.askMessage, styles.isAssistant)} aria-label="Answer">
      <AssistantParts parts={parts} />
      <SourceBlock parts={tools} />
      <AnswerFooter hasTools={tools.length > 0} showRetry={showRetry} onRetry={onRetry} busy={busy} />
    </article>
  );
}

function Message({ message, showRetry, onRetry, busy }: MessageItemProps) {
  if (message.role === 'user') {
    const text = message.parts?.find((part) => isTextUIPart(part));
    return <UserMessage text={text && isTextUIPart(text) ? text.text : ''} />;
  }
  return <AssistantMessage message={message} showRetry={showRetry} onRetry={onRetry} busy={busy} />;
}

function friendlyError(error: Error | null): string {
  const raw = String(error?.message || '');
  try {
    const body = JSON.parse(raw);
    if (body?.detail) return String(body.detail);
  } catch {
    /* The SDK may expose a plain status message instead. */
  }
  if (/failed to fetch/i.test(raw)) return 'Could not reach the server. Check your connection and try again.';
  return raw || 'The question could not be answered.';
}

function ThreadStatus({ status, messages }: { status: ChatStatus; messages: UIMessage[] }) {
  const parts = messages[messages.length - 1]?.parts || [];
  const hasRunningTool = parts.some((part) => isToolUIPart(part) && part.state !== 'output-available');
  const hasText = parts.some((part) => isTextUIPart(part) && part.text);
  if (hasRunningTool) return null;
  const copy =
    status === 'submitted' ? 'Sending the question…' : status === 'streaming' && !hasText ? 'Writing an answer…' : null;
  if (!copy) return null;
  return (
    <p {...stylex.props(styles.askThreadStatus)} role="status" aria-live="polite">
      <span {...stylex.props(styles.askLoadingMark)} aria-hidden="true" />
      {copy}
    </p>
  );
}

function ThreadError({
  error,
  onRetry,
  busy,
}: {
  error: Error | null | undefined;
  onRetry: () => void;
  busy: boolean;
}) {
  if (!error) return null;
  return (
    <div {...stylex.props(styles.askThreadError)} role="alert">
      <span>
        <TriangleAlert aria-hidden="true" />
        {friendlyError(error)}
      </span>
      <ActionButton type="button" variant="outline" size="sm" onClick={onRetry} disabled={busy}>
        Try again
      </ActionButton>
    </div>
  );
}

function useThreadScroll(messages: UIMessage[], status: ChatStatus) {
  const logRef = React.useRef<HTMLDivElement | null>(null);
  const endRef = React.useRef<HTMLDivElement | null>(null);
  const [pinned, setPinned] = React.useState(true);
  const [showJump, setShowJump] = React.useState(false);

  const updatePosition = () => {
    const log = logRef.current;
    if (!log) return;
    const nextPinned = log.scrollHeight - log.scrollTop - log.clientHeight < 64;
    setPinned(nextPinned);
    setShowJump(!nextPinned);
  };
  React.useEffect(() => {
    if (pinned) endRef.current?.scrollIntoView({ behavior: 'auto', block: 'end' });
  }, [messages, status, pinned]);

  const jumpToLatest = () => {
    const reduced = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
    endRef.current?.scrollIntoView({ behavior: reduced ? 'auto' : 'smooth', block: 'end' });
    setPinned(true);
    setShowJump(false);
  };
  return { logRef, endRef, pinned, showJump, updatePosition, jumpToLatest };
}

function ThreadMessages({
  messages,
  status,
  error,
  onRetry,
  busy,
}: {
  messages: UIMessage[];
  status: ChatStatus;
  error: Error | null | undefined;
  onRetry: () => void;
  busy: boolean;
}) {
  const lastAssistant = [...messages].reverse().find((item) => item.role === 'assistant');
  return (
    <>
      {messages.map((message) => (
        <Message
          key={message.id}
          message={message}
          showRetry={message.id === lastAssistant?.id && status === 'ready' && !error}
          onRetry={onRetry}
          busy={busy}
        />
      ))}
      <ThreadStatus status={status} messages={messages} />
      <ThreadError error={error} onRetry={onRetry} busy={busy} />
    </>
  );
}

export function Thread({
  messages,
  status,
  error,
  onRetry,
  busy,
}: {
  messages: UIMessage[];
  status: ChatStatus;
  error: Error | null | undefined;
  onRetry: () => void;
  busy: boolean;
}) {
  const { logRef, endRef, showJump, updatePosition, jumpToLatest } = useThreadScroll(messages, status);
  return (
    <div
      ref={logRef}
      {...stylex.props(styles.askThread, messages.length === 0 && styles.askThreadEmpty)}
      role="log"
      aria-label="Conversation"
      onScroll={updatePosition}
    >
      <div {...stylex.props(styles.askThreadList)}>
        <ThreadMessages messages={messages} status={status} error={error} onRetry={onRetry} busy={busy} />
        <div ref={endRef} />
      </div>
      {showJump && (
        <ActionButton
          type="button"
          variant="outline"
          size="sm"
          {...stylex.props(styles.askJumpLatest)}
          onClick={jumpToLatest}
        >
          Jump to latest answer <ArrowDown aria-hidden="true" />
        </ActionButton>
      )}
    </div>
  );
}
