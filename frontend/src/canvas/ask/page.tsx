import { useCallback, useEffect, useState } from 'react';
import { focusField } from '../app/browser';
import { useViewport } from '../app/viewport';
import { useNodeRect } from '../ui/layoutEvents';
import { Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { AskComposer } from './composer';
import { AskExamples, AskHeader, ProviderNotice } from './content';
import { useAskController } from './controller';
import { friendlyError, type AskBootstrap, type Conversation } from './model';
import { styles } from './styles';
import { AskThread, MIN_TRANSCRIPT_HEIGHT } from './thread';

const emptyModels: string[] = [];

function useAskLayout(hasMessages: boolean) {
  const viewport = useViewport();
  const root = useNodeRect('ask-root');
  const app = useNodeRect('app-root');
  const heading = useNodeRect('ask-heading');
  const composer = useNodeRect('ask-compose-area');
  // Subtract root scroll from both rectangles: the shell offset must not grow when the page is scrolled.
  const shellOffset = root && app ? root.y - app.y : viewport.top;
  const available = Math.max(0, viewport.bottom - Math.max(viewport.top, shellOffset) - styles.page.padding);
  const minimum = (heading?.height ?? 0) + (composer?.height ?? 0) + styles.main.gap * 2 + MIN_TRANSCRIPT_HEIGHT;
  return hasMessages ? { height: available, minHeight: minimum } : {};
}

function useAskFocus(hasMessages: boolean, query: string) {
  useEffect(() => {
    if (hasMessages || !new URLSearchParams(query).has('focus')) return;
    focusField('ask-question');
    const timer = setTimeout(() => focusField('ask-question'), 50);
    return () => clearTimeout(timer);
  }, [hasMessages, query]);
}

function AskHeading({ hasMessages, available }: { hasMessages: boolean; available: boolean }) {
  return (
    <View id="ask-heading" style={{ gap: 18, flexShrink: 0 }}>
      <AskHeader hasMessages={hasMessages} />
      {!available && <ProviderNotice />}
    </View>
  );
}

function ActionError({ error }: { error: string | null }) {
  return error ? (
    <Notice id="ask-action-error" tone="danger">
      <Text style={styles.text}>{error}</Text>
    </Notice>
  ) : null;
}

export function AskPage({
  bootstrap,
  query,
  onSaved,
}: {
  bootstrap: AskBootstrap;
  query: string;
  onSaved: (value: Conversation) => void;
}) {
  const chat = useAskController(bootstrap, onSaved);
  const [actionError, setActionError] = useState<string | null>(null);
  const hasMessages = chat.messages.length > 0;
  const available = bootstrap.provider_available ?? true;
  const busy = chat.status === 'submitted' || chat.status === 'streaming';
  const layout = useAskLayout(hasMessages);
  useAskFocus(hasMessages, query);
  const retry = useCallback(() => {
    setActionError(null);
    void chat.retry().catch((error) => setActionError(friendlyError(error)));
  }, [chat.retry]);
  const pick = (question: string) => {
    setActionError(null);
    void chat.send(question).catch((error) => setActionError(friendlyError(error)));
  };
  return (
    <View id="ask-root" style={{ ...styles.main, ...layout }} label="Ask your course material">
      <AskHeading hasMessages={hasMessages} available={available} />
      <AskThread messages={chat.messages} status={chat.status} error={chat.error} busy={busy} onRetry={retry} />
      <View id="ask-compose-area" style={{ gap: 18, flexShrink: 0 }}>
        <AskComposer
          busy={busy}
          available={available}
          models={bootstrap.models || emptyModels}
          model={chat.model}
          setModel={chat.setModel}
          send={chat.send}
          stop={chat.stop}
        />
        <ActionError error={actionError} />
      </View>
      {!hasMessages && available && Boolean(bootstrap.examples?.length) && (
        <AskExamples examples={bootstrap.examples || []} disabled={busy} onPick={pick} />
      )}
    </View>
  );
}
