import * as stylex from '@stylexjs/stylex';
import { useCallback, useEffect, useRef, useState } from 'react';
import { buttonStyles } from '@/components/ui/styles';
import AskPage from './Ask';
import ConversationRail from './ConversationRail';
import type { AskBootstrap, Conversation } from './types';
import { media } from '@/styles/constants.stylex';
import { colors, layout } from '@/styles/tokens.stylex';

const styles = stylex.create({
  askMain: {
    padding: {
      default: 'clamp(1.75rem, 5vw, 4.25rem) clamp(1rem, 6vw, 7rem) 2rem',
      [media.tablet]: 'clamp(1.25rem, 5vw, 2rem) clamp(1rem, 4vw, 2.5rem) 1rem',
    },
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    height: '100%',
    minHeight: 0,
    minWidth: 0,
  },
  askRouteError: {
    padding: '2rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    marginBlock: '3rem',
    marginInline: 'auto',
    backgroundColor: colors.surface,
    textAlign: 'center',
    maxWidth: '36rem',
  },
  appErrorActions: {
    gap: '0.75rem',
    display: 'flex',
    justifyContent: 'center',
    marginTop: '1.25rem',
  },
  askLayout: {
    gap: 0,
    marginBlock: 0,
    marginInline: 'auto',
    display: 'grid',
    gridTemplateColumns: {
      default: 'minmax(18rem, 19rem) minmax(0, 1fr)',
      [media.tablet]: 'minmax(0, 1fr)',
    },
    maxWidth: '73.75rem',
    minHeight: 'calc(100svh - 4.25rem)',
    minWidth: 0,
    width: '100%',
  },
  textMuted: {
    color: colors.textSecondary,
  },
});

interface AskContentProps {
  root: React.RefObject<HTMLElement | null>;
  bootstrap: AskBootstrap | null | undefined;
  error: string | null;
  onConversation: (conversation: Conversation) => void;
}

function AskContent({ root, bootstrap, error, onConversation }: AskContentProps) {
  return (
    <section ref={root} {...stylex.props(styles.askMain)} id="ask-root" aria-label="Ask your course material">
      {error ? (
        <div {...stylex.props(styles.askRouteError)} role="alert">
          <h1>Conversation unavailable</h1>
          <p>
            {error.includes('404')
              ? 'That saved conversation no longer exists.'
              : 'Ask could not load this conversation right now.'}
          </p>
          <div {...stylex.props(styles.appErrorActions)}>
            <a {...stylex.props(buttonStyles.base, buttonStyles.primary)} href="/ask">
              Start a new question
            </a>
            <button
              {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
              type="button"
              onClick={() => window.location.reload()}
            >
              Try again
            </button>
          </div>
        </div>
      ) : bootstrap ? (
        <AskPage
          root={root}
          conversationId={bootstrap.conversation_id}
          initialMessages={bootstrap.initial_messages}
          examples={bootstrap.examples}
          models={bootstrap.models}
          defaultModel={bootstrap.default_model}
          providerAvailable={bootstrap.provider_available}
          onConversation={onConversation}
        />
      ) : (
        <p {...stylex.props(styles.textMuted)} role="status">
          Opening Ask…
        </p>
      )}
    </section>
  );
}

interface AskRouteProps {
  query: string;
  initialData: AskBootstrap | null;
}

export function AskRoute({ query, initialData }: AskRouteProps) {
  const [bootstrap, setBootstrap] = useState<AskBootstrap | null>(initialData);
  const [error, setError] = useState<string | null>(null);
  const askRoot = useRef<HTMLElement>(null);
  const onConversation = useCallback((conversation: Conversation) => {
    const id = conversation?.id;
    if (!id) return;
    const url = new URL(window.location.href);
    url.searchParams.set('c', String(id));
    window.history.replaceState(window.history.state, '', url);
    setBootstrap((current) => (current ? { ...current, conversation_id: id } : current));
    window.dispatchEvent(new Event('tree:conversation-updated'));
  }, []);
  useEffect(() => {
    // The route loader already loaded the bootstrap into initialData; only fetch when
    // this route rendered without it (plain client navigation).
    if (initialData) return;
    const params = new URLSearchParams(query);
    const conversationId = params.get('c');
    const suffix = conversationId ? `?conversation_id=${encodeURIComponent(conversationId)}` : '';
    const controller = new AbortController();
    fetch(`/api/ask/bootstrap${suffix}`, { signal: controller.signal, headers: { Accept: 'application/json' } })
      .then((response) => {
        if (!response.ok) throw new Error(`Server returned HTTP ${response.status}`);
        // SAFETY: the bootstrap endpoint's JSON is the AskBootstrap contract
        // (see features/ask/types.ts); the cast is the typed-transport seam.
        return response.json() as Promise<AskBootstrap>;
      })
      .then((value) => {
        if (!controller.signal.aborted) setBootstrap(value);
      })
      .catch((loadError) => {
        if (!controller.signal.aborted) setError(loadError.message || 'Could not load Ask');
      });
    return () => controller.abort();
  }, [initialData, query]);
  return (
    <div {...stylex.props(styles.askLayout)}>
      <ConversationRail activeId={bootstrap?.conversation_id} />
      <AskContent root={askRoot} bootstrap={bootstrap} error={error} onConversation={onConversation} />
    </div>
  );
}
