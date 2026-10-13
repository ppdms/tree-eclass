import { useCallback, useRef, useState } from 'react';
import { useLoaderData, useLocation, useNavigate, useNavigation, type Location, type RouteObject } from 'react-router';
import type { loadAsk } from '../app/loaders';
import { reloadPage } from '../app/browser';
import { AppLink } from '../app/links';
import { useViewport } from '../app/viewport';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { useAskBootstrap, type AskLoadError } from './bootstrap';
import type { AskBootstrap, Conversation } from './model';
import { currentNewQuestionVersion, requestNewQuestion } from './navigation';
import { AskPage } from './page';
import { ConversationRail } from './rail';
import { styles } from './styles';

let acknowledgedQuery: string | null = null;

function ConversationUnavailable({ error }: { error: AskLoadError }) {
  return (
    <Notice id="ask-conversation-unavailable" tone="danger">
      <Text style={styles.title}>Conversation unavailable</Text>
      <Text style={styles.text}>
        {error.status === 404
          ? 'That saved conversation no longer exists.'
          : 'Ask could not load this conversation right now.'}
      </Text>
      <View style={styles.row}>
        <AppLink id="ask-unavailable-new" href="/ask" style={styles.link} onClick={requestNewQuestion}>
          Start a new question
        </AppLink>
        <Button id="ask-unavailable-reload" onClick={reloadPage}>
          Try again
        </Button>
      </View>
    </Notice>
  );
}

function useSavedConversation(identity: string) {
  const navigate = useNavigate();
  const location = useLocation();
  const navigation = useNavigation();
  const [savedId, setSavedId] = useState<string | number | null>(null);
  const [refreshed, setRefreshed] = useState(0);
  const onSaved = useCallback(
    (conversation: Conversation) => {
      // A committed turn must not cancel a navigation the learner has already started.
      if (navigation.state === 'idle') {
        const search = new URLSearchParams(location.search);
        search.set('c', String(conversation.id));
        acknowledgedQuery = `?${search}`;
        void navigate(
          { pathname: location.pathname, search: acknowledgedQuery, hash: location.hash },
          { replace: true, preventScrollReset: true, state: { ...location.state, askIdentity: identity } },
        );
      }
      setSavedId(conversation.id);
      setRefreshed((value) => value + 1);
    },
    [identity, location, navigate, navigation.state],
  );
  return { savedId, refreshed, onSaved };
}

function AskScreen({
  initialData,
  query,
  identity,
}: {
  initialData: AskBootstrap | null;
  query: string;
  identity: string;
}) {
  const { bootstrap, error } = useAskBootstrap(initialData, query);
  const { savedId, refreshed, onSaved } = useSavedConversation(identity);
  const viewport = useViewport();
  const desktop = viewport.width >= 900;
  const height = Math.max(200, viewport.bottom - viewport.top - 290);
  return (
    <View id="ask-route" ready={Boolean(bootstrap)} style={styles.page}>
      <View
        style={{
          ...styles.layout,
          flexDirection: desktop ? 'row' : 'column',
          alignItems: desktop ? 'start' : 'stretch',
        }}
      >
        <ConversationRail
          activeId={savedId ?? bootstrap?.conversation_id ?? null}
          refresh={refreshed}
          desktop={desktop}
          height={height}
        />
        {error ? (
          <ConversationUnavailable error={error} />
        ) : bootstrap ? (
          <AskPage bootstrap={bootstrap} query={query} onSaved={onSaved} />
        ) : (
          <Text id="ask-opening" style={styles.muted}>
            Opening Ask…
          </Text>
        )}
      </View>
    </View>
  );
}

function hasAskIdentity(state: Location['state']): state is { askIdentity: string } {
  return state !== null && typeof state === 'object' && 'askIdentity' in state && typeof state.askIdentity === 'string';
}

function AskRoutePage() {
  const data = useLoaderData<typeof loadAsk>();
  const location = useLocation();
  const resetVersion = currentNewQuestionVersion();
  const previous = useRef({ location, identity: location.key, resetVersion });
  if (previous.current.location.key !== location.key) {
    const old = previous.current.location;
    const hashOnly = old.pathname === location.pathname && old.search === location.search && old.hash !== location.hash;
    const identity =
      resetVersion !== previous.current.resetVersion
        ? location.key
        : hashOnly
          ? previous.current.identity
          : hasAskIdentity(location.state)
            ? location.state.askIdentity
            : location.key;
    previous.current = { location, identity, resetVersion };
  }
  return (
    <AskScreen
      key={previous.current.identity}
      identity={previous.current.identity}
      initialData={data}
      query={location.search}
    />
  );
}

function AskFallback() {
  return (
    <Text id="ask-opening" style={{ ...styles.muted, padding: 24 }}>
      Opening Ask…
    </Text>
  );
}

export const askRoute: RouteObject = {
  path: 'ask',
  shouldRevalidate: ({ nextUrl, defaultShouldRevalidate }) => {
    const acknowledged = acknowledgedQuery !== null && nextUrl.search === acknowledgedQuery;
    acknowledgedQuery = null;
    return acknowledged ? false : defaultShouldRevalidate;
  },
  Component: AskRoutePage,
  HydrateFallback: AskFallback,
};
