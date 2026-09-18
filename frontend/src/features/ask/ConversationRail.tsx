import { useDeferredResource } from '@/lib/useDeferredResource';
import { media } from '@/styles/constants.stylex';
import * as stylex from '@stylexjs/stylex';
import { conversationRailStyles } from './conversationRailStyles';
import * as React from 'react';
import { MessageSquare, Plus, Search } from 'lucide-react';
import { buttonStyles } from '@/components/ui/styles';
import type { Conversation } from '@/features/ask/types';
import { ConversationItem } from './ConversationItem';

const styles = conversationRailStyles;

function ConversationList({
  visible,
  query,
  activeId,
  onChanged,
}: {
  visible: Conversation[];
  query: string;
  activeId: string | number | null | undefined;
  onChanged: () => void;
}) {
  return (
    <nav {...stylex.props(styles.askHistory)} aria-label="Saved conversations">
      {visible.length ? (
        visible.map((item) => (
          <ConversationItem
            key={item.id}
            item={item}
            active={String(item.id) === String(activeId)}
            onChanged={onChanged}
          />
        ))
      ) : (
        <p {...stylex.props(styles.askHistoryEmpty)}>
          {query ? (
            'No conversations match that search.'
          ) : (
            <>
              No saved questions yet. <a href="/ask">Ask one to keep it here.</a>
            </>
          )}
        </p>
      )}
    </nav>
  );
}

function ConversationHistory({
  error,
  load,
  visible,
  query,
  activeId,
}: {
  error: string | null;
  load: () => void;
  visible: Conversation[];
  query: string;
  activeId: string | number | null | undefined;
}) {
  if (error)
    return (
      <div {...stylex.props(styles.askHistoryError)} role="alert">
        <span>Conversation history is unavailable.</span>
        <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.secondary)} onClick={load}>
          Retry
        </button>
      </div>
    );
  return <ConversationList visible={visible} query={query} activeId={activeId} onChanged={load} />;
}

function ConversationSidebarHeading({ count }: { count: number }) {
  return (
    <div {...stylex.props(styles.askSidebarHeading)}>
      <div {...stylex.props(styles.askSidebarSectionTitle)}>
        <MessageSquare aria-hidden="true" />
        <p {...stylex.props(styles.askSidebarTitle)}>Conversations</p>
      </div>
      <span {...stylex.props(styles.askSidebarCount)}>{count}</span>
    </div>
  );
}

function ConversationSearch({ query, setQuery }: { query: string; setQuery: (value: string) => void }) {
  return (
    <label {...stylex.props(styles.askHistorySearch)}>
      <Search size={15} aria-hidden="true" {...stylex.props(styles.searchIcon)} />
      <input
        type="search"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Search conversations"
        aria-label="Search conversations"
        {...stylex.props(styles.searchInput)}
      />
    </label>
  );
}

function ConversationSidebar({
  conversations,
  visible,
  query,
  setQuery,
  activeId,
  load,
  error,
  mobileOpen,
}: {
  conversations: Conversation[];
  visible: Conversation[];
  query: string;
  setQuery: (value: string) => void;
  activeId: string | number | null | undefined;
  load: () => void;
  error: string | null;
  mobileOpen: boolean;
}) {
  return (
    <aside
      {...stylex.props(styles.askSidebar, mobileOpen && styles.askSidebarMobileOpen)}
      aria-label="Ask conversations"
    >
      <a {...stylex.props(styles.askNew)} href="/ask">
        <Plus aria-hidden="true" />
        <span>New question</span>
      </a>
      <ConversationSidebarHeading count={conversations.length} />
      <ConversationSearch query={query} setQuery={setQuery} />
      <ConversationHistory error={error} load={load} visible={visible} query={query} activeId={activeId} />
    </aside>
  );
}

function ConversationRailContent({
  conversations,
  visible,
  query,
  setQuery,
  activeId,
  load,
  error,
  mobileOpen,
  setMobileOpen,
}: {
  conversations: Conversation[];
  visible: Conversation[];
  query: string;
  setQuery: (value: string) => void;
  activeId: string | number | null | undefined;
  load: () => void;
  error: string | null;
  mobileOpen: boolean;
  setMobileOpen: React.Dispatch<React.SetStateAction<boolean>>;
}) {
  return (
    <div {...stylex.props(styles.askRail)}>
      <button
        type="button"
        {...stylex.props(styles.askHistoryToggle)}
        aria-expanded={mobileOpen}
        aria-controls="ask-conversation-sidebar"
        onClick={() => setMobileOpen((open) => !open)}
      >
        <span>{mobileOpen ? 'Hide conversations' : 'Show conversations'}</span>
        <span aria-hidden="true">{mobileOpen ? '−' : '+'}</span>
      </button>
      <div id="ask-conversation-sidebar">
        <ConversationSidebar
          conversations={conversations}
          visible={visible}
          query={query}
          setQuery={setQuery}
          activeId={activeId}
          load={load}
          error={error}
          mobileOpen={mobileOpen}
        />
      </div>
    </div>
  );
}

export interface ConversationRailProps {
  activeId?: string | number | null;
}

function useDesktopHistory() {
  const [desktop, setDesktop] = React.useState(false);
  React.useEffect(() => {
    const query = window.matchMedia(media.tablet.replace('@media ', ''));
    const update = () => setDesktop(!query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  return desktop;
}

export default function ConversationRail({ activeId }: ConversationRailProps) {
  const [query, setQuery] = React.useState('');
  const [mobileOpen, setMobileOpen] = React.useState(false);
  const desktop = useDesktopHistory();
  const {
    data,
    error: failed,
    retry: load,
  } = useDeferredResource<{ conversations: Conversation[] }>('/api/ask/conversations', desktop || mobileOpen);
  const conversations = data?.conversations || [];
  const error = failed ? 'Could not load conversations' : null;
  React.useEffect(() => {
    window.addEventListener('tree:conversation-updated', load);
    return () => window.removeEventListener('tree:conversation-updated', load);
  }, [load]);
  const normalizedQuery = query.trim().toLocaleLowerCase();
  const visible = conversations.filter((item) =>
    `${item.display_title || item.title || ''} ${item.last_message_excerpt || ''}`
      .toLocaleLowerCase()
      .includes(normalizedQuery),
  );
  return (
    <ConversationRailContent
      conversations={conversations}
      visible={visible}
      query={query}
      setQuery={setQuery}
      activeId={activeId}
      load={load}
      error={error}
      mobileOpen={mobileOpen}
      setMobileOpen={setMobileOpen}
    />
  );
}
