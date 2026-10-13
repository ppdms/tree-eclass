import { useState } from 'react';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Field } from '../ui/forms';
import { ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { useConversationHistory } from './history';
import { ConversationItem } from './item';
import { filterConversations, type Conversation } from './model';
import { requestNewQuestion } from './navigation';
import { styles } from './styles';

interface ConversationListProps {
  items: Conversation[];
  query: string;
  activeId: string | number | null;
  onChanged: () => void;
}

function ConversationList({ items, query, activeId, onChanged }: ConversationListProps) {
  const visible = filterConversations(items, query);
  if (visible.length)
    return (
      <View style={styles.column}>
        {visible.map((item) => (
          <ConversationItem
            key={item.id}
            item={item}
            active={String(item.id) === String(activeId)}
            onChanged={onChanged}
          />
        ))}
      </View>
    );
  return (
    <View style={styles.column}>
      <Text style={styles.muted}>
        {query.trim() ? 'No conversations match that search.' : 'No saved questions yet.'}
      </Text>
      {!query.trim() && (
        <AppLink id="ask-history-empty-new" href="/ask" style={styles.link} onClick={requestNewQuestion}>
          Ask one to keep it here.
        </AppLink>
      )}
    </View>
  );
}

function ConversationSearch({
  count,
  query,
  onChange,
}: {
  count: number;
  query: string;
  onChange: (value: string) => void;
}) {
  return (
    <>
      <View style={{ ...styles.row, justifyContent: 'space-between' }}>
        <Text style={{ ...styles.text, fontWeight: 650 }}>Conversations</Text>
        <Text style={styles.small}>{count}</Text>
      </View>
      <Field
        id="ask-history-search"
        label="Search conversations"
        type="search"
        value={query}
        onChange={onChange}
        placeholder="Search conversations"
      />
    </>
  );
}

function ConversationHistory({
  history,
  query,
  activeId,
}: {
  history: ReturnType<typeof useConversationHistory>;
  query: string;
  activeId: string | number | null;
}) {
  if (history.failed) {
    return (
      <Notice id="ask-history-error" tone="danger">
        <Text style={styles.muted}>Conversation history is unavailable.</Text>
        <Button id="ask-history-retry" onClick={history.retry}>
          Retry
        </Button>
      </Notice>
    );
  }
  return history.pending ? (
    <Text style={styles.muted}>Opening conversations…</Text>
  ) : (
    <ConversationList items={history.items} query={query} activeId={activeId} onChanged={history.retry} />
  );
}

export function ConversationRail({
  activeId,
  refresh,
  desktop,
  height,
}: {
  activeId: string | number | null;
  refresh: number;
  desktop: boolean;
  height: number;
}) {
  const [query, setQuery] = useState('');
  const [mobileOpen, setMobileOpen] = useState(false);
  const enabled = desktop || mobileOpen;
  const history = useConversationHistory(enabled, refresh);
  return (
    <View style={desktop ? styles.rail : styles.railMobile}>
      {!desktop && (
        <Button
          id="ask-history-toggle"
          expanded={mobileOpen}
          controls="ask-conversation-sidebar"
          onClick={() => setMobileOpen((open) => !open)}
        >
          {mobileOpen ? 'Hide conversations' : 'Show conversations'}
        </Button>
      )}
      <View
        id="ask-conversation-sidebar"
        style={{ ...styles.column, display: enabled ? 'flex' : 'none' }}
        label="Ask conversations"
      >
        <AppLink id="ask-history-new" href="/ask" style={styles.link} onClick={requestNewQuestion}>
          New question
        </AppLink>
        <ConversationSearch count={history.items.length} query={query} onChange={setQuery} />
        <ScrollArea id="ask-history-list" height={desktop ? height : Math.min(height, 360)}>
          <ConversationHistory history={history} query={query} activeId={activeId} />
        </ScrollArea>
      </View>
    </View>
  );
}
