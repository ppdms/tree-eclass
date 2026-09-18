import type { UIMessage } from 'ai';

// ── Ask ────────────────────────────────────────────────────────────────────

export interface Conversation {
  id: string | number;
  title?: string;
  display_title?: string;
  updated_at?: string;
  last_message_excerpt?: string;
}

export interface AskBootstrap {
  conversation_id?: string | number;
  initial_messages?: UIMessage[];
  examples?: string[];
  models?: string[];
  default_model?: string;
  provider_available?: boolean;
}

export interface ConversationsPayload {
  conversations?: Conversation[];
}
