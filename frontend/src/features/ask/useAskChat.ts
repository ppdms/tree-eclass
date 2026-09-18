import { storageKey } from '@/lib/browserStorage';
import * as React from 'react';
import { useChat, type UseChatHelpers } from '@ai-sdk/react';
import { DefaultChatTransport, type UIMessage } from 'ai';
import { z } from 'zod/v4';
import { isServer, type JsonValue } from '@/lib/display';
import type { Conversation } from '@/features/ask/types';

const conversationSchema = z
  .object({
    id: z.union([z.string(), z.number()]),
    title: z.string().optional(),
    display_title: z.string().optional(),
    updated_at: z.string().optional(),
    last_message_excerpt: z.string().optional(),
  })
  .partial();

function asConversation(data: JsonValue): Conversation | null {
  const parsed = conversationSchema.safeParse(data);
  if (!parsed.success || parsed.data.id === undefined) return null;
  return {
    id: parsed.data.id,
    title: parsed.data.title,
    display_title: parsed.data.display_title,
    updated_at: parsed.data.updated_at,
    last_message_excerpt: parsed.data.last_message_excerpt,
  };
}

function asConversationData(part: { type: string; data: unknown }): Conversation | null {
  if (part.type !== 'data-conversation') return null;
  // SAFETY: the SDK's data part payload is a JSON value by construction
  // (it round-trips through the wire); the cast only narrows the
  // representation before schema parsing.
  return asConversation(part.data as JsonValue);
}

/**
 * Wire the AI SDK chat to the server-owned thread.
 *
 * The transport and callback configuration is the same one the page always
 * used; it lives here so Ask itself stays small.
 */
export function useAskChat({
  root,
  initialMessages,
  conversation,
  modelRef,
  onConversation,
}: {
  root: React.RefObject<HTMLElement | null>;
  initialMessages: UIMessage[];
  conversation: React.MutableRefObject<string | number | null | undefined>;
  modelRef: React.MutableRefObject<string>;
  onConversation?: (conversation: Conversation) => void;
}): UseChatHelpers<UIMessage> {
  return useChat({
    messages: initialMessages,
    transport: new DefaultChatTransport({
      api: '/api/ask/stream',
      // The server owns the thread, so only the new question goes over
      // the wire; prior turns are read from the stored conversation.
      prepareSendMessagesRequest({ messages }) {
        const last = messages[messages.length - 1];
        const question = (last?.parts || [])
          .filter((part) => part.type === 'text')
          .map((part) => part.text)
          .join('');
        return {
          body: {
            question,
            conversation_id: conversation.current,
            // The model picker's choice rides along; the server
            // ignores values outside its configured list.
            model: modelRef.current,
          },
        };
      },
    }),
    // The thread id is minted server-side once an answer exists. The page
    // around this island owns the address bar and the conversation list,
    // so it is told rather than reaching in here.
    onData(part) {
      const conversationData = asConversationData(part);
      if (!conversationData) return;
      conversation.current = conversationData.id;
      onConversation?.(conversationData);
      const target = root?.current || document.getElementById('ask-root');
      target?.dispatchEvent(new CustomEvent('ask:conversation', { detail: conversationData }));
    },
  });
}

const MODEL_STORAGE_KEY = 'ask:model';

export function useAskModel(models: string[], defaultModel?: string) {
  const preferred = models.includes(defaultModel || '') ? defaultModel || '' : models[0] || '';
  const [model, setModel] = React.useState<string>(preferred);
  React.useEffect(() => {
    if (!models.length) {
      setModel('');
      return;
    }
    const saved = window.localStorage.getItem(storageKey(MODEL_STORAGE_KEY)) ?? '';
    setModel(models.includes(saved) ? saved : preferred);
  }, [models, preferred]);
  const modelRef = React.useRef(model);
  React.useEffect(() => {
    modelRef.current = model;
    if (model) window.localStorage.setItem(storageKey(MODEL_STORAGE_KEY), model);
  }, [model]);
  return { model, setModel, modelRef };
}

export function useAskFocus(root: React.RefObject<HTMLElement | null>, hasMessages: boolean): void {
  React.useEffect(() => {
    if (isServer()) return;
    const wantsFocus = root?.current?.dataset.focusOnLoad || new URLSearchParams(window.location.search).has('focus');
    if (!wantsFocus || hasMessages) return;
    const focus = () => {
      const el = document.getElementById('ask-question');
      if (el) el.focus();
    };
    const raf = requestAnimationFrame(() => {
      focus();
      // Hydration may not have committed the input yet; retry once after the frame.
      setTimeout(focus, 50);
    });
    return () => cancelAnimationFrame(raf);
  }, [root, hasMessages]);
}
