import { Chat, useChat } from '@ai-sdk/react';
import { DefaultChatTransport, isTextUIPart, type UIMessage } from 'ai';
import type { JsonValue } from '@/lib/display';
import { useCallback, useEffect, useRef, useState, type MutableRefObject } from 'react';
import { setPreference, usePreference } from '../app/preferences';
import { conversationData, type AskBootstrap, type Conversation } from './model';

const emptyModels: string[] = [];

interface Attempt {
  active: boolean;
  committed: boolean;
}

interface ChatContext {
  conversation: MutableRefObject<string | number | null>;
  model: MutableRefObject<string>;
  mounted: MutableRefObject<boolean>;
  attempt: MutableRefObject<Attempt>;
  onConversation: MutableRefObject<(value: Conversation) => void>;
}

function createAskChat(messages: UIMessage[], context: ChatContext): Chat<UIMessage> {
  return new Chat<UIMessage>({
    messages,
    transport: new DefaultChatTransport({
      api: '/api/ask/stream',
      credentials: 'same-origin',
      // configureAPI guards this SDK fetch with the original captured page runtime.
      prepareSendMessagesRequest({ messages: outgoing }) {
        const last = outgoing[outgoing.length - 1];
        const question = (last?.parts || [])
          .filter(isTextUIPart)
          .map((part) => part.text)
          .join('');
        return { body: { question, conversation_id: context.conversation.current, model: context.model.current } };
      },
    }),
    onData(part) {
      if (part.type !== 'data-conversation' || !context.mounted.current || !context.attempt.current.active) return;
      const saved = conversationData(part);
      if (!saved) return;
      // The server sends this only after SaveTurn has committed the complete question/answer pair.
      context.conversation.current = saved.id;
      context.attempt.current.committed = true;
      context.onConversation.current(saved);
    },
  });
}

function useAskModel(models: string[], defaultModel?: string) {
  const preferred = models.includes(defaultModel || '') ? defaultModel || '' : models[0] || '';
  const parseModel = useCallback((value: JsonValue) => (isModelName(value) ? value : preferred), [preferred]);
  const [saved] = usePreference('ask:model', preferred, parseModel);
  const model = models.includes(saved) ? saved : preferred;
  const modelRef = useRef(model);
  modelRef.current = model;
  useEffect(() => {
    if (model && saved !== model) setPreference('ask:model', model);
  }, [model, saved]);
  const setModel = useCallback(
    (value: string) => {
      if (!models.includes(value)) return;
      modelRef.current = value;
      setPreference('ask:model', value);
    },
    [models],
  );
  return { model, modelRef, setModel };
}

function isModelName(value: JsonValue): value is string {
  return typeof value === 'string';
}

export function useAskController(bootstrap: AskBootstrap, onSaved: (value: Conversation) => void) {
  const { model, modelRef, setModel } = useAskModel(bootstrap.models || emptyModels, bootstrap.default_model);
  const conversation = useRef(bootstrap.conversation_id ?? null);
  const mounted = useRef(true);
  const attempt = useRef<Attempt>({ active: false, committed: false });
  const onConversation = useRef(onSaved);
  onConversation.current = onSaved;
  const [chat] = useState(() =>
    createAskChat(bootstrap.initial_messages || [], {
      conversation,
      model: modelRef,
      mounted,
      attempt,
      onConversation,
    }),
  );
  const state = useChat({ chat });
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      attempt.current.active = false;
      void chat.stop();
    };
  }, [chat]);
  const run = useCallback(
    async (question?: string): Promise<boolean> => {
      if (attempt.current.active || chat.status === 'submitted' || chat.status === 'streaming') return false;
      const turn = { active: true, committed: false };
      attempt.current = turn;
      try {
        if (question === undefined) await chat.regenerate();
        else await chat.sendMessage({ text: question });
        return mounted.current && turn.committed;
      } finally {
        turn.active = false;
      }
    },
    [chat],
  );
  const stop = useCallback(() => {
    attempt.current.active = false;
    void chat.stop();
  }, [chat]);
  const retry = useCallback(() => run(), [run]);
  return { ...state, model, setModel, send: run, retry, stop };
}
