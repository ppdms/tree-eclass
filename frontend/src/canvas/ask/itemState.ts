import { useEffect, useRef, useState } from 'react';
import { requestJson } from '../app/api';
import { focusField } from '../app/browser';
import { displayTitle, friendlyError, type Conversation } from './model';

export interface ConversationItemState {
  editing: boolean;
  title: string;
  setTitle: (value: string) => void;
  confirmOpen: boolean;
  setConfirmOpen: (value: boolean) => void;
  error: string | null;
  pending: 'rename' | 'delete' | null;
  mutate: (kind: 'rename' | 'delete') => Promise<void>;
  cancel: () => void;
  rename: () => void;
}

function useConversationMutation(item: Conversation, onChanged: () => void, onComplete: () => void) {
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<'rename' | 'delete' | null>(null);
  const active = useRef<AbortController | null>(null);
  useEffect(() => () => active.current?.abort(), []);
  const mutate = async (kind: 'rename' | 'delete', title: string) => {
    if (active.current || (kind === 'rename' && !title.trim())) return;
    const controller = new AbortController();
    active.current = controller;
    setPending(kind);
    setError(null);
    try {
      const options: RequestInit = {
        method: kind === 'rename' ? 'PATCH' : 'DELETE',
        signal: controller.signal,
      };
      if (kind === 'rename') {
        options.headers = { 'Content-Type': 'application/json' };
        options.body = JSON.stringify({ title });
      }
      await requestJson(`/api/ask/conversations/${encodeURIComponent(String(item.id))}`, options);
      if (controller.signal.aborted) return;
      onComplete();
      onChanged();
    } catch (failure) {
      if (!controller.signal.aborted) setError(`Could not ${kind} this conversation. ${friendlyError(failure)}`);
    } finally {
      if (!controller.signal.aborted) setPending(null);
      if (active.current === controller) active.current = null;
    }
  };
  return { error, pending, active, mutate, clearError: () => setError(null) };
}

export function useConversationItem(item: Conversation, onChanged: () => void): ConversationItemState {
  const [editing, setEditing] = useState(false);
  const [title, setTitle] = useState(displayTitle(item));
  const [confirmOpen, setConfirmOpen] = useState(false);
  const mutation = useConversationMutation(item, onChanged, () => setEditing(false));
  useEffect(() => {
    if (editing) focusField(`ask-conversation-${item.id}-title`);
  }, [editing, item.id]);
  const mutate = async (kind: 'rename' | 'delete') => {
    if (mutation.active.current || (kind === 'rename' && !title.trim())) return;
    if (kind === 'delete') setConfirmOpen(false);
    await mutation.mutate(kind, title);
  };
  const cancel = () => {
    if (mutation.pending) return;
    setTitle(displayTitle(item));
    setEditing(false);
    mutation.clearError();
  };
  return {
    editing,
    title,
    setTitle,
    confirmOpen,
    setConfirmOpen,
    error: mutation.error,
    pending: mutation.pending,
    mutate,
    cancel,
    rename: () => {
      if (!mutation.pending) {
        setTitle(displayTitle(item));
        setEditing(true);
        mutation.clearError();
      }
    },
  };
}
