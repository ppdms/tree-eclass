import { useEffect, useRef, useState } from 'react';
import { api, message, requestKey } from './api';
import type { Context, Mark, MarkCreate, MutationContext, Selection, Sitting, Source } from './types';

export interface MarkActions {
  marks: Mark[];
  loading: boolean;
  readError: string | null;
  error: string | null;
  announcement: string;
  busy: Set<string>;
  retry: () => void;
  dismiss: () => void;
  create: (payload: Omit<MarkCreate, 'idempotency_key'>) => Promise<boolean>;
  update: (mark: Mark, patch: { body?: string; color?: string }) => Promise<boolean>;
  remove: (mark: Mark) => Promise<boolean>;
  bookmark: (page: number, context: MutationContext) => Promise<boolean>;
}
interface MarkSnapshot {
  marks: Mark[];
  loading: boolean;
  error: string | null;
}
export function sourceKey(source: Source): string {
  return JSON.stringify([String(source.document_id), source.source_hash ?? '']);
}
export function mutationContext(course: string, context: Context, source: Source, session: Sitting | null) {
  return {
    course_id: course,
    document_id: String(source.document_id),
    source_hash: source.source_hash ?? '',
    action_id: context.action?.action_id ?? '',
    unit_key: context.unit_key,
    plan_revision: context.plan_revision,
    session_id: session?.id,
  } satisfies MutationContext;
}
export function selectionPayload(selection: Selection, kind: MarkCreate['kind'], color: string, body?: string) {
  return {
    ...selection.context,
    page_number: selection.page,
    kind,
    color,
    body: body?.trim() || null,
    quote: selection.quote,
    prefix: selection.prefix,
    suffix: selection.suffix,
    char_start: selection.char_start,
    char_end: selection.char_end,
    rects: selection.rects.map(({ x, y, w, h }) => ({ x: Number(x), y: Number(y), w: Number(w), h: Number(h) })),
  } satisfies Omit<MarkCreate, 'idempotency_key'>;
}
function useMarkWrites() {
  const [error, setError] = useState<string | null>(null);
  const [announcement, setAnnouncement] = useState('');
  const [busy, setBusy] = useState(new Set<string>());
  const running = useRef(new Set<string>());
  const intents = useRef(new Map<string, string>());
  const [initialController] = useState(() => new AbortController());
  const controller = useRef(initialController);
  useEffect(() => {
    if (controller.current.signal.aborted) controller.current = new AbortController();
    const lifetime = controller.current;
    return () => lifetime.abort();
  }, []);
  const run = async (id: string, operation: () => Promise<void>): Promise<boolean> => {
    if (running.current.has(id) || controller.current.signal.aborted) return false;
    running.current.add(id);
    setBusy(new Set(running.current));
    setError(null);
    try {
      await operation();
      return !controller.current.signal.aborted;
    } catch (failure) {
      if (!controller.current.signal.aborted) setError(`Save could not be confirmed: ${message(failure)}`);
      return false;
    } finally {
      running.current.delete(id);
      if (!controller.current.signal.aborted) setBusy(new Set(running.current));
    }
  };
  return { error, setError, announcement, setAnnouncement, busy, intents, controller, run };
}

export function useMarks(course: string, source: Source | null, context: Context): MarkActions {
  const key = source ? sourceKey(source) : '';
  const [snapshots, setSnapshots] = useState<Record<string, MarkSnapshot>>({});
  const [attempt, setAttempt] = useState(0);
  const { error, setError, announcement, setAnnouncement, busy, intents, controller, run } = useMarkWrites();
  useMarkRead({ course, source, context, key, attempt, setSnapshots });
  const replace = (target: string, change: (marks: Mark[]) => Mark[]) => {
    setSnapshots((all) => ({
      ...all,
      [target]: {
        marks: change(all[target]?.marks ?? []),
        loading: false,
        error: null,
      },
    }));
  };
  const actions = markOperations({
    key,
    source,
    marks: snapshots[key]?.marks ?? [],
    controller: controller.current,
    intents: intents.current,
    run,
    replace,
    announce: setAnnouncement,
  });
  const snapshot = snapshots[key];
  return {
    ...actions,
    marks: snapshot?.marks ?? [],
    loading: snapshot?.loading ?? Boolean(source),
    readError: snapshot?.error ?? null,
    error,
    announcement,
    busy,
    retry: () => setAttempt((value) => value + 1),
    dismiss: () => setError(null),
  };
}
interface ReadArgs {
  course: string;
  source: Source | null;
  context: Context;
  key: string;
  attempt: number;
  setSnapshots: React.Dispatch<React.SetStateAction<Record<string, MarkSnapshot>>>;
}
function useMarkRead({ course, source, context, key, attempt, setSnapshots }: ReadArgs): void {
  const adopted = useRef<Context | null>(null);
  const previous = useRef('');
  useEffect(() => {
    if (!source) return;
    const previousKey = previous.current;
    previous.current = key;
    const snapshotMatches = context.annotations_document_id === String(source.document_id);
    if (adopted.current !== context && !attempt && snapshotMatches) {
      adopted.current = context;
      setSnapshots((all) => ({ ...all, [key]: { marks: context.annotations, loading: false, error: null } }));
      return;
    }
    if (!attempt && snapshotMatches && adopted.current === context && previousKey === key) return;
    const controller = new AbortController();
    setSnapshots((all) => ({ ...all, [key]: { marks: [], loading: true, error: null } }));
    void api
      .annotations(course, String(source.document_id), controller.signal)
      .then((result) => {
        if (!controller.signal.aborted)
          setSnapshots((all) => ({ ...all, [key]: { marks: result.annotations, loading: false, error: null } }));
      })
      .catch((failure) => {
        if (!controller.signal.aborted)
          setSnapshots((all) => ({ ...all, [key]: { marks: [], loading: false, error: message(failure) } }));
      });
    return () => controller.abort();
  }, [course, key, context, attempt]);
}
interface OperationArgs {
  key: string;
  source: Source | null;
  marks: Mark[];
  controller: AbortController;
  intents: Map<string, string>;
  run: (id: string, operation: () => Promise<void>) => Promise<boolean>;
  replace: (key: string, change: (marks: Mark[]) => Mark[]) => void;
  announce: (message: string) => void;
}
function markOperations(args: OperationArgs) {
  const { key, source, controller, intents, run, replace, announce } = args;
  const create = (payload: Omit<MarkCreate, 'idempotency_key'>) => {
    const signature = JSON.stringify(payload);
    const intent = intents.get(signature) ?? requestKey('ann');
    intents.set(signature, intent);
    const target = JSON.stringify([payload.document_id, payload.source_hash]);
    return run(`create:${target}`, async () => {
      if (!payload.source_hash) throw new Error('The original source identity is missing. Reopen this document.');
      const result = await api.create({ ...payload, idempotency_key: intent }, controller.signal);
      if (controller.signal.aborted) return;
      replace(target, (marks) => [
        ...marks.filter((mark) => String(mark.id) !== String(result.annotation.id)),
        result.annotation,
      ]);
      intents.delete(signature);
      announce(`${result.annotation.kind} saved.`);
    });
  };
  const remove = (mark: Mark) =>
    run(`mark:${mark.id}`, async () => {
      await api.delete(mark.id, controller.signal);
      if (controller.signal.aborted) return;
      replace(key, (marks) => marks.filter((item) => String(item.id) !== String(mark.id)));
      announce('Annotation deleted.');
    });
  const update = (mark: Mark, patch: { body?: string; color?: string }) =>
    run(`mark:${mark.id}`, async () => {
      const result = await api.update(mark.id, patch, controller.signal);
      if (controller.signal.aborted) return;
      replace(key, (marks) => marks.map((item) => (String(item.id) === String(mark.id) ? result.annotation : item)));
      announce('Annotation updated.');
    });
  const bookmark = async (page: number, context: MutationContext) => {
    if (!source) return false;
    const existing = args.marks.filter(
      (mark) => mark.kind === 'bookmark' && mark.status !== 'deleted' && mark.page_number === page,
    );
    if (existing.length) {
      for (const mark of existing) if (!(await remove(mark))) return false;
      return true;
    }
    return create({ ...context, page_number: page, kind: 'bookmark', color: 'yellow', quote: '', rects: [] });
  };
  return { create, remove, update, bookmark };
}
