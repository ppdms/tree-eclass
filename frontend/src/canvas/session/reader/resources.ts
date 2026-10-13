import { useCallback, useLayoutEffect, useRef, useState } from 'react';
import type { MutableRefObject } from 'react';
import { PageCache } from './cache';
import { friendlyDocumentError, openSource, sourceFormat } from './source';
import type { ReaderSource } from './source';
import type { SessionDocument } from './types';

export interface ReaderResources {
  source: ReaderSource | null;
  cache: PageCache | null;
  revision: number;
  error: string | null;
  retry(): void;
}

export function useReaderResources(document: SessionDocument, requested: MutableRefObject<number>): ReaderResources {
  const [source, setSource] = useState<ReaderSource | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [revision, setRevision] = useState(0);
  const cache = useRef<PageCache | null>(null);
  const initialDocument = useRef(document).current;
  useLayoutEffect(() => {
    const abort = new AbortController();
    setSource(null);
    setError(null);
    if (sourceFormat(initialDocument) === 'unsupported') return undefined;
    void openSource(initialDocument, requested.current, abort.signal).then(
      (loaded) => {
        if (abort.signal.aborted) {
          loaded.destroy();
          return;
        }
        cache.current = new PageCache(loaded, () => setRevision((value) => value + 1));
        setSource(loaded);
      },
      (failure) => {
        if (!abort.signal.aborted) setError(friendlyDocumentError(failure));
      },
    );
    return () => {
      abort.abort();
      cache.current?.destroy();
      cache.current = null;
    };
  }, [attempt, initialDocument, requested]);
  const retry = useCallback(() => setAttempt((value) => value + 1), []);
  return { source, cache: cache.current, revision, error, retry };
}
