import * as React from 'react';
import type { CourseFilesPayload } from '@/lib/types';
import { requestJson } from '../app/api';

type Knowledge = Pick<CourseFilesPayload, 'file_insights'>;
interface Resource<T> {
  key: string;
  value: T | null;
  error: boolean;
  pending: boolean;
}

function useFileTree(courseId: string | number, key: string) {
  const [state, setFiles] = React.useState<Resource<CourseFilesPayload> | null>(null);
  React.useEffect(() => {
    const controller = new AbortController();
    setFiles({ key, value: null, error: false, pending: true });
    const url = `/api/v1/courses/${encodeURIComponent(String(courseId))}/files?include_knowledge=false`;
    void requestJson<CourseFilesPayload>(url, { signal: controller.signal }).then(
      (value) => {
        if (!controller.signal.aborted) setFiles({ key, value, error: false, pending: false });
      },
      () => {
        if (!controller.signal.aborted) setFiles({ key, value: null, error: true, pending: false });
      },
    );
    return () => controller.abort();
  }, [courseId, key]);
  return state?.key === key ? state : null;
}

function useMetadata(courseId: string | number, key: string) {
  const [state, setKnowledge] = React.useState<Resource<Knowledge> | null>(null);
  const [metadataAttempt, refreshMetadata] = React.useReducer((value: number) => value + 1, 0);
  React.useEffect(() => {
    const controller = new AbortController();
    setKnowledge((previous) => ({
      key,
      value: previous?.key === key ? previous.value : null,
      error: false,
      pending: true,
    }));
    const url = `/api/v1/courses/${encodeURIComponent(String(courseId))}/file-metadata`;
    void requestJson<Knowledge>(url, { signal: controller.signal }).then(
      (value) => {
        if (!controller.signal.aborted) setKnowledge({ key, value, error: false, pending: false });
      },
      () => {
        if (!controller.signal.aborted) {
          setKnowledge((previous) => ({
            key,
            value: previous?.key === key ? previous.value : null,
            error: true,
            pending: false,
          }));
        }
      },
    );
    return () => controller.abort();
  }, [courseId, key, metadataAttempt]);
  return { state: state?.key === key ? state : null, refreshMetadata };
}

export function useCourseFiles(courseId: string | number, attempt: number) {
  const key = JSON.stringify([courseId, attempt]);
  const fileState = useFileTree(courseId, key);
  const { state: knowledgeState, refreshMetadata } = useMetadata(courseId, key);
  const files = fileState?.value ?? null;
  const knowledge = knowledgeState?.value ?? null;
  const current = React.useMemo(() => (files ? { ...files, ...knowledge } : null), [files, knowledge]);
  return {
    files: current,
    error: fileState?.error ?? false,
    knowledgeError: knowledgeState?.error ?? false,
    knowledgeLoading: !knowledgeState || knowledgeState.pending,
    refreshMetadata,
  };
}
