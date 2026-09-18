import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { CourseFilesPayload } from '@/lib/types';

type Knowledge = Pick<CourseFilesPayload, 'file_insights'>;

export function useCourseFiles(courseId: string | number, request: number) {
  const [files, setFiles] = React.useState<CourseFilesPayload | null>(null);
  const [knowledge, setKnowledge] = React.useState<Knowledge | null>(null);
  const [error, setError] = React.useState(false);
  const [knowledgeError, setKnowledgeError] = React.useState(false);
  React.useEffect(() => {
    const controller = new AbortController();
    const options = { signal: controller.signal };
    const base = `/api/v1/courses/${encodeURIComponent(courseId)}`;
    setError(false);
    setKnowledgeError(false);
    setFiles(null);
    setKnowledge(null);
    fetchJson<CourseFilesPayload>(`${base}/files?include_knowledge=false`, options)
      .then((value) => {
        if (!controller.signal.aborted) setFiles(value);
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });
    fetchJson<Knowledge>(`${base}/file-metadata`, options)
      .then((value) => {
        if (!controller.signal.aborted) setKnowledge(value);
      })
      .catch(() => {
        if (!controller.signal.aborted) setKnowledgeError(true);
      });
    return () => controller.abort();
  }, [courseId, request]);
  return {
    files: files ? { ...files, ...knowledge } : null,
    error,
    knowledgeError,
    knowledgeLoading: !knowledge && !knowledgeError,
  };
}
