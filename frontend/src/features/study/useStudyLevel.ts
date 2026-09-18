import * as React from 'react';

export interface UseStudyLevelResult {
  level: number;
  busy: boolean;
  update: (next: number) => Promise<void>;
}

export function useStudyLevel(
  courseId: string | number,
  filePath: string | undefined,
  initial?: number,
  onChanged?: (level: number) => void,
): UseStudyLevelResult {
  const [level, setLevel] = React.useState<number>(initial || 0);
  const [busy, setBusy] = React.useState(false);
  const update = async (next: number) => {
    if (busy) return;
    const previous = level;
    setBusy(true);
    setLevel(next);
    try {
      const response = await fetch(`/api/courses/${courseId}/files/study-level`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ file_path: filePath, level: next }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || 'Could not save study level.');
      }
      onChanged?.(next);
    } catch {
      setLevel(previous);
    } finally {
      setBusy(false);
    }
  };
  return { level, busy, update };
}
