import { useEffect, useState } from 'react';
import { z } from 'zod/v4';
import type { JsonValue } from '@/lib/display';
import { usePreference } from '../../app/preferences';
import type { PageSize } from '../../pdf';
import { fitZoom } from './layout';
import type { ReaderSource } from './source';
import type { ReaderState } from './types';

interface ReaderPreference {
  mode?: ReaderState['mode'];
  zoom?: number;
}
const readerPreferenceSchema = z
  .looseObject({
    mode: z.enum(['manual', 'width', 'page']).optional(),
    zoom: z.number().optional(),
  })
  .nullable();
function parseReaderPreference(value: JsonValue): ReaderPreference | null {
  const parsed = readerPreferenceSchema.safeParse(value);
  return parsed.success ? parsed.data : null;
}
export interface ReaderScale {
  mode: ReaderState['mode'];
  zoom: number;
  fitWidth(): void;
  fitPage(): void;
  zoomIn(): void;
  zoomOut(): void;
}

function useNaturalSize(source: ReaderSource | null, ready: boolean, manual: boolean): PageSize | null {
  const [natural, setNatural] = useState<PageSize | null>(null);
  useEffect(() => setNatural(null), [source]);
  useEffect(() => {
    if (!source || !ready || manual) return undefined;
    let live = true;
    void source.size(1).then(
      (size) => {
        if (live) setNatural(size);
      },
      () => {},
    );
    return () => {
      live = false;
    };
  }, [source, ready, manual]);
  return natural;
}

export function useReaderScale(
  documentId: string,
  source: ReaderSource | null,
  width: number,
  height: number,
  ready: boolean,
): ReaderScale {
  const [preference, setPreference] = usePreference<ReaderPreference | null>(
    `tree-eclass:study:reader:${documentId}`,
    null,
    parseReaderPreference,
  );
  const mode =
    preference?.mode === 'manual' || preference?.mode === 'width' || preference?.mode === 'page'
      ? preference.mode
      : width <= 760
        ? 'width'
        : 'page';
  const manual = mode === 'manual';
  const natural = useNaturalSize(source, ready, manual);
  const size = natural ?? source?.firstSize;
  const zoom =
    mode === 'manual'
      ? Math.max(0.3, Math.min(3, Number.isFinite(preference?.zoom) ? preference!.zoom! : 1))
      : size
        ? fitZoom(mode, width, height, size)
        : 1;
  useEffect(() => {
    if (source && (preference?.mode !== mode || preference.zoom !== zoom)) setPreference({ mode, zoom });
  }, [source, mode, zoom, preference, setPreference]);
  return {
    mode,
    zoom,
    fitWidth: () => setPreference({ mode: 'width', zoom }),
    fitPage: () => setPreference({ mode: 'page', zoom }),
    zoomIn: () => setPreference({ mode: 'manual', zoom: Math.min(3, zoom + 0.15) }),
    zoomOut: () => setPreference({ mode: 'manual', zoom: Math.max(0.3, zoom - 0.15) }),
  };
}
