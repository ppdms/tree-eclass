import { useEffect, useState } from 'react';
import { decodeImage } from '../app/browser';
import { useViewport } from '../app/viewport';
import { Raster, Text, View } from './primitives';
import { publishImage, releaseImage } from './resources';
import { colors } from './palette';
import type { Length, Rect, UIStyle } from './types';

interface ImageState {
  ready: boolean;
  width: number;
  height: number;
  error: string;
}
interface Entry {
  id: string;
  references: number;
  controller: AbortController;
  state: ImageState;
  listeners: Set<(state: ImageState) => void>;
}
const entries = new Map<string, Entry>();
let sequence = 0;
const loading: ImageState = { ready: false, width: 0, height: 0, error: '' };

function acquire(key: string, src: string, width: number, height?: number): Entry {
  const previous = entries.get(key);
  if (previous) {
    previous.references++;
    return previous;
  }
  const entry: Entry = {
    id: `native-image-${++sequence}`,
    references: 1,
    controller: new AbortController(),
    state: loading,
    listeners: new Set(),
  };
  entries.set(key, entry);
  void decodeImage(src, { width, height, signal: entry.controller.signal })
    .then((bitmap) => {
      if (!entry.references) {
        bitmap.close();
        return;
      }
      const dimensions = { width: bitmap.width, height: bitmap.height };
      publishImage(entry.id, bitmap);
      entry.state = { ready: true, ...dimensions, error: '' };
    })
    .catch((error) => {
      if (entry.references) entry.state = { ...loading, error: error instanceof Error ? error.message : String(error) };
    })
    .finally(() => {
      for (const listener of entry.listeners) listener(entry.state);
    });
  return entry;
}

function release(key: string, entry: Entry): void {
  if (--entry.references) return;
  entries.delete(key);
  entry.controller.abort();
  releaseImage(entry.id);
}

function useImage(src: string, width: number, height?: number) {
  const [value, setValue] = useState<{ key: string; id: string; state: ImageState } | null>(null);
  const key = JSON.stringify([src, width, height]);
  useEffect(() => {
    if (width <= 0) return;
    const entry = acquire(key, src, width, height);
    const update = (state: ImageState) => setValue({ key, id: entry.id, state });
    entry.listeners.add(update);
    update(entry.state);
    return () => {
      entry.listeners.delete(update);
      release(key, entry);
    };
  }, [key, src, width, height]);
  return value?.key === key ? value : null;
}

export interface SourceImageProps {
  id: string;
  src: string;
  alt?: string;
  style?: UIStyle;
}

function isPixelLength(length: Length | undefined): length is number {
  return typeof length === 'number';
}

export function SourceImage({ id, src, alt = '', style = {} }: SourceImageProps) {
  const { dpr } = useViewport();
  const [width, setWidth] = useState(isPixelLength(style.width) ? style.width : 0);
  const fixedHeight = isPixelLength(style.height) ? style.height : undefined;
  const image = useImage(
    src,
    Math.ceil(width * dpr),
    fixedHeight === undefined ? undefined : Math.ceil(fixedHeight * dpr),
  );
  const state = image?.state ?? loading;
  const ratio = state.ready ? state.width / state.height : 1;
  const maxHeight = isPixelLength(style.maxHeight) ? style.maxHeight : Infinity;
  const height = fixedHeight ?? (state.ready ? Math.min(width / ratio, maxHeight) : 36);
  const measured = (rect: Rect) => setWidth((previous) => (previous === rect.width ? previous : rect.width));
  return (
    <View
      id={id}
      onLayout={measured}
      pending={!state.ready && !state.error}
      style={{ minWidth: 0, ...style, height, overflow: 'hidden' }}
    >
      {state.ready && image ? (
        <Raster
          source={image.id}
          style={{ width: Math.min(width, height * ratio), height, marginLeft: 'auto', marginRight: 'auto' }}
        />
      ) : (
        <Text style={{ fontSize: 12, color: state.error ? colors.danger : colors.muted }}>
          {alt || (state.error ? 'Image unavailable' : 'Loading image…')}
        </Text>
      )}
    </View>
  );
}
