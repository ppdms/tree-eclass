import { useCallback, useEffect, useRef, useState } from 'react';
import { capturePointer, focusNode, releasePointer } from '../app/browser';
import { colors } from './palette';
import { Text, View } from './primitives';
import { clampRange, rangeBounds, rangeFraction, type RangeBounds } from './rangeMath';
import type { CanvasKey, CanvasNodePointer, UIStyle } from './types';

export interface RangeProps {
  id: string;
  label?: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (value: number) => void;
  disabled?: boolean;
  style?: UIStyle;
}

function keyValue(event: CanvasKey, value: number, bounds: RangeBounds): number | undefined {
  if (event.composing || event.alt || event.meta || event.ctrl) return undefined;
  if (event.key === 'ArrowLeft' || event.key === 'ArrowDown') return value - bounds.step;
  if (event.key === 'ArrowRight' || event.key === 'ArrowUp') return value + bounds.step;
  if (event.key === 'Home') return bounds.min;
  if (event.key === 'End') return bounds.max;
  return undefined;
}

function useRangeInteraction(props: RangeProps, bounds: RangeBounds, width: number) {
  const pointer = useRef<number | null>(null);
  const current = useRef(clampRange(props.value, bounds));
  current.current = clampRange(props.value, bounds);
  const release = useCallback(() => {
    if (pointer.current !== null) releasePointer(pointer.current);
    pointer.current = null;
  }, []);
  useEffect(() => release, [release]);
  useEffect(() => {
    if (props.disabled) release();
  }, [props.disabled, release]);
  const change = (next: number) => {
    const value = clampRange(next, bounds);
    if (value !== current.current) {
      current.current = value;
      props.onChange(value);
    }
  };
  const onPointer = (event: CanvasNodePointer) => {
    if (props.disabled) return;
    if (event.phase === 'down') {
      if (pointer.current !== null || (!event.touch && event.button !== 0)) return;
      pointer.current = event.id;
      capturePointer(props.id, event.id);
      focusNode(props.id);
    } else if (pointer.current !== event.id) return;
    if (event.phase === 'cancel') {
      release();
      return true;
    }
    if (event.phase !== 'hover') {
      const fraction = Math.max(0, Math.min(1, (event.localX - 12) / Math.max(1, width - 24)));
      change(bounds.min + fraction * (bounds.max - bounds.min));
    }
    if (event.phase === 'up') release();
    return true;
  };
  const onKeyInput = (event: CanvasKey) => {
    if (props.disabled) return;
    const next = keyValue(event, current.current, bounds);
    if (next === undefined) return;
    change(next);
    return true;
  };
  return { onPointer, onKeyInput };
}

function RangeTrack({ fraction, width }: { fraction: number; width: number }) {
  return (
    <>
      <View
        style={{
          position: 'absolute',
          left: 12,
          right: 12,
          top: 19,
          height: 6,
          borderRadius: 3,
          backgroundColor: colors.border,
          overflow: 'hidden',
        }}
      >
        <View style={{ height: 6, width: `${fraction * 100}%`, backgroundColor: colors.accent }} />
      </View>
      <View
        style={{
          position: 'absolute',
          left: 12 + Math.max(0, width - 24) * fraction - 9,
          top: 13,
          width: 18,
          height: 18,
          borderRadius: 9,
          borderWidth: 2,
          borderColor: colors.accent,
          backgroundColor: colors.surface,
        }}
      />
    </>
  );
}

export function Range(props: RangeProps) {
  const { id, label, value, min, max, step, disabled = false, style } = props;
  const [width, setWidth] = useState(0);
  const bounds = rangeBounds(min, max, step);
  const selected = clampRange(value, bounds);
  const fraction = rangeFraction(selected, bounds);
  const interaction = useRangeInteraction(props, bounds, width);
  const targetStyle: UIStyle = {
    width: '100%',
    minWidth: 44,
    height: 44,
    position: 'relative',
    borderRadius: 12,
    hoverBackground: colors.hover,
  };
  if (disabled) targetStyle.opacity = 0.5;
  return (
    <View id={`${id}-group`} style={{ minWidth: 0, gap: 4, ...style }}>
      {label && (
        <View style={{ flexDirection: 'row', justifyContent: 'space-between', gap: 8 }}>
          <Text style={{ fontSize: 13, fontWeight: 600, color: colors.text }}>{label}</Text>
          <Text style={{ fontSize: 13, color: colors.muted }}>{String(selected)}</Text>
        </View>
      )}
      <View
        id={id}
        label={label}
        disabled={disabled}
        focusable={!disabled}
        {...interaction}
        onLayout={(rect) => setWidth((previous) => (previous === rect.width ? previous : rect.width))}
        style={targetStyle}
      >
        <RangeTrack fraction={fraction} width={width} />
      </View>
    </View>
  );
}
