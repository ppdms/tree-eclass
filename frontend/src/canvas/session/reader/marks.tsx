import { useRef } from 'react';
import { View, Text, Vector } from '../../ui/primitives';
import type { CanvasNodePointer } from '../../ui/types';
import type { Annotation, AnnotationRect, ReaderSelection } from './types';
import { annotationAnchorId } from './types';

const markerColors = {
  yellow: '#d97706',
  green: '#65a30d',
  blue: '#2563eb',
  pink: '#db2777',
};
const highlightColors = {
  yellow: 'rgba(250,204,21,0.35)',
  green: 'rgba(163,230,53,0.35)',
  blue: 'rgba(147,197,253,0.35)',
  pink: 'rgba(251,113,133,0.35)',
};
function isMarkerColor(color: string): color is keyof typeof markerColors {
  return Object.hasOwn(markerColors, color);
}
interface MarkProps {
  annotation: Annotation;
  width: number;
  height: number;
  activate(annotation: Annotation): void;
}
interface MarkTrigger {
  id: string;
  label: string;
  onClick(): void;
  onPointer(event: CanvasNodePointer): boolean | void;
}

function rectStyle(rect: AnnotationRect, width: number, height: number) {
  return {
    position: 'absolute' as const,
    left: rect.x * width,
    top: rect.y * height,
    width: rect.w * width,
    height: rect.h * height,
  };
}

function useMarkPointer(
  annotation: Annotation,
  width: number,
  height: number,
  activate: (annotation: Annotation) => void,
) {
  const pressed = useRef<{ id: number; x: number; y: number } | null>(null);
  return (event: CanvasNodePointer) => {
    if (event.phase === 'down' && !event.touch && !event.modified && event.button === 0) {
      pressed.current = { id: event.id, x: event.x, y: event.y };
      return true;
    }
    if (event.phase === 'cancel') pressed.current = null;
    const point = pressed.current;
    if (event.phase !== 'up' || !point || point.id !== event.id) return;
    pressed.current = null;
    if (
      Math.hypot(event.x - point.x, event.y - point.y) < 8 &&
      event.localX >= 0 &&
      event.localY >= 0 &&
      event.localX <= width &&
      event.localY <= height
    )
      activate(annotation);
  };
}

function AnnotationMarker({
  annotation,
  width,
  height,
  trigger,
}: Omit<MarkProps, 'activate'> & { trigger: MarkTrigger }) {
  const first = annotation.rects?.[0];
  const markerLeft = first
    ? first.x + first.w > 0.72
      ? first.x * width - 32
      : (first.x + first.w) * width + 4
    : width * 0.025;
  const colorName = annotation.color ?? 'yellow';
  return (
    <View
      {...trigger}
      style={{
        position: 'absolute',
        left: annotation.kind === 'bookmark' ? width - 28 : Math.max(0, Math.min(width - 28, markerLeft)),
        top: annotation.kind === 'bookmark' ? 0 : (first?.y ?? 0.025) * height,
        width: 28,
        height: 28,
        backgroundColor:
          annotation.status === 'orphaned' ? '#71717a' : isMarkerColor(colorName) ? markerColors[colorName] : undefined,
        borderRadius: annotation.kind === 'bookmark' ? 0 : 5,
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {annotation.kind === 'bookmark' ? (
        <Vector path="M4 0H24V28L14 21L4 28Z" fill="#dc2626" style={{ width: 28, height: 28 }} />
      ) : (
        <Text style={{ color: '#ffffff', fontSize: 18 }}>{annotation.kind === 'question' ? '?' : 'N'}</Text>
      )}
    </View>
  );
}

function Mark({ annotation, width, height, activate }: MarkProps) {
  const colorName = annotation.color ?? 'yellow';
  const color =
    annotation.status === 'orphaned'
      ? '#71717a55'
      : isMarkerColor(colorName)
        ? highlightColors[colorName]
        : highlightColors.yellow;
  const first = annotation.rects?.[0];
  const marker = annotation.kind !== 'highlight';
  const onPointer = useMarkPointer(
    annotation,
    marker ? 28 : (first?.w ?? 0) * width,
    marker ? 28 : (first?.h ?? 0) * height,
    activate,
  );
  const trigger = {
    id: annotationAnchorId(annotation),
    label: annotation.body || annotation.quote || annotation.kind,
    onClick: () => activate(annotation),
    onPointer,
  };
  return (
    <>
      {(annotation.rects ?? []).map((rect, index) => (
        <View
          key={`${rect.x}:${rect.y}:${rect.w}:${rect.h}:${index}`}
          {...(!marker && index === 0 ? trigger : {})}
          style={{
            ...rectStyle(rect, width, height),
            backgroundColor: color,
            borderBottomWidth: annotation.kind === 'note' ? 2 : 0,
            borderColor: '#d97706',
          }}
        />
      ))}
      {marker ? <AnnotationMarker annotation={annotation} width={width} height={height} trigger={trigger} /> : null}
    </>
  );
}

export function PageMarks({
  annotations,
  width,
  height,
  activate,
}: {
  annotations: Annotation[];
  width: number;
  height: number;
  activate(annotation: Annotation): void;
}) {
  return annotations
    .filter((annotation) => annotation.status !== 'deleted')
    .map((annotation) => (
      <Mark key={String(annotation.id)} annotation={annotation} width={width} height={height} activate={activate} />
    ));
}

export function SelectionPaint({
  selection,
  width,
  height,
}: {
  selection: ReaderSelection | null;
  width: number;
  height: number;
}) {
  return (
    selection?.rects.map((rect, index) => (
      <View
        key={`${rect.x}:${rect.y}:${index}`}
        style={{ ...rectStyle(rect, width, height), backgroundColor: 'rgba(59,130,246,0.32)' }}
      />
    )) ?? null
  );
}
