import * as React from 'react';
import { requestJson } from '../app/api';
import { publishCatalogChanged } from '../app/events';
import { Button } from '../ui/controls';
import { Popover } from '../ui/overlays';
import { Text, Vector, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { fileIdentity } from './helpers';

const LEVELS = ['not studied', 'glanced', 'familiar', 'studied', 'mastered', 'ignored'] as const;
const LEVEL_COLORS = [colors.muted, colors.danger, colors.warning, colors.success, colors.success, colors.muted];
const CIRCLE = 'M14 8A6 6 0 1 1 2 8A6 6 0 1 1 14 8Z';
const DOT = 'M8.7 8A.7 .7 0 1 1 7.3 8A.7 .7 0 1 1 8.7 8Z';

export interface StudyLevelProps {
  courseId: string | number;
  filePath?: string;
  initial?: number;
  onChanged?: (level: number) => void;
  id?: string;
}

function useStudyLevel({ courseId, filePath, initial, onChanged }: StudyLevelProps) {
  const [level, setLevel] = React.useState(initial || 0);
  const [busy, setBusy] = React.useState(false);
  const busyRef = React.useRef(false);
  const update = async (next: number) => {
    if (busyRef.current) return;
    const previous = level;
    busyRef.current = true;
    setBusy(true);
    setLevel(next);
    let saved = false;
    try {
      await requestJson(`/api/courses/${encodeURIComponent(String(courseId))}/files/study-level`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_path: filePath, level: next }),
      });
      saved = true;
    } catch {
      setLevel(previous);
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
    if (saved) {
      publishCatalogChanged();
      onChanged?.(next);
    }
  };
  return { level, busy, update };
}

function LevelMark({ level }: { level: number }) {
  const path = level === 5 ? 'M3 8H13' : level === 0 ? CIRCLE : `${CIRCLE}${DOT}`;
  return (
    <Vector
      path={path}
      stroke={LEVEL_COLORS[level]}
      style={{ width: 16, height: 16, flexShrink: 0, opacity: level === 3 ? 0.7 : 1 }}
    />
  );
}

function LevelChoices({
  id,
  level,
  busy,
  onPick,
}: {
  id: string;
  level: number;
  busy: boolean;
  onPick: (level: number) => void;
}) {
  return (
    <View style={{ gap: 6 }}>
      {LEVELS.map((name, choice) => (
        <Button
          key={name}
          id={`${id}-choice-${name.replaceAll(' ', '-')}`}
          disabled={busy}
          label={`Set study level to ${name}`}
          pressed={choice === level}
          onClick={() => onPick(choice)}
        >
          <LevelMark level={choice} />
          <Text style={{ color: colors.text }}>{name}</Text>
        </Button>
      ))}
    </View>
  );
}

export function StudyLevel(props: StudyLevelProps) {
  const { level, busy, update } = useStudyLevel(props);
  const [open, setOpen] = React.useState(false);
  const id = props.id || fileIdentity('study-level', props.courseId, props.filePath);
  const close = React.useCallback(() => setOpen(false), []);
  const pick = (next: number) => {
    close();
    void update(next);
  };
  return (
    <View>
      <Button
        id={id}
        title={`Study level: ${LEVELS[level]}`}
        disabled={busy}
        label={`Study level: ${LEVELS[level]}. Activate to change.`}
        expanded={open}
        controls={`${id}-picker`}
        onClick={() => setOpen((value) => !value)}
        style={{ opacity: level === 5 ? 0.5 : 1 }}
      >
        <LevelMark level={level} />
        <Text style={{ color: colors.text, fontSize: 12 }}>{LEVELS[level]}</Text>
      </Button>
      <Popover id={`${id}-picker`} title="Set study level" open={open} onClose={close} anchorId={id}>
        <LevelChoices id={id} level={level} busy={busy} onPick={pick} />
      </Popover>
    </View>
  );
}
