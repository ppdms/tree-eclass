import { useState } from 'react';
import type { StudyAction } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Field, Select } from '../ui/forms';
import { Popover } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { actionIdentity, sessionHref } from './model';
import { OUTCOMES, type OutcomeResponse, type Reflection } from './outcomeModel';
import { styles } from './styles';
import { useOutcome } from './useOutcome';

const CONFIDENCE_OPTIONS = [
  { value: '', label: 'Not set' },
  { value: '1', label: 'Low' },
  { value: '3', label: 'Fair' },
  { value: '5', label: 'Strong' },
];

function ReflectionFields({
  id,
  fields,
  onChange,
}: {
  id: string;
  fields: Reflection;
  onChange: (fields: Reflection) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <View style={styles.stack}>
      <Button id={`${id}-details`} expanded={open} controls={`${id}-fields`} onClick={() => setOpen(!open)}>
        {open ? 'Hide details' : 'Add details'}
      </Button>
      {open && (
        <View id={`${id}-fields`} style={styles.stack}>
          <View style={styles.fieldGrid}>
            <Field
              id={`${id}-minutes`}
              label="Minutes"
              type="number"
              min={1}
              max={1440}
              step={1}
              value={fields.minutes}
              onChange={(minutes) => onChange({ ...fields, minutes })}
            />
            <Select
              id={`${id}-confidence`}
              label="Confidence"
              options={CONFIDENCE_OPTIONS}
              value={fields.confidence}
              onChange={(confidence) => onChange({ ...fields, confidence })}
            />
          </View>
          <Field
            id={`${id}-note`}
            label="Note"
            value={fields.note}
            maxLength={4000}
            placeholder="What clicked, or what blocked you?"
            onChange={(note) => onChange({ ...fields, note })}
          />
        </View>
      )}
    </View>
  );
}

function OutcomeFields({ id, state }: { id: string; state: ReturnType<typeof useOutcome> }) {
  return (
    <View style={styles.stack}>
      <View style={styles.fieldGrid}>
        {OUTCOMES.map((outcome) => (
          <Button
            key={outcome.value}
            id={`${id}-${outcome.value}`}
            disabled={state.busy}
            onClick={() => {
              void state.record(outcome.value);
            }}
          >
            {state.busy ? 'Saving…' : outcome.label}
          </Button>
        ))}
      </View>
      <ReflectionFields id={id} fields={state.fields} onChange={state.setFields} />
      {state.error && (
        <Notice tone="danger">
          <Text>{state.error}</Text>
        </Notice>
      )}
    </View>
  );
}

export function RecordOutcome({
  action,
  onRecorded,
}: {
  action: StudyAction;
  onRecorded: (result: OutcomeResponse) => void;
}) {
  const [open, setOpen] = useState(false);
  const state = useOutcome(action, onRecorded);
  const id = `study-outcome-${actionIdentity(action)}`;
  return (
    <View style={styles.stack}>
      <View style={styles.actions}>
        <AppLink id={`${id}-start`} href={sessionHref(action)} style={styles.link}>
          ▶ Start session
        </AppLink>
        <Button id={`${id}-finish`} expanded={open} controls={id} onClick={() => setOpen(!open)}>
          Finish
        </Button>
      </View>
      {state.error && !open && (
        <Notice tone="danger">
          <Text>{state.error}</Text>
        </Notice>
      )}
      <Popover id={id} title="Log study progress" open={open} onClose={() => setOpen(false)} anchorId={`${id}-finish`}>
        <OutcomeFields id={id} state={state} />
      </Popover>
    </View>
  );
}
