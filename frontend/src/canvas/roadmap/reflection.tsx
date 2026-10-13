import { useState } from 'react';
import { Button } from '../ui/controls';
import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import type { RoadmapReflection } from './event';
import { styles } from './styles';

interface ReflectionProps {
  id: string;
  value: RoadmapReflection;
  onChange: (value: RoadmapReflection) => void;
  disabled: boolean;
}

function ConfidenceFields({ id, value, onChange, disabled }: ReflectionProps) {
  return (
    <>
      <Text style={styles.meta}>Confidence</Text>
      <View style={styles.row}>
        {['1', '3', '5'].map((confidence) => (
          <Button
            key={confidence}
            id={`${id}:confidence:${confidence}`}
            disabled={disabled}
            pressed={value.confidence === confidence}
            tone={value.confidence === confidence ? 'primary' : 'normal'}
            onClick={() => onChange({ ...value, confidence })}
          >
            {confidence}
          </Button>
        ))}
      </View>
    </>
  );
}

export function ReflectionFields({ id, value, onChange, disabled }: ReflectionProps) {
  const [open, setOpen] = useState(false);
  return (
    <View style={styles.stack}>
      <Button id={`${id}:details`} expanded={open} controls={`${id}:fields`} onClick={() => setOpen(!open)}>
        Add details
      </Button>
      {open && (
        <View id={`${id}:fields`} style={styles.stack}>
          <Field
            id={`${id}:minutes`}
            label="Minutes"
            type="number"
            value={value.minutes}
            min={1}
            max={1440}
            step={1}
            disabled={disabled}
            onChange={(minutes) => onChange({ ...value, minutes })}
          />
          <ConfidenceFields id={id} value={value} onChange={onChange} disabled={disabled} />
          <Field
            id={`${id}:note`}
            label="Note"
            value={value.note}
            multiline
            rows={3}
            maxLength={4000}
            placeholder="What clicked or blocked you?"
            disabled={disabled}
            onChange={(note) => onChange({ ...value, note })}
          />
        </View>
      )}
    </View>
  );
}
