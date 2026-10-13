import { useState, type ReactNode } from 'react';
import { Button, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { styles, useSettingsContext, Hint } from './shared';

interface SectionProps {
  id: string;
  title: string;
  status?: string;
  description?: string;
  advanced?: boolean;
  children: ReactNode;
}
export function Section({ id, title, status, description, advanced = false, children }: SectionProps) {
  const context = useSettingsContext();
  const open = !advanced || context.opened.has(id);
  const [visited, setVisited] = useState(!advanced);
  if (open && !visited) setVisited(true);
  const label = `${title}${context.dirty.has(id) ? ' · Unsaved changes' : ''}`;
  return (
    <View id={id} onLayout={(rect) => context.observe(id, rect)} style={styles.stack}>
      <Panel>
        {advanced ? (
          <Button
            id={`settings-toggle-${id}`}
            expanded={open}
            controls={`${id}-content`}
            onClick={() => context.setOpen(id, !open)}
          >
            {`${open ? '−' : '+'} ${label}`}
          </Button>
        ) : (
          <Text style={styles.heading}>{label}</Text>
        )}
        {status && <Text style={{ ...styles.muted, color: colors.accent }}>{status}</Text>}
        {visited && (
          <View id={`${id}-content`} style={{ ...styles.stack, display: open ? 'flex' : 'none' }}>
            {description && <Hint>{description}</Hint>}
            {children}
          </View>
        )}
      </Panel>
    </View>
  );
}

export function Disclosure({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const [visited, setVisited] = useState(false);
  const toggle = () => {
    setOpen(!open);
    if (!open) setVisited(true);
  };
  return (
    <View style={styles.card}>
      <Button id={id} expanded={open} controls={`${id}-content`} onClick={toggle}>
        {`${open ? '−' : '+'} ${title}`}
      </Button>
      {visited && (
        <View id={`${id}-content`} style={{ ...styles.stack, display: open ? 'flex' : 'none' }}>
          {children}
        </View>
      )}
    </View>
  );
}
