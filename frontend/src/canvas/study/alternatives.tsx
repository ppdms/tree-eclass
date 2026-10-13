import type { StudyAction } from '@/lib/types';
import { AppLink } from '../app/links';
import { Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { actionIdentity, alternativeHref } from './model';
import { styles } from './styles';

function kindLabel(action: StudyAction): string {
  const kind = action.action_type || action.kind;
  return kind === 'recall' ? 'Recall' : kind === 'solve' || kind === 'practice' ? 'Practice' : 'Read';
}

function AlternativeRow({ item, fallback, ordinal }: { item: StudyAction; fallback: boolean; ordinal: number }) {
  const id = actionIdentity(item);
  return (
    <View id={`study-queue-row-${id}`} style={styles.queueRow}>
      <Text style={styles.ordinal}>{String(ordinal).padStart(2, '0')}</Text>
      <View style={styles.stack}>
        <Text style={styles.label}>{kindLabel(item)}</Text>
        <Text style={styles.heading}>{item.instruction || item.recommended_action || item.file_name}</Text>
        <Text
          style={styles.muted}
        >{`${item.course_name || ''}${item.minutes ? ` · ${item.minutes} minutes` : ''}`}</Text>
      </View>
      <AppLink
        id={`study-queue-start-${id}`}
        href={alternativeHref(item, fallback)}
        target={fallback ? '_blank' : undefined}
        rel={fallback ? 'noopener noreferrer' : undefined}
        label={`Start ${item.instruction || item.file_name || 'study action'}`}
        style={styles.link}
      >
        Start ↗
      </AppLink>
    </View>
  );
}

export function Alternatives({ actions, fallback }: { actions: StudyAction[]; fallback: boolean }) {
  if (!actions.length) return null;
  return (
    <Panel id="study-alternatives">
      <View style={styles.stack}>
        <View style={styles.row}>
          <Text style={styles.heading}>Up next</Text>
          <Text style={styles.count}>{String(actions.length)}</Text>
        </View>
        <View>
          {actions.slice(0, 6).map((item, index) => (
            <AlternativeRow key={actionIdentity(item)} item={item} fallback={fallback} ordinal={index + 1} />
          ))}
        </View>
      </View>
    </Panel>
  );
}
