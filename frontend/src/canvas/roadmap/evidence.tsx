import { useState } from 'react';
import type { EvidenceLink } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { Popover, ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { semanticId, styles } from './styles';

export interface EvidenceProps {
  id: string;
  links?: EvidenceLink[];
  courseId: string | number;
  actionId?: string;
}

function EvidenceRow({
  link,
  courseId,
  actionId,
  id,
}: {
  link: EvidenceLink;
  courseId: string | number;
  actionId?: string;
  id: string;
}) {
  const reader = Boolean(link.renderable && link.document_id);
  const action = actionId ? `&action_id=${encodeURIComponent(actionId)}` : '';
  const target = reader
    ? `/study/session?course_id=${encodeURIComponent(courseId)}&document_id=${encodeURIComponent(
        String(link.document_id),
      )}${action}`
    : link.url || (link.source_path ? `/files${encodeURI(link.source_path)}` : undefined);
  const category =
    link.evidence_class === 'official_material'
      ? 'Official'
      : link.evidence_class === 'past_exam'
        ? 'Past exam'
        : link.evidence_class?.startsWith('community')
          ? 'Community'
          : 'Material';
  const title = readableValue(link.label || link.title || link.evidence_ref, 'Source');
  return (
    <View style={{ ...styles.stack, gap: 5, padding: 10 }}>
      <Text style={styles.meta}>{category}</Text>
      {target ? (
        <AppLink
          id={id}
          href={target}
          external={!reader}
          target={reader ? undefined : '_blank'}
          rel={reader ? undefined : 'noopener noreferrer'}
          style={{ ...styles.body, color: colors.link }}
        >
          {title}
        </AppLink>
      ) : (
        <Text style={styles.body}>{title}</Text>
      )}
    </View>
  );
}

export function Evidence({ id, links = [], courseId, actionId }: EvidenceProps) {
  const [open, setOpen] = useState(false);
  if (!links.length) return null;
  const anchorId = `${id}:trigger`;
  return (
    <View>
      <Button id={anchorId} expanded={open} controls={`${id}:popover`} onClick={() => setOpen(!open)}>
        {`${links.length} ${links.length === 1 ? 'source' : 'sources'}`}
      </Button>
      <Popover id={`${id}:popover`} anchorId={anchorId} title="Sources" open={open} onClose={() => setOpen(false)}>
        <ScrollArea id={`${id}:scroll`} height={Math.min(288, links.length * 72)} maxHeight={288}>
          {links.map((link) => {
            const identity = semanticId(
              link.evidence_class,
              link.evidence_ref,
              link.document_id,
              link.url,
              link.source_path,
              link.page_number,
              link.label,
              link.title,
            );
            return (
              <EvidenceRow
                key={identity}
                id={`${id}:${identity}`}
                link={link}
                courseId={courseId}
                actionId={actionId}
              />
            );
          })}
        </ScrollArea>
      </Popover>
    </View>
  );
}
