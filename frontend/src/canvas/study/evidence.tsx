import { useState } from 'react';
import type { EvidenceLink, StudyAction } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { Popover, ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { actionIdentity, evidenceHref, evidenceIdentity } from './model';
import { styles } from './styles';

function EvidenceRow({ action, link }: { action: StudyAction; link: EvidenceLink }) {
  const href = evidenceHref(action, link);
  const id = `study-evidence-${actionIdentity(action)}-${evidenceIdentity(link)}`;
  if (!href)
    return (
      <Text id={id} style={styles.muted}>
        {link.label || link.title || 'Evidence unavailable'}
      </Text>
    );
  const label =
    (link.label || link.title || link.source_name || 'Open source') +
    (link.page_number ? ` · p. ${link.page_number}` : '');
  const internal = href.startsWith('/study');
  return (
    <AppLink
      id={id}
      block
      href={href}
      target={internal ? undefined : '_blank'}
      rel={internal ? undefined : 'noopener noreferrer'}
      style={styles.sourceRow}
    >
      <Text>{label}</Text>
    </AppLink>
  );
}

export function StudyEvidence({ action }: { action: StudyAction }) {
  const [open, setOpen] = useState(false);
  const links = action.evidence_links || [];
  if (!links.length) return null;
  const id = `study-sources-${actionIdentity(action)}`;
  return (
    <>
      <Button id={`${id}-trigger`} expanded={open} controls={id} onClick={() => setOpen((value) => !value)}>
        {`${links.length} source${links.length === 1 ? '' : 's'}`}
      </Button>
      <Popover id={id} title="Study sources" open={open} onClose={() => setOpen(false)} anchorId={`${id}-trigger`}>
        <ScrollArea id={`${id}-scroll`} height={288}>
          <View style={styles.stack}>
            {links.map((link) => (
              <EvidenceRow key={evidenceIdentity(link)} action={action} link={link} />
            ))}
          </View>
        </ScrollArea>
      </Popover>
    </>
  );
}
