import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ExternalLink, Files } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Popover } from '@astryxdesign/core/Popover';
import { readableValue } from '@/lib/display';
import type { EvidenceLink } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  courseEvidencePopover: {
    padding: '.75rem',
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '.25rem',
    backgroundColor: colors.surface,
    display: 'grid',
    maxHeight: '18rem',
    overflowY: 'auto',
    width: 'min(24rem, calc(100vw - 2rem))',
  },
  courseEvidenceTrigger: {
    gap: '.35rem',
    paddingBlock: 0,
    paddingInline: '.3rem',
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    textTransform: 'none',
    height: '1.6rem',
  },
  courseEvidenceRow: {
    padding: '.55rem',
    borderRadius: '.35rem',
    gap: '.55rem',
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: {
      default: 'transparent',
      ':hover': colors.surfaceHover,
    },
    color: colors.textPrimary,
    display: 'grid',
    gridTemplateColumns: 'auto minmax(0, 1fr) auto',
  },
  courseEvidenceClass: {
    color: colors.success,
    fontSize: typography.sizeXs,
    fontWeight: 650,
  },
  courseEvidenceTitle: {
    overflow: 'hidden',
    fontSize: '0.75rem',
    fontWeight: 500,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
});

function evidenceLabel(link: EvidenceLink): string {
  if (link.evidence_class === 'official_material') return 'Official';
  if (link.evidence_class === 'past_exam') return 'Past exam';
  if (link.evidence_class?.startsWith('community')) return 'Community';
  return 'Material';
}

function evidenceTarget(link: EvidenceLink, courseId: string | number, actionId?: string) {
  if (link.renderable && link.document_id) {
    const action = actionId ? `&action_id=${encodeURIComponent(actionId)}` : '';
    return `/study/session?course_id=${courseId}&document_id=${encodeURIComponent(String(link.document_id))}${action}`;
  }
  return link.url || (link.source_path ? `/files${encodeURI(link.source_path)}` : undefined);
}

function EvidenceRow({
  link,
  courseId,
  actionId,
}: {
  link: EvidenceLink;
  courseId: string | number;
  actionId?: string;
}) {
  const target = evidenceTarget(link, courseId, actionId);
  const title = readableValue(link.label || link.title || link.evidence_ref, 'Source');
  const content = (
    <>
      <span {...stylex.props(styles.courseEvidenceClass)}>{evidenceLabel(link)}</span>
      <strong {...stylex.props(styles.courseEvidenceTitle)}>{title}</strong>
      {target && <ExternalLink size={13} />}
    </>
  );
  return target ? (
    <a
      href={target}
      target={target.startsWith('/study') ? undefined : '_blank'}
      rel="noopener"
      {...stylex.props(styles.courseEvidenceRow)}
    >
      {content}
    </a>
  ) : (
    <div {...stylex.props(styles.courseEvidenceRow)}>{content}</div>
  );
}

export function Evidence({
  links = [],
  courseId,
  actionId,
}: {
  links?: EvidenceLink[];
  courseId: string | number;
  actionId?: string;
}) {
  if (!links.length) return null;
  return (
    <Popover
      placement="below"
      alignment="start"
      content={
        <div {...stylex.props(styles.courseEvidencePopover)}>
          {links.map((link, index) => (
            <EvidenceRow
              key={`${link.evidence_ref || link.label}-${index}`}
              link={link}
              courseId={courseId}
              actionId={actionId}
            />
          ))}
        </div>
      }
    >
      <Button
        variant="ghost"
        size="sm"
        icon={<Files size={13} aria-hidden="true" />}
        {...stylex.props(styles.courseEvidenceTrigger)}
      >
        {links.length} {links.length === 1 ? 'source' : 'sources'}
      </Button>
    </Popover>
  );
}
