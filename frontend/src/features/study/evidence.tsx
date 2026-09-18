import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ExternalLink, Files } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Popover } from '@astryxdesign/core/Popover';
import type { EvidenceLink, StudyAction } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studySourcePopover: {
    padding: '0.45rem',
    width: 'min(22rem, calc(100vw - 2rem))',
  },
  studySourceList: {
    gap: '0.2rem',
    display: 'grid',
  },
  studySourceLink: {
    padding: '0.65rem',
    borderRadius: layout.radiusMedium,
    gap: '0.65rem',
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: colors.textPrimarySoft,
    display: 'grid',
    fontSize: typography.sizeSm,
    gridTemplateColumns: 'auto minmax(0, 1fr) auto',
  },
  studySourceLinkSvg: {
    color: colors.textSecondary,
    height: '0.9rem',
    width: '0.9rem',
  },
  studySourceTrigger: {
    margin: 0,
    color: colors.textSecondary,
  },
  studySourceTriggerSvg: {
    height: '0.95rem',
    width: '0.95rem',
  },
});

export function Evidence({ action }: { action: StudyAction }) {
  const links: EvidenceLink[] = action?.evidence_links || [];
  if (!links.length) return null;
  return (
    <Popover
      placement="below"
      alignment="start"
      content={
        <div {...stylex.props(styles.studySourcePopover)}>
          <div {...stylex.props(styles.studySourceList)}>
            {links.map((link, index) => {
              const href =
                link.renderable && link.document_id
                  ? `/study/session?course_id=${action.course_id}` +
                    `&document_id=${encodeURIComponent(String(link.document_id))}` +
                    `&action_id=${encodeURIComponent(action.action_id || '')}`
                  : link.url || (link.source_path ? `/files${encodeURI(link.source_path)}` : null);
              return href ? (
                <a
                  {...stylex.props(styles.studySourceLink)}
                  href={href}
                  target={href.startsWith('/study') ? undefined : '_blank'}
                  rel={href.startsWith('/study') ? undefined : 'noopener'}
                  key={index}
                >
                  <Files aria-hidden="true" {...stylex.props(styles.studySourceLinkSvg)} />
                  <span>
                    {link.label || link.title || link.source_name || 'Open source'}
                    {link.page_number ? ` · p. ${link.page_number}` : ''}
                  </span>
                  <ExternalLink aria-hidden="true" {...stylex.props(styles.studySourceLinkSvg)} />
                </a>
              ) : (
                <span key={index}>{link.label || link.title || 'Evidence unavailable'}</span>
              );
            })}
          </div>
        </div>
      }
    >
      <Button type="button" variant="ghost" size="sm" style={styles.studySourceTrigger}>
        <Files aria-hidden="true" {...stylex.props(styles.studySourceTriggerSvg)} /> {links.length} source
        {links.length === 1 ? '' : 's'}
      </Button>
    </Popover>
  );
}
