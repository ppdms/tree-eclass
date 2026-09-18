import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  settingsNav: {
    gap: '1rem',
    display: 'flex',
    flexDirection: 'column',
    position: {
      default: 'sticky',
      [media.tablet]: 'static',
    },
    minWidth: 0,
    top: '5rem',
  },
  settingsNavHeading: {
    display: {
      default: 'grid',
      [media.tablet]: 'none',
    },
  },
  settingsNavHeadingTitle: {
    display: 'block',
    fontSize: '0.9375rem',
  },
  settingsNavHeadingDesc: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginTop: '0.2rem',
  },
  settingsNavList: {
    gap: '0.25rem',
    display: 'flex',
    flexDirection: {
      default: 'column',
      [media.tablet]: 'row',
    },
    overflowX: {
      default: 'visible',
      [media.tablet]: 'auto',
    },
  },
  settingsNavLink: {
    borderColor: {
      default: 'transparent',
      ':focus-visible': colors.border,
      ':hover': colors.border,
    },
    borderRadius: '0.6rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.5rem',
    outline: {
      default: 'none',
      ':focus-visible': `0.125rem solid ${colors.focusRing}`,
    },
    paddingBlock: '0.5rem',
    paddingInline: '0.625rem',
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: {
      default: 'transparent',
      ':focus-visible': colors.surfaceRaised,
      ':hover': colors.surfaceRaised,
    },
    color: {
      default: colors.textSecondary,
      ':focus-visible': colors.textPrimary,
      ':hover': colors.textPrimary,
    },
    display: 'flex',
    fontSize: typography.sizeSm,
    outlineOffset: {
      default: 0,
      ':focus-visible': '0.125rem',
    },
    transitionDuration: '150ms',
    transitionProperty: 'background-color, border-color, color',
    whiteSpace: 'nowrap',
    minHeight: '2.35rem',
  },
  settingsNavActive: {
    borderColor: colors.border,
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimary,
    fontWeight: typography.weightMedium,
  },
  navIcon: {
    flexShrink: 0,
    textAlign: 'center',
    width: '1rem',
  },
  settingsNavDirtyDot: {
    borderRadius: '9999px',
    backgroundColor: colors.warning,
    boxShadow: `0 0 0 0.1875rem color-mix(in srgb, ${colors.warning} 18%, transparent)`,
    height: '0.4rem',
    marginLeft: 'auto',
    width: '0.4rem',
  },
  settingsNavScrollHint: {
    color: colors.textSecondary,
    display: {
      default: 'none',
      [media.tablet]: 'block',
    },
    fontSize: typography.sizeXs,
    marginTop: '0.25rem',
  },
});

const SETTINGS_LINKS: [string, string, string][] = [
  ['appearance', 'palette2', 'Appearance'],
  ['add-course', 'journal-bookmark', 'Add course'],
  ['hidden-courses', 'eye-slash', 'Hidden courses'],
  ['knowledge', 'database-check', 'Knowledge maintenance'],
  ['ai', 'cpu', 'AI backends and models'],
  ['discord-course-mapping', 'plug', 'Discord course mapping'],
  ['discord-exporter', 'download', 'Discord exporter'],
  ['webhook', 'bell', 'Webhook'],
  ['credentials', 'key-fill', 'eClass credentials'],
  ['storage', 'cloud', 'Document storage'],
  ['preferences', 'sliders', 'Application preferences'],
  ['data-export', 'download', 'Learner data'],
];

function useActiveSection(): string {
  const [active, setActive] = React.useState<string>(SETTINGS_LINKS[0]?.[0] ?? 'appearance');
  React.useEffect(() => {
    const sections = SETTINGS_LINKS.map(([id]) => document.getElementById(id)).filter(
      (v): v is HTMLElement => v !== null,
    );
    if (!sections.length) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
        const first = visible[0];
        if (first) setActive(first.target.id);
      },
      { rootMargin: '-40% 0% -55% 0%' },
    );
    sections.forEach((section) => observer.observe(section));
    return () => observer.disconnect();
  }, []);
  return active;
}

export interface SettingsNavProps {
  dirtySections?: Set<string>;
}

export function SettingsNav({ dirtySections = new Set<string>() }: SettingsNavProps) {
  const active = useActiveSection();
  return (
    <aside {...stylex.props(styles.settingsNav)} aria-label="Settings sections">
      <div {...stylex.props(styles.settingsNavHeading)}>
        <strong {...stylex.props(styles.settingsNavHeadingTitle)}>Settings sections</strong>
        <p {...stylex.props(styles.settingsNavHeadingDesc)}>Jump to a workspace area.</p>
      </div>
      <nav {...stylex.props(styles.settingsNavList)} aria-label="Settings areas">
        {SETTINGS_LINKS.map(([id, icon, label]) => (
          <a
            href={`#${id}`}
            key={id}
            {...stylex.props(styles.settingsNavLink, active === id && styles.settingsNavActive)}
            aria-current={active === id ? 'location' : undefined}
            onClick={() => {
              const target = document.getElementById(id);
              if (target instanceof HTMLDetailsElement) target.open = true;
            }}
          >
            <span {...stylex.props(styles.navIcon)}>
              <Icon name={icon} aria-hidden="true" />
            </span>
            <span>{label}</span>
            {dirtySections.has(id) && (
              <span
                {...stylex.props(styles.settingsNavDirtyDot)}
                title="Unsaved changes"
                aria-label="Unsaved changes"
              />
            )}
          </a>
        ))}
      </nav>
      <p {...stylex.props(styles.settingsNavScrollHint)}>Swipe to see all sections</p>
    </aside>
  );
}
