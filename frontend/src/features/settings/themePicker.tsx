import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { getThemePreference, setTheme, type ThemeName } from '@/shell/navChrome';
import { commonStyles } from '@/styles/common';
import { media } from '@/styles/constants.stylex';
import { colors, effects, layout, spacing, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  themeOptionLabel: {
    gap: spacing.xs,
    alignItems: 'center',
    color: colors.textPrimary,
    display: 'inline-flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightMedium,
    width: '100%',
  },
  labelIcon: {
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '1rem',
  },
  themeCheck: {
    alignItems: 'center',
    color: colors.success,
    display: 'inline-flex',
    fontSize: '1rem',
    marginLeft: 'auto',
  },
  themeOption: {
    margin: 0,
    padding: 0,
    cursor: 'pointer',
    display: 'block',
  },
  hiddenInput: {
    margin: 0,
    opacity: 0,
    pointerEvents: 'none',
    position: 'absolute',
    height: 0,
    width: 0,
  },
  themeOptionCard: {
    padding: spacing.md,
    borderColor: {
      default: colors.border,
      ':hover': colors.borderLight,
    },
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.sm,
    backgroundColor: {
      default: colors.surfaceRaised,
      ':hover': colors.surfaceHover,
    },
    display: 'flex',
    flexDirection: 'column',
    transitionDuration: '150ms',
    transitionProperty: 'border-color, background-color, box-shadow, transform',
  },
  themeOptionCardActive: {
    borderColor: colors.focusRing,
    backgroundColor: colors.surfaceHover,
    boxShadow: effects.shadow,
  },
  themeOptionSwatch: {
    borderRadius: layout.radiusSmall,
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    display: 'flex',
    height: '2rem',
  },
  themeOptionSwatchSystem: {
    borderColor: colors.border,
  },
  themeOptionSwatchLight: {
    borderColor: colors.borderLight,
  },
  themeOptionSwatchDark: {
    borderColor: colors.border,
  },
  themeOptionSwatchEink: {
    borderColor: colors.textPrimary,
  },
  themeSwatchBand: {
    backgroundColor: 'transparent',
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    height: '100%',
  },
  bandBg: (backgroundColor: string) => ({ backgroundColor }),
  themePicker: {
    margin: 0,
    padding: 0,
    borderWidth: 0,
    gap: spacing.md,
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(4, minmax(0, 1fr))',
      [media.tablet]: 'repeat(2, minmax(0, 1fr))',
    },
  },
});

const THEMES: [ThemeName, string, string][] = [
  ['system', 'circle-half', 'System'],
  ['light', 'sun', 'Light'],
  ['dark', 'moon-stars-fill', 'Dark'],
  ['eink', 'presentation', 'E-ink'],
];

function themeSwatchStyle(name: ThemeName) {
  if (name === 'light') return styles.themeOptionSwatchLight;
  if (name === 'dark') return styles.themeOptionSwatchDark;
  if (name === 'eink') return styles.themeOptionSwatchEink;
  return styles.themeOptionSwatchSystem;
}

function ThemeOptionLabel({ icon, label, checked }: { icon: string; label: string; checked: boolean }) {
  return (
    <span {...stylex.props(styles.themeOptionLabel)}>
      <span {...stylex.props(styles.labelIcon)}>
        <Icon name={icon} aria-hidden="true" />
      </span>
      {label}
      {checked && (
        <span {...stylex.props(styles.themeCheck)}>
          <Icon name="check-lg" aria-hidden="true" />
        </span>
      )}
    </span>
  );
}

function ThemeOption({
  name,
  icon,
  label,
  checked,
  onChange,
}: {
  name: ThemeName;
  icon: string;
  label: string;
  checked: boolean;
  onChange: () => void;
}) {
  return (
    <label {...stylex.props(styles.themeOption)}>
      <input
        type="radio"
        name="theme"
        value={name}
        checked={checked}
        onChange={onChange}
        {...stylex.props(styles.hiddenInput)}
      />
      <span {...stylex.props(styles.themeOptionCard, checked && styles.themeOptionCardActive)}>
        <span {...stylex.props(styles.themeOptionSwatch, themeSwatchStyle(name))} aria-hidden="true">
          <span {...stylex.props(styles.themeSwatchBand, styles.bandBg(name === 'light' ? '#fafafa' : '#09090b'))} />
          <span {...stylex.props(styles.themeSwatchBand, styles.bandBg(name === 'light' ? '#ffffff' : '#111113'))} />
          <span {...stylex.props(styles.themeSwatchBand, styles.bandBg(name === 'light' ? '#18181b' : '#fafafa'))} />
        </span>
        <ThemeOptionLabel icon={icon} label={label} checked={checked} />
      </span>
    </label>
  );
}

export function ThemePicker() {
  const [theme, setThemePreference] = React.useState<ThemeName>('system');
  const choose = (next: ThemeName) => {
    setThemePreference(next);
    setTheme(next);
  };

  React.useEffect(() => {
    setThemePreference(getThemePreference());
    const update = (event: Event) => {
      // SAFETY: the theme event is dispatched with a {theme} detail by
      // navChrome's setTheme; the cast narrows the generic Event.
      const detail = (event as CustomEvent<{ theme?: ThemeName }>).detail;
      setThemePreference(detail?.theme || getThemePreference());
    };
    document.addEventListener('treeEclass:theme', update);
    return () => document.removeEventListener('treeEclass:theme', update);
  }, []);
  return (
    <fieldset {...stylex.props(styles.themePicker)} aria-label="Theme">
      <legend {...stylex.props(commonStyles.srOnly)}>Theme</legend>
      <ThemeOptions theme={theme} onChange={choose} />
    </fieldset>
  );
}

function ThemeOptions({ theme, onChange }: { theme: ThemeName; onChange: (name: ThemeName) => void }) {
  return THEMES.map(([name, icon, label]) => (
    <ThemeOption
      key={name}
      name={name}
      icon={icon}
      label={label}
      checked={theme === name}
      onChange={() => onChange(name)}
    />
  ));
}
