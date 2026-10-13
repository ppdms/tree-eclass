import { colors } from '../ui/palette';
import type { UIStyle } from '../ui/types';

export const pages = [
  ['/', 'Activity'],
  ['/courses', 'Courses'],
  ['/study', 'Study'],
  ['/exercises', 'Exercises'],
  ['/settings', 'Settings'],
] as const;

// Items on glass use translucent capsule fills, never glass on glass.
export const linkStyle: UIStyle = {
  minHeight: 44,
  padding: 12,
  borderRadius: 22,
  fontSize: 14,
  fontWeight: 550,
  color: colors.muted,
  hoverColor: colors.text,
  hoverBackground: colors.glassHover,
};

export function section(path: string): string {
  if (['/', '/activity', '/announcements', '/history', '/timeline'].includes(path)) return 'Activity';
  return (
    pages.find(([href]) => href !== '/' && (path === href || path.startsWith(`${href}/`)))?.[1] ??
    (path.startsWith('/ask') ? 'Ask' : 'Activity')
  );
}

export function selected(path: string, label: string): UIStyle {
  const style = { ...linkStyle };
  if (section(path) === label) {
    style.color = colors.text;
    style.backgroundColor = colors.glassFill;
    style.fontWeight = 650;
  }
  return style;
}
