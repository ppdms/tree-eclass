export type ThemeName = 'system' | 'light' | 'dark' | 'eink';
export type ResolvedTheme = Exclude<ThemeName, 'system'>;

export const colors = {
  page: '@page',
  surface: '@surface',
  raised: '@raised',
  hover: '@hover',
  border: '@border',
  text: '@text',
  muted: '@muted',
  accent: '@accent',
  success: '@success',
  warning: '@warning',
  danger: '@danger',
  link: '@link',
  onAccent: '@onAccent',
  soft: '@soft',
  subtle: '@subtle',
  attention: '@attention',
  dangerSurface: '@dangerSurface',
  selection: '@selection',
  glassFill: '@glassFill',
  glassHover: '@glassHover',
  glassEdge: '@glassEdge',
  study0: '@study0',
  study1: '@study1',
  study2: '@study2',
  study3: '@study3',
  study4: '@study4',
} as const;

type ColorName = keyof typeof colors;
const themes = {
  dark: {
    page: '#09090b',
    surface: '#18181b',
    raised: '#27272a',
    hover: '#3f3f46',
    border: '#3f3f46',
    text: '#fafafa',
    muted: '#a1a1aa',
    accent: '#bef264',
    success: '#a3e635',
    warning: '#facc15',
    danger: '#fb7185',
    link: '#93c5fd',
    onAccent: '#18181b',
    soft: '#d4d4d8',
    subtle: '#111113',
    attention: '#292315',
    dangerSurface: '#291820',
    selection: '#2563eb60',
    glassFill: '#ffffff24',
    glassHover: '#ffffff14',
    glassEdge: '#ffffff1f',
    study0: '#202023',
    study1: '#fb7185',
    study2: '#facc15',
    study3: '#77a62b',
    study4: '#a3e635',
  },
  light: {
    page: '#fafafa',
    surface: '#ffffff',
    raised: '#f4f4f5',
    hover: '#e4e4e7',
    border: '#d4d4d8',
    text: '#18181b',
    muted: '#52525b',
    accent: '#4d7c0f',
    success: '#4d7c0f',
    warning: '#a16207',
    danger: '#be123c',
    link: '#1d4ed8',
    onAccent: '#ffffff',
    soft: '#3f3f46',
    subtle: '#f4f4f5',
    attention: '#fef3c7',
    dangerSurface: '#ffe4e6',
    selection: '#2563eb40',
    glassFill: '#00000012',
    glassHover: '#0000000a',
    glassEdge: '#0000000f',
    study0: '#e4e4e7',
    study1: '#be123c',
    study2: '#a16207',
    study3: '#65a30d',
    study4: '#3f6212',
  },
  eink: {
    page: '#ffffff',
    surface: '#ffffff',
    raised: '#ffffff',
    hover: '#ffffff',
    border: '#000000',
    text: '#000000',
    muted: '#000000',
    accent: '#000000',
    success: '#000000',
    warning: '#000000',
    danger: '#000000',
    link: '#000000',
    onAccent: '#ffffff',
    soft: '#000000',
    subtle: '#ffffff',
    attention: '#ffffff',
    dangerSurface: '#ffffff',
    selection: '#88888880',
    glassFill: '#ffffff',
    glassHover: '#ffffff',
    glassEdge: '#000000',
    study0: '#ffffff',
    study1: '#d4d4d4',
    study2: '#a3a3a3',
    study3: '#525252',
    study4: '#000000',
  },
} satisfies Record<ResolvedTheme, Record<ColorName, string>>;

let current: ResolvedTheme = 'dark';

export function setPalette(theme: ResolvedTheme): boolean {
  if (theme === current) return false;
  current = theme;
  return true;
}

export function currentPalette(): ResolvedTheme {
  return current;
}

function isColorName(value: string): value is ColorName {
  return value in colors;
}

export function color(value: string): string {
  if (!value.startsWith('@')) return value;
  const key = value.slice(1);
  if (!isColorName(key)) throw new Error(`Unknown canvas color ${value}`);
  return themes[current][key];
}
