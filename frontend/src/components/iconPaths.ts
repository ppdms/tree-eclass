const PATHS = {
  list: '<path d="M4 6h16M4 12h16M4 18h16" />',
  search: '<circle cx="11" cy="11" r="7" /><path d="m20 20-4-4" />',
  tree:
    '<path fill="currentColor" stroke="none" ' +
    'd="M158 4q-3 -4 -8 -4t-8 4l-56 85q-3 4 -0.5 9t8.5 5h2' +
    'l-38 61q-3 4 -0.5 9t8.5 5h3l-31 62q-2 4 1 8.5t8 4.5' +
    'h84v47h38v-47h84q5 0 8 -4.5t1 -8.5l-31 -62h3q6 0 8.5 -5' +
    't-0.5 -9l-38 -61h2q6 0 8.5 -5t-0.5 -9z" />',
  'arrow-up-right': '<path d="M7 17 17 7M7 7h10v10" />',
  close: '<path d="m6 6 12 12M18 6 6 18" />',
} satisfies Record<string, string>;

export type ShellIconName = keyof typeof PATHS;

export function iconSvg(name: ShellIconName): string {
  const viewBox = name === 'tree' ? '0 0 300 300' : '0 0 24 24';
  return (
    `<svg data-nav-icon="${name}" aria-hidden="true" viewBox="${viewBox}" ` +
    'fill="none" stroke="currentColor" stroke-width="1.8" ' +
    `stroke-linecap="round" stroke-linejoin="round" width="1em" height="1em">${PATHS[name]}</svg>`
  );
}

export function iconPath(name: ShellIconName): string {
  return PATHS[name] ?? '';
}
