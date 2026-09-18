import * as stylex from '@stylexjs/stylex';

export const media = stylex.defineConsts({
  narrow: '@media (max-width: 30rem)',
  mobile: '@media (max-width: 40rem)',
  tabletOnly: '@media (min-width: 40.0625rem) and (max-width: 56.25rem)',
  tablet: '@media (max-width: 56.25rem)',
  desktop: '@media (min-width: 64rem)',
  reduceMotion: '@media (prefers-reduced-motion: reduce)',
});

export const layers = stylex.defineConsts({
  navigation: '100',
  overlay: '1000',
  popover: '1100',
});
