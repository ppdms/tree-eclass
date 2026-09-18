import * as stylex from '@stylexjs/stylex';

export const commonStyles = stylex.create({
  srOnly: {
    margin: -1,
    padding: 0,
    borderWidth: 0,
    overflow: 'hidden',
    clip: 'rect(0, 0, 0, 0)',
    position: 'absolute',
    whiteSpace: 'nowrap',
    height: 1,
    width: 1,
  },
  truncate: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  centerContent: {
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'center',
  },
  fillParent: {
    height: '100%',
    width: '100%',
  },
});
