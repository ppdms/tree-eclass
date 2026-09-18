import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';
import { styles } from './styles';

type SkeletonProps = Omit<React.HTMLAttributes<HTMLDivElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};

function Skeleton({ style, ...props }: SkeletonProps) {
  return <div {...stylex.props(styles.skeleton, style)} {...props} />;
}

export { Skeleton };
export type { SkeletonProps };
