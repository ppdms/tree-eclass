import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import { Card as AstryxCard } from '@astryxdesign/core/Card';
import type { StyleXStyles } from '@stylexjs/stylex';
import { styles } from './styles';

type StyledDivProps = Omit<React.HTMLAttributes<HTMLDivElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};
type StyledHeadingProps = Omit<React.HTMLAttributes<HTMLHeadingElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};
type StyledParagraphProps = Omit<React.HTMLAttributes<HTMLParagraphElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};

const Card = React.forwardRef<HTMLDivElement, StyledDivProps>(({ style, ...props }, ref) => (
  <AstryxCard ref={ref} xstyle={style} {...props} />
));
Card.displayName = 'Card';

const CardHeader = React.forwardRef<HTMLDivElement, StyledDivProps>(({ style, ...props }, ref) => (
  <div ref={ref} {...stylex.props(styles.cardHeader, style)} {...props} />
));
CardHeader.displayName = 'CardHeader';

const CardTitle = React.forwardRef<HTMLHeadingElement, StyledHeadingProps>(({ style, ...props }, ref) => (
  <h3 ref={ref} {...stylex.props(styles.cardTitle, style)} {...props} />
));
CardTitle.displayName = 'CardTitle';

const CardDescription = React.forwardRef<HTMLParagraphElement, StyledParagraphProps>(({ style, ...props }, ref) => (
  <p ref={ref} {...stylex.props(styles.cardDescription, style)} {...props} />
));
CardDescription.displayName = 'CardDescription';

const CardContent = React.forwardRef<HTMLDivElement, StyledDivProps>(({ style, ...props }, ref) => (
  <div ref={ref} {...stylex.props(styles.cardContent, style)} {...props} />
));
CardContent.displayName = 'CardContent';

const CardFooter = React.forwardRef<HTMLDivElement, StyledDivProps>(({ style, ...props }, ref) => (
  <div ref={ref} {...stylex.props(styles.cardFooter, style)} {...props} />
));
CardFooter.displayName = 'CardFooter';

export { Card, CardHeader, CardFooter, CardTitle, CardDescription, CardContent };
