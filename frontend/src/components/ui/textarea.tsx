import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';
import { styles } from './styles';

type TextareaProps = Omit<React.TextareaHTMLAttributes<HTMLTextAreaElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};

const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(({ style, ...props }, ref) => (
  <textarea ref={ref} {...stylex.props(styles.field, style)} {...props} />
));
Textarea.displayName = 'Textarea';

export { Textarea };
export type { TextareaProps };
