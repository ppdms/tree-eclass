import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';
import { styles } from './styles';

type InputProps = Omit<React.InputHTMLAttributes<HTMLInputElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};

const Input = React.forwardRef<HTMLInputElement, InputProps>(({ style, type, ...props }, ref) => (
  <input type={type} ref={ref} {...stylex.props(styles.field, style)} {...props} />
));
Input.displayName = 'Input';

export { Input };
export type { InputProps };
