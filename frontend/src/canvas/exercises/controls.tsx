import { useContext, type ReactNode } from 'react';
import { OnGlass } from '../ui/controls';
import { Icon, Text, View } from '../ui/primitives';
import type { HostProps, IconName, UIStyle } from '../ui/types';
import { styles } from './styles';

interface ActionProps {
  id: string;
  children: ReactNode;
  onClick: () => void;
  disabled?: boolean;
  style?: UIStyle;
  icon?: IconName;
}

export function ExerciseAction({ id, children, onClick, disabled = false, style, icon }: ActionProps) {
  const glass = useContext(OnGlass);
  const props = {
    id,
    disabled,
    style: {
      ...styles.button,
      ...(glass ? styles.glassButton : undefined),
      ...style,
      ...(disabled ? styles.disabled : undefined),
    },
    onClick: disabled ? undefined : onClick,
  } satisfies HostProps;
  if (!icon) return <Text {...props}>{children}</Text>;
  return (
    <View {...props} style={{ ...props.style, flexDirection: 'row', alignItems: 'center', gap: 6 }}>
      <Text id={`${id}-label`}>{children}</Text>
      <Icon name={icon} style={{ width: 14, height: 14 }} />
    </View>
  );
}
