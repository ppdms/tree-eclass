import { useNavigate } from 'react-router';
import { Text } from '../ui/primitives';
import type { HostProps } from '../ui/types';

/** Host links retain inspectable hrefs; only logical routes enter the memory router. */
export function ActivityLink(props: HostProps & { href: string }) {
  const navigate = useNavigate();
  return (
    <Text
      {...props}
      onClick={
        props.external
          ? undefined
          : () => {
              void navigate(props.href);
            }
      }
    />
  );
}
