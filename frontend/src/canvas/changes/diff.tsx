import { useMemo } from 'react';
import type { ChangeItem } from '@/lib/types';
import { AppLink } from '../app/links';
import { colors } from '../ui/palette';
import { Icon, Text, Vector, View } from '../ui/primitives';
import { buildChangeRows, changeLabel, type ChangeKind, type ChangeRow } from './model';
import { styles } from './styles';

export interface ChangeDiffTreeProps {
  changes: ChangeItem[];
  webdavFolder?: string;
  scope: string;
}

export const CHANGE_PRESENTATION = {
  added: { symbol: '+', color: colors.success },
  deleted: { symbol: '−', color: colors.danger },
  modified: { symbol: '~', color: colors.warning },
  mixed: { symbol: '±', color: colors.link },
  unchanged: { symbol: '', color: colors.muted },
} satisfies Record<ChangeKind, { symbol: string; color: string }>;

/** Box-drawing glyphs are unavailable in bundled Mono; connectors use canvas geometry. */
function Branch({ prefix }: { prefix: string }) {
  const unit = 7.8;
  const path = [...prefix]
    .map((character, index) => {
      const x = index * unit + unit / 2;
      if (character === '│') return `M${x} 0V16`;
      if (character === '─') return `M${index * unit} 8h${unit}`;
      if (character === '├') return `M${x} 0V16M${x} 8h${unit / 2}`;
      if (character === '└') return `M${x} 0V8h${unit / 2}`;
      return '';
    })
    .join('');
  return <Vector path={path} style={{ width: prefix.length * unit, height: 16, color: colors.muted }} />;
}

function DiffRow({ row, scope }: { row: ChangeRow; scope: string }) {
  const presentation = CHANGE_PRESENTATION[row.kind];
  const id = `change-tree-${scope}-${encodeURIComponent(row.identity)}`;
  const textStyle = {
    ...styles.fileName,
    strike: row.kind === 'deleted',
    fontWeight: row.directory ? 650 : 400,
  };
  return (
    <View id={id} style={{ ...styles.row, color: presentation.color }}>
      {row.prefix && <Branch prefix={row.prefix} />}
      <Icon name={row.directory ? 'folder' : 'file'} style={styles.icon} />
      {presentation.symbol && <Text style={styles.symbol}>{presentation.symbol}</Text>}
      {row.href ? (
        <AppLink
          id={`${id}-source`}
          href={row.href}
          target="_blank"
          rel="noopener noreferrer"
          label={`Open ${row.name}`}
          title={row.path}
          style={textStyle}
        >
          {row.name}
        </AppLink>
      ) : (
        <Text title={row.path} label={`${changeLabel(row.type)}: ${row.name}`} style={textStyle}>
          {row.name}
        </Text>
      )}
      {row.diffHref && (
        <AppLink
          id={`${id}-comparison`}
          href={row.diffHref}
          target="_blank"
          rel="noopener noreferrer"
          label={`Open visual diff for ${row.name}`}
          title="Open visual diff"
          style={styles.diffLink}
        >
          Visual diff
        </AppLink>
      )}
    </View>
  );
}

export function ChangeDiffTree({ changes, webdavFolder, scope }: ChangeDiffTreeProps) {
  const rows = useMemo(() => buildChangeRows(changes, webdavFolder), [changes, webdavFolder]);
  if (!changes.length) {
    return <Text style={styles.muted}>No file-level details were recorded for this change.</Text>;
  }
  return (
    <View id={`change-tree-${scope}`} label="Changed files" style={styles.tree}>
      {rows.map((row) => (
        <DiffRow key={row.identity} row={row} scope={scope} />
      ))}
    </View>
  );
}
