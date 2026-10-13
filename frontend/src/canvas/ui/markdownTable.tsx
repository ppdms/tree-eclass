import { MarkdownInlines } from './markdownInlines';
import type { MarkdownBlock, MarkdownCell } from './markdownTypes';
import { colors } from './palette';
import { View } from './primitives';

type TableBlock = Extract<MarkdownBlock, { type: 'table' }>;

interface TableCellProps {
  id: string;
  cell: MarkdownCell | undefined;
  header: boolean;
  last: boolean;
  align: TableBlock['align'][number] | undefined;
}

function TableCell({ id, cell, header, last, align }: TableCellProps) {
  return (
    <View
      id={id}
      style={{
        minWidth: 0,
        padding: 10,
        backgroundColor: header ? colors.raised : undefined,
        borderBottomWidth: last ? 0 : 1,
        borderColor: colors.border,
      }}
    >
      {cell && (
        <MarkdownInlines
          id={`${id}-content`}
          parts={cell.parts}
          style={{ fontWeight: header ? 600 : 400, textAlign: align ?? 'left' }}
        />
      )}
    </View>
  );
}

export function MarkdownTable({ prefix, block }: { prefix: string; block: TableBlock }) {
  const id = `${prefix}-${block.key}`;
  const columns = block.rows[0]?.cells.length ?? 0;
  return (
    <View
      id={id}
      label="Markdown table"
      focusable
      scroll={{ axis: 'both' }}
      style={{
        minWidth: 0,
        width: '100%',
        maxHeight: 400,
        overflow: 'hidden',
        borderWidth: 1,
        borderColor: colors.border,
        borderRadius: 8,
      }}
    >
      <View
        id={`${id}-grid`}
        style={{
          display: 'grid',
          width: '100%',
          minWidth: columns * 112,
          gridTemplateColumns: Array.from({ length: columns }, () => '1fr' as const),
        }}
      >
        {block.rows.flatMap((row, rowIndex) =>
          Array.from({ length: columns }, (_, column) => {
            const cellId = `${prefix}-${row.key}-column-${column}`;
            return (
              <TableCell
                key={cellId}
                id={cellId}
                cell={row.cells[column]}
                header={rowIndex === 0}
                last={rowIndex === block.rows.length - 1}
                align={block.align[column]}
              />
            );
          }),
        )}
      </View>
    </View>
  );
}
