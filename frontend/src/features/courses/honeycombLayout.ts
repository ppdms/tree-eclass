const REMAINDER_ROWS = [[], [], [1], [1, 2], [2, 0, 1]] as const;

const LEFT_EDGE_OFFSETS = [[], [0], [0, 1], [1, 0, 1], [0, 1, 0, 1]] as const;

export interface HoneycombRow<T> {
  cells: T[];
  offset: number;
}

export function honeycombRowLengths(total: number, requestedRows: number) {
  if (total <= 0) return [];
  const rowCount = Math.min(total, Math.max(1, requestedRows));
  const baseLength = Math.floor(total / rowCount);
  const lengths = Array.from({ length: rowCount }, () => baseLength);
  const remainderRows = REMAINDER_ROWS[rowCount] ?? [];
  for (let index = 0; index < total % rowCount; index += 1) {
    const rowIndex = remainderRows[index] ?? Math.floor(rowCount / 2);
    lengths[rowIndex] = (lengths[rowIndex] ?? 0) + 1;
  }
  return lengths;
}

export function honeycombRows<T>(cells: T[], requestedRows: number): HoneycombRow<T>[] {
  const lengths = honeycombRowLengths(cells.length, requestedRows);
  if (!lengths.length) return [];
  const offsets = LEFT_EDGE_OFFSETS[lengths.length] ?? [];
  let cursor = 0;
  return lengths.map((length, rowIndex) => {
    const row = {
      cells: cells.slice(cursor, cursor + length),
      offset: offsets[rowIndex] ?? rowIndex % 2,
    };
    cursor += length;
    return row;
  });
}
