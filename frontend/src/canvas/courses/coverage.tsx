import { memo, useMemo } from 'react';
import type { CourseSummary } from '@/lib/types';
import { honeycombRows, type HoneycombRow } from '@/features/courses/honeycombLayout';
import { Text, Vector, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { studyLevels, styles } from './styles';

const HEXAGON = 'M7 0L14 4.25V12.75L7 17L0 12.75V4.25Z';
const CELLS_PER_GROUP = 48;

export function levelCounts(course: CourseSummary) {
  const counts = studyLevels.map(({ level }) =>
    Math.max(0, Math.floor(Number(course.study_distribution?.[level] || 0))),
  );
  const tracked = counts.reduce((total, count) => total + count, 0);
  const total = Math.max(0, Math.floor(Number(course.total_files || 0)));
  counts[0] = (counts[0] ?? 0) + Math.max(0, total - tracked);
  return counts;
}

function coverageGroups(counts: number[]) {
  const cells = counts.flatMap((count, level) => Array.from({ length: count }, () => level));
  const groups: HoneycombRow<number>[][] = [];
  for (let index = 0; index < cells.length; index += CELLS_PER_GROUP) {
    groups.push(honeycombRows(cells.slice(index, index + CELLS_PER_GROUP), 4));
  }
  return groups;
}

function StudyCell({ level }: { level: number }) {
  const color = studyLevels[level]?.color ?? colors.study0;
  return <Vector path={HEXAGON} fill={color} stroke={colors.border} style={styles.cell} />;
}

function Honeycomb({ rows }: { rows: HoneycombRow<number>[] }) {
  return (
    <View style={styles.honeycomb}>
      {rows.map((row, rowIndex) => (
        <View key={rowIndex} style={{ ...styles.honeycombRow, marginLeft: row.offset * 7 }}>
          {row.cells.map((level, index) => (
            <StudyCell key={`${rowIndex}-${index}`} level={level} />
          ))}
        </View>
      ))}
    </View>
  );
}

/** One cell still represents one file; bounded groups wrap without changing the distribution. */
export const CourseCoverage = memo(function CourseCoverage({ course }: { course: CourseSummary }) {
  const counts = useMemo(() => levelCounts(course), [course]);
  const groups = useMemo(() => coverageGroups(counts), [counts]);
  const total = counts.reduce((sum, count) => sum + count, 0);
  const summary = counts.map((count, level) => `${count} at ${level * 25}%`).join(' · ');
  return (
    <View id={`course-${course.id}-coverage`} style={styles.coverage}>
      <Text style={styles.studyHeading}>
        STUDY STATUS · {total} {total === 1 ? 'FILE' : 'FILES'}
      </Text>
      {total ? (
        <View style={styles.honeycombs}>
          {groups.map((rows, index) => (
            <Honeycomb key={index} rows={rows} />
          ))}
        </View>
      ) : (
        <Text style={styles.summary}>No study files</Text>
      )}
      {total > 0 && <Text style={styles.summary}>{summary}</Text>}
    </View>
  );
});

export function CoverageLegend() {
  return (
    <View id="courses-coverage-legend" style={styles.legend}>
      <Text style={styles.description}>
        Study status: one hexagon per study file; color shows its study level. Ignored files are excluded.
      </Text>
      <View style={styles.legendLevels}>
        {studyLevels.map(({ level, label }) => (
          <View key={level} style={styles.legendItem}>
            <StudyCell level={level} />
            <Text style={styles.summary}>{label}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}
