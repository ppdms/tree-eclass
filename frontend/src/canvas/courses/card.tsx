import type { CourseSummary } from '@/lib/types';
import { AppLink } from '../app/links';
import { Text, View } from '../ui/primitives';
import type { CanvasKey } from '../ui/types';
import { CourseCoverage } from './coverage';
import { styles } from './styles';

interface CourseCardProps {
  course: CourseSummary;
  position: number;
  total: number;
  dragging: boolean;
  saving: boolean;
  onDragStart: () => void;
  onDragOver: (sourceId: string) => void;
  onDragEnd: (cancelled: boolean) => void;
  onMove: (offset: number) => void;
}

interface MoveButtonProps {
  id: string;
  label: string;
  disabled: boolean;
  onClick: () => void;
}

function MoveButton({ id, label, disabled, onClick }: MoveButtonProps) {
  return (
    <View id={id} onClick={disabled ? undefined : onClick} style={{ ...styles.move, ...(disabled && styles.disabled) }}>
      <Text>{label}</Text>
    </View>
  );
}

function CourseMoves({ course, position, total, saving, onMove }: CourseCardProps) {
  return (
    <View style={styles.moves}>
      <Text style={styles.position}>
        Position {position} of {total}
      </Text>
      <MoveButton
        id={`course-${course.id}-earlier`}
        label="Move earlier"
        disabled={saving || position === 1}
        onClick={() => onMove(-1)}
      />
      <MoveButton
        id={`course-${course.id}-later`}
        label="Move later"
        disabled={saving || position === total}
        onClick={() => onMove(1)}
      />
    </View>
  );
}

export function CourseCard(props: CourseCardProps) {
  const { course, dragging, saving, onMove, onDragEnd } = props;
  const keyDown = ({ key }: CanvasKey) => {
    if (key === 'Escape' && dragging) {
      onDragEnd(true);
      return true;
    }
    if (saving) return;
    if (key === 'ArrowLeft' || key === 'ArrowUp') {
      onMove(-1);
      return true;
    }
    if (key === 'ArrowRight' || key === 'ArrowDown') {
      onMove(1);
      return true;
    }
  };
  return (
    <View
      id={`course-${course.id}`}
      dragId={saving ? undefined : String(course.id)}
      onDragStart={saving ? undefined : props.onDragStart}
      onDragOver={saving ? undefined : props.onDragOver}
      onDragEnd={props.onDragEnd}
      onKeyInput={keyDown}
      style={{ ...styles.card, ...(dragging && styles.dragging) }}
    >
      <AppLink block id={`course-${course.id}-link`} href={`/courses/${course.id}`} style={styles.cardLink}>
        <Text style={styles.courseName}>{course.name}</Text>
        {course.code && <Text style={styles.code}>{course.code}</Text>}
        <CourseCoverage course={course} />
      </AppLink>
      <CourseMoves {...props} />
    </View>
  );
}
