import * as stylex from '@stylexjs/stylex';
import { useEffect, useRef, useState, type KeyboardEvent, type MouseEvent, type PointerEvent } from 'react';
import { Icon } from '@/components/Icon';
import type { CourseSummary } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { CourseHoneycomb } from './CourseHoneycomb';
import { useCourseOrder } from './useCourseOrder';

const styles = stylex.create({
  courseShelfWrapper: {
    gap: '0.75rem',
    display: 'flex',
    flexDirection: 'column',
  },
  courseControlsRow: {
    gap: '1rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'space-between',
    marginBlockEnd: '.25rem',
  },
  courseSearchWrapper: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.5rem',
    paddingBlock: '0.35rem',
    paddingInline: '0.65rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    boxSizing: 'border-box',
    color: colors.textSecondary,
    display: 'inline-flex',
    maxWidth: '20rem',
    width: '100%',
  },
  courseSearchInput: {
    borderStyle: 'none',
    outline: 'none',
    backgroundColor: 'transparent',
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    lineHeight: 1.4,
    width: '100%',
    '::placeholder': {
      color: colors.textSecondary,
    },
  },
  courseOrderStatus: {
    marginBlock: 0,
    marginInline: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    textAlign: 'right',
  },
  courseOrderStatusError: {
    color: colors.danger,
  },
  courseCardGrid: {
    gap: {
      default: '1rem',
      [media.tabletOnly]: '1.125rem',
    },
    listStyle: 'none',
    marginBlock: 0,
    marginInline: 0,
    paddingBlock: 0,
    paddingInline: 0,
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(3, minmax(0, 1fr))',
      [media.tabletOnly]: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  courseCardItem: {
    display: 'flex',
    position: 'relative',
    transitionDuration: '140ms',
    transitionProperty: 'opacity, transform',
    minWidth: 0,
  },
  courseCardItemDragging: {
    opacity: 0.72,
    transform: 'scale(0.985)',
    zIndex: 2,
  },
  courseCard: {
    padding: {
      default: '1rem',
      [media.tabletOnly]: '1.75rem',
      [media.mobile]: '1.125rem',
    },
    borderColor: { default: colors.border, ':hover': colors.borderLight },
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    textDecoration: 'none',
    backgroundColor: { default: colors.surface, ':hover': colors.surfaceRaised },
    boxSizing: 'border-box',
    color: colors.textPrimary,
    cursor: 'grab',
    display: 'flex',
    flexDirection: 'column',
    transitionDuration: '180ms',
    transitionProperty: 'background-color, border-color',
    height: {
      default: '10rem',
      [media.tabletOnly]: 'auto',
      [media.mobile]: 'auto',
    },
    minHeight: {
      default: '10rem',
      [media.tabletOnly]: '11rem',
      [media.mobile]: '10rem',
    },
    width: '100%',
  },
  courseCardHeading: {
    margin: 0,
    display: 'block',
  },
  courseCardTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: {
      default: typography.sizeXl,
      [media.tabletOnly]: typography.size2xl,
      [media.mobile]: typography.size2xl,
    },
    fontWeight: 600,
    letterSpacing: '-0.025em',
    lineHeight: 1.3,
    overflowWrap: 'anywhere',
    textWrap: 'balance',
    maxWidth: '28ch',
  },
  courseSearchEmpty: {
    margin: 0,
    paddingBlock: '2rem',
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    textAlign: 'center',
  },
});

function useCourseHoneycombRows() {
  const titleRef = useRef<HTMLHeadingElement>(null);
  const [rowCount, setRowCount] = useState(4);

  useEffect(() => {
    const title = titleRef.current;
    if (!title) return;
    const measure = () => {
      const lineHeight = Number.parseFloat(window.getComputedStyle(title).lineHeight);
      const lines = Math.max(1, Math.round(title.getBoundingClientRect().height / lineHeight));
      setRowCount(lines > 1 ? 3 : 4);
    };
    const observer = new ResizeObserver(measure);
    observer.observe(title);
    measure();
    return () => observer.disconnect();
  }, []);

  return { rowCount, titleRef };
}

interface CourseCardProps {
  course: CourseSummary;
  position: number;
  total: number;
  dragging: boolean;
  saving: boolean;
  onDragChange: (courseId: string | null) => void;
  onMove: (sourceId: string, targetId: string) => void;
  onCommit: () => void;
  onCancel: () => void;
}

function useCourseDrag(props: CourseCardProps) {
  const pointer = useRef({ id: -1, x: 0, y: 0, moved: false });
  const suppressClick = useRef(false);
  const courseId = String(props.course.id);
  const onPointerDown = (event: PointerEvent<HTMLAnchorElement>) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      props.saving ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey ||
      event.altKey
    )
      return;
    suppressClick.current = false;
    pointer.current = { id: event.pointerId, x: event.clientX, y: event.clientY, moved: false };
    event.currentTarget.setPointerCapture(event.pointerId);
  };
  const onPointerMove = (event: PointerEvent<HTMLAnchorElement>) => {
    if (pointer.current.id !== event.pointerId) return;
    const distance = Math.hypot(event.clientX - pointer.current.x, event.clientY - pointer.current.y);
    if (!pointer.current.moved && distance < 6) return;
    pointer.current.moved = true;
    suppressClick.current = true;
    event.preventDefault();
    props.onDragChange(courseId);
    const target = document.elementFromPoint(event.clientX, event.clientY)?.closest<HTMLElement>('[data-course-id]');
    if (target?.dataset.courseId) props.onMove(courseId, target.dataset.courseId);
  };
  const finish = (cancelled = false) => {
    if (pointer.current.moved) (cancelled ? props.onCancel : props.onCommit)();
    pointer.current = { id: -1, x: 0, y: 0, moved: false };
    props.onDragChange(null);
  };
  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    if (!suppressClick.current) return;
    event.preventDefault();
    suppressClick.current = false;
  };
  return { onPointerDown, onPointerMove, finish, onClick };
}

function CourseCard(props: CourseCardProps) {
  const { course, position, total, dragging } = props;
  const { rowCount, titleRef } = useCourseHoneycombRows();
  const drag = useCourseDrag(props);
  const onKeyDown = (event: KeyboardEvent<HTMLAnchorElement>) => {
    if (props.saving) return;
    if (event.key === 'Escape' && dragging) {
      event.preventDefault();
      drag.finish(true);
      return;
    }
    const offset = ['ArrowLeft', 'ArrowUp'].includes(event.key) ? -1 : 1;
    if (!['ArrowLeft', 'ArrowUp', 'ArrowRight', 'ArrowDown'].includes(event.key)) return;
    const targetPosition = Math.max(0, Math.min(total - 1, position - 1 + offset));
    const target = event.currentTarget.closest('ol')?.children.item(targetPosition);
    if (!(target instanceof HTMLElement)) return;
    if (!target.dataset.courseId || target.dataset.courseId === String(course.id)) return;
    event.preventDefault();
    props.onMove(String(course.id), target.dataset.courseId);
    props.onCommit();
  };
  return (
    <li {...stylex.props(styles.courseCardItem, dragging && styles.courseCardItemDragging)} data-course-id={course.id}>
      <a
        {...stylex.props(styles.courseCard)}
        href={`/courses/${course.id}`}
        lang="el"
        draggable="false"
        aria-keyshortcuts="ArrowUp ArrowDown ArrowLeft ArrowRight"
        onKeyDown={onKeyDown}
        onPointerDown={drag.onPointerDown}
        onPointerMove={drag.onPointerMove}
        onPointerUp={() => drag.finish()}
        onPointerCancel={() => drag.finish(true)}
        onLostPointerCapture={() => drag.finish()}
        onClick={drag.onClick}
      >
        <span {...stylex.props(styles.courseCardHeading)}>
          <h2 ref={titleRef} {...stylex.props(styles.courseCardTitle)}>
            {course.name}
          </h2>
        </span>
        <CourseHoneycomb course={course} rowCount={rowCount} />
      </a>
    </li>
  );
}

function CourseShelfControls({
  search,
  onSearchChange,
  status,
  isError,
}: {
  search: string;
  onSearchChange: (value: string) => void;
  status: string;
  isError: boolean;
}) {
  return (
    <div {...stylex.props(styles.courseControlsRow)}>
      <div {...stylex.props(styles.courseSearchWrapper)}>
        <Icon name="search" aria-hidden="true" />
        <input
          type="search"
          value={search}
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder="Filter courses…"
          aria-label="Filter courses"
          {...stylex.props(styles.courseSearchInput)}
        />
      </div>
      {status && (
        <p
          {...stylex.props(styles.courseOrderStatus, isError && styles.courseOrderStatusError)}
          role={isError ? 'alert' : 'status'}
          aria-live="polite"
        >
          {status}
        </p>
      )}
    </div>
  );
}

function filterCourses(courses: CourseSummary[], query: string): CourseSummary[] {
  if (!query) return courses;
  return courses.filter((course) => {
    const name = (course.name || '').toLowerCase();
    const code = (course.code || '').toLowerCase();
    return name.includes(query) || code.includes(query);
  });
}

interface CourseCardListProps {
  courses: CourseSummary[];
  draggingId: string | null;
  saving: boolean;
  onDragChange: (courseId: string | null) => void;
  onMove: (sourceId: string, targetId: string) => void;
  onCommit: () => void;
  onCancel: () => void;
}

function CourseCardList({
  courses,
  draggingId,
  saving,
  onDragChange,
  onMove,
  onCommit,
  onCancel,
}: CourseCardListProps) {
  return (
    <ol {...stylex.props(styles.courseCardGrid)} id="course-list" aria-label="Active courses">
      {courses.map((course, index) => (
        <CourseCard
          key={course.id}
          course={course}
          position={index + 1}
          total={courses.length}
          dragging={draggingId === String(course.id)}
          saving={saving}
          onDragChange={onDragChange}
          onMove={onMove}
          onCommit={onCommit}
          onCancel={onCancel}
        />
      ))}
    </ol>
  );
}

export function CourseShelf({ courses }: { courses: CourseSummary[] }) {
  const { ordered, previewMove, commit, cancel, saveState } = useCourseOrder(courses);
  const [draggingId, setDraggingId] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const status = {
    idle: '',
    saving: 'Saving course order…',
    saved: 'Course order saved',
    error: 'Could not save the new course order. The previous order was restored.',
  }[saveState];

  const visibleCourses = filterCourses(ordered, search.trim().toLowerCase());

  return (
    <div {...stylex.props(styles.courseShelfWrapper)}>
      <CourseShelfControls search={search} onSearchChange={setSearch} status={status} isError={saveState === 'error'} />
      {visibleCourses.length ? (
        <CourseCardList
          courses={visibleCourses}
          draggingId={draggingId}
          saving={saveState === 'saving'}
          onDragChange={setDraggingId}
          onMove={previewMove}
          onCommit={commit}
          onCancel={cancel}
        />
      ) : (
        <p {...stylex.props(styles.courseSearchEmpty)}>No courses match &ldquo;{search}&rdquo;.</p>
      )}
    </div>
  );
}
