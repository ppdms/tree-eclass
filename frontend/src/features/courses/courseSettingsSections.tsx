import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ExternalLink, EyeOff, RefreshCw, Save, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { Input } from '@/components/ui/input';
import type { CourseSummary } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  courseSettingsCard: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '1rem',
    paddingInline: '1rem',
    backgroundColor: colors.surface,
  },
  courseSettingsProfile: {
    gridColumn: {
      default: '1 / -1',
      [media.tablet]: 'auto',
    },
  },
  courseSettingsCardHeader: {
    gap: '1rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '.85rem',
  },
  courseSettingsCardHeading: {
    gap: '.15rem',
    display: 'grid',
  },
  courseSettingsCardTitle: {
    fontSize: '.85rem',
    fontWeight: 600,
  },
  courseSettingsCardSub: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  courseSourceDetails: {
    marginTop: '.75rem',
  },
  courseSourceSummary: {
    color: colors.textSecondary,
    cursor: 'pointer',
    fontSize: typography.sizeXs,
  },
  courseSourceDl: {
    margin: 0,
    padding: '.8rem',
    borderRadius: '.5rem',
    gap: '.6rem',
    backgroundColor: colors.background,
    display: 'grid',
    marginTop: '.8rem',
  },
  courseSourceRow: {
    gap: '.75rem',
    display: 'grid',
    gridTemplateColumns: '5rem minmax(0, 1fr)',
  },
  courseSourceDt: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  courseSourceDd: {
    margin: 0,
    fontSize: typography.sizeXs,
    overflowWrap: 'anywhere',
    minWidth: 0,
  },
  courseSourceLink: {
    gap: '.35rem',
    alignItems: 'center',
    display: 'inline-flex',
  },
  courseNameField: {
    gap: '.5rem',
    alignItems: {
      default: 'center',
      [media.mobile]: 'stretch',
    },
    display: 'flex',
    flexDirection: {
      default: 'row',
      [media.mobile]: 'column',
    },
    maxWidth: '38rem',
  },
  courseSettingsActions: {
    gap: '.5rem',
    display: 'flex',
    flexWrap: 'wrap',
  },
  courseDangerZone: {
    borderColor: `color-mix(in srgb, ${colors.danger} 28%, ${colors.border})`,
  },
});

export interface CourseConfirmation {
  action: string;
  message: string;
  target: string;
}

function CourseSourceDetails({ course }: { course: CourseSummary }) {
  return (
    <details {...stylex.props(styles.courseSourceDetails)}>
      <summary {...stylex.props(styles.courseSourceSummary)}>Source details</summary>
      <dl {...stylex.props(styles.courseSourceDl)}>
        <div {...stylex.props(styles.courseSourceRow)}>
          <dt {...stylex.props(styles.courseSourceDt)}>Course ID</dt>
          <dd {...stylex.props(styles.courseSourceDd)}>{course.id}</dd>
        </div>
        <div {...stylex.props(styles.courseSourceRow)}>
          <dt {...stylex.props(styles.courseSourceDt)}>Folder</dt>
          <dd {...stylex.props(styles.courseSourceDd)}>
            <code>{course.webdav_folder}</code>
          </dd>
        </div>
        <div {...stylex.props(styles.courseSourceRow)}>
          <dt {...stylex.props(styles.courseSourceDt)}>eClass</dt>
          <dd {...stylex.props(styles.courseSourceDd)}>
            <a
              href={`https://eclass.aueb.gr/modules/document/index.php?course=INF${course.id}`}
              target="_blank"
              {...stylex.props(styles.courseSourceLink)}
              rel="noopener noreferrer"
            >
              Open course <ExternalLink />
            </a>
          </dd>
        </div>
      </dl>
    </details>
  );
}

export function CourseRenameForm({
  course,
  busy,
  onSubmit,
}: {
  course: CourseSummary;
  busy: boolean;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form
      {...stylex.props(styles.courseSettingsCard, styles.courseSettingsProfile)}
      onSubmit={onSubmit}
    >
      <header {...stylex.props(styles.courseSettingsCardHeader)}>
        <div {...stylex.props(styles.courseSettingsCardHeading)}>
          <strong {...stylex.props(styles.courseSettingsCardTitle)}>Course name</strong>
          <span {...stylex.props(styles.courseSettingsCardSub)}>Used throughout your workspace</span>
        </div>
      </header>
      <div {...stylex.props(styles.courseNameField)}>
        <Input name="name" required defaultValue={course.name} />
        <Button type="submit" disabled={busy} icon={<Save aria-hidden="true" />}>
          {busy ? 'Saving…' : 'Save'}
        </Button>
      </div>
      <CourseSourceDetails course={course} />
    </form>
  );
}

export function CourseActionButtons({
  course,
  busy,
  onAction,
}: {
  course: CourseSummary;
  busy: boolean;
  onAction: (action: string, message: string, target: string) => void;
}) {
  return (
    <section {...stylex.props(styles.courseSettingsCard)}>
      <header {...stylex.props(styles.courseSettingsCardHeader)}>
        <div {...stylex.props(styles.courseSettingsCardHeading)}>
          <strong {...stylex.props(styles.courseSettingsCardTitle)}>Sync & indexing</strong>
          <span {...stylex.props(styles.courseSettingsCardSub)}>Refresh or rebuild this course's local data</span>
        </div>
      </header>
      <div {...stylex.props(styles.courseSettingsActions)}>
        <Button
          variant="outline"
          onClick={() => onAction('check', `Run a fresh check for ${course.name}?`, `/courses/${course.id}`)}
          disabled={busy}
        >
          <RefreshCw /> Check now
        </Button>
        <Button
          variant="ghost"
          onClick={() =>
            onAction(
              'reset',
              `Reset the file tree and change history for ${course.name}? Course settings will stay.`,
              `/courses/${course.id}`,
            )
          }
          disabled={busy}
        >
          Reset index
        </Button>
      </div>
    </section>
  );
}

export function CourseDangerButtons({
  course,
  busy,
  onAction,
}: {
  course: CourseSummary;
  busy: boolean;
  onAction: (action: string, message: string, target: string) => void;
}) {
  return (
    <section {...stylex.props(styles.courseSettingsCard, styles.courseDangerZone)}>
      <header {...stylex.props(styles.courseSettingsCardHeader)}>
        <CourseDangerHeading />
      </header>
      <div {...stylex.props(styles.courseSettingsActions)}>
        <Button
          variant="outline"
          onClick={() =>
            onAction(
              'hide',
              `Hide ${course.name}? Its files stay synchronized but leave the daily workspace.`,
              '/courses',
            )
          }
          disabled={busy}
        >
          <EyeOff /> Hide course
        </Button>
        <Button
          variant="destructive"
          onClick={() =>
            onAction(
              'delete',
              `Delete ${course.name} permanently? This removes its synchronized data, ` +
                'including your notes and highlights, and cannot be undone.',
              '/courses',
            )
          }
          disabled={busy}
        >
          <Trash2 /> Delete course
        </Button>
      </div>
    </section>
  );
}

function CourseDangerHeading() {
  return (
    <div {...stylex.props(styles.courseSettingsCardHeading)}>
      <strong {...stylex.props(styles.courseSettingsCardTitle)}>Course access</strong>
      <span {...stylex.props(styles.courseSettingsCardSub)}>Hide it from the workspace or remove it completely</span>
    </div>
  );
}

function confirmTitle(action?: string): string {
  if (action === 'delete') return 'Delete course?';
  if (action === 'hide') return 'Hide course?';
  if (action === 'reset') return 'Reset course data?';
  return 'Run course check?';
}

function confirmLabel(action?: string): string {
  if (action === 'delete') return 'Delete course';
  if (action === 'hide') return 'Hide course';
  if (action === 'reset') return 'Reset data';
  return 'Run check';
}

export function CourseSettingsConfirm({
  confirmation,
  onCancel,
  onConfirm,
}: {
  confirmation: CourseConfirmation | null;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <ConfirmDialog
      open={Boolean(confirmation)}
      title={confirmTitle(confirmation?.action)}
      description={confirmation?.message || ''}
      confirmLabel={confirmLabel(confirmation?.action)}
      danger={confirmation?.action === 'delete'}
      onCancel={onCancel}
      onConfirm={onConfirm}
    />
  );
}
