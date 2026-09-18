import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { Center } from '@/components/ui/layout';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import type { ChangeDetailPayload, ChangeItem } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { ChangeDetailHeader, ChangedFilesSection } from './changeDetailParts';

const styles = stylex.create({
  changeDetailPage: {
    marginInline: 'auto',
    marginBottom: '3rem',
    maxWidth: '52rem',
    minWidth: 0,
    paddingTop: '0.5rem',
    width: '100%',
  },
  courseEmpty: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'dashed',
    borderWidth: 1,
    marginBlock: '1.25rem',
    marginInline: 'auto',
    paddingBlock: '2.5rem',
    paddingInline: '1.5rem',
    backgroundColor: colors.surface,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    textAlign: 'center',
    maxWidth: '38rem',
  },
  errorLink: {
    textDecoration: {
      default: 'underline',
      ':hover': 'none',
    },
    color: {
      default: colors.info,
      ':hover': colors.infoStrong,
    },
  },
  loadingText: {
    margin: 0,
    paddingBlock: '3rem',
    color: colors.textSecondary,
    fontSize: typography.sizeBase,
    textAlign: 'center',
  },
});

export interface ChangeDetailPageProps {
  courseId: string;
  changeNo: string;
  initialData?: ChangeDetailPayload | null;
}

interface ChangeDetailState {
  data: ChangeDetailPayload | null;
  error: string | null;
}

function useChangeDetailData(
  courseId: string,
  changeNo: string,
  initialData: ChangeDetailPayload | null,
): ChangeDetailState {
  const [data, setData] = React.useState<ChangeDetailPayload | null>(initialData);
  const [error, setError] = React.useState<string | null>(null);
  React.useEffect(() => {
    // The route loader already loaded the change into initialData; only fetch when the
    // route rendered without it (plain client navigation).
    if (initialData) return;
    setData(null);
    setError(null);
    fetchJson<ChangeDetailPayload>(
      `api/courses/${encodeURIComponent(courseId)}/changes/${encodeURIComponent(changeNo)}`,
    )
      .then(setData)
      .catch((loadError) => {
        const parsed = errorLikeSchema.safeParse(loadError);
        const errorLike = parsed.success ? parsed.data : { message: String(loadError) };
        setError(errorMessage(errorLike, 'Could not load this change'));
      });
  }, [courseId, changeNo, initialData]);
  return { data, error };
}

function ChangeDetailError({ courseId }: { courseId: string }) {
  return (
    <div {...stylex.props(styles.changeDetailPage)}>
      <p {...stylex.props(styles.courseEmpty)} role="alert">
        Could not load this change.{' '}
        <a {...stylex.props(styles.errorLink)} href={`/courses/${courseId}`}>
          Return to the course
        </a>
        .
      </p>
    </div>
  );
}

function ChangeDetailLoading() {
  return (
    <div {...stylex.props(styles.changeDetailPage)}>
      <Center role="status">
        <p {...stylex.props(styles.loadingText)}>Opening change details…</p>
      </Center>
    </div>
  );
}

function ChangeDetailBody({
  courseId,
  changeNo,
  data,
}: {
  courseId: string;
  changeNo: string;
  data: ChangeDetailPayload;
}) {
  const changes: ChangeItem[] = data.changes || [];
  const record = data.change_record || {};
  return (
    <div {...stylex.props(styles.changeDetailPage)}>
      <ChangeDetailHeader
        courseId={courseId}
        courseName={data.course?.name || 'Course'}
        changeNo={changeNo}
        record={record}
        changes={changes}
        webdavFolder={data.webdav_folder}
      />
      <ChangedFilesSection changes={changes} webdavFolder={data.webdav_folder} />
      <a {...stylex.props(buttonStyles.base, buttonStyles.secondary)} href={`/courses/${courseId}`}>
        Back to course
      </a>
    </div>
  );
}

export default function ChangeDetailPage({ courseId, changeNo, initialData = null }: ChangeDetailPageProps) {
  const { data, error } = useChangeDetailData(courseId, changeNo, initialData);
  if (error) return <ChangeDetailError courseId={courseId} />;
  if (!data) return <ChangeDetailLoading />;
  return <ChangeDetailBody courseId={courseId} changeNo={changeNo} data={data} />;
}
