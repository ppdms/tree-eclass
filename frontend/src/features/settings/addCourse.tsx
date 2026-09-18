import { navigate } from '@/app/navigation';
import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { z } from 'zod/v4';
import { buttonStyles } from '@/components/ui/styles';
import type { SettingsPayload } from './types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { Field } from './fields';

const styles = stylex.create({
  settingsMatrix: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  settingsSectionDescription: {
    marginBlock: '0.35rem',
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  settingsFormGrid: {
    gap: '1rem',
    display: 'grid',
  },
  settingsFormSuccess: {
    color: colors.success,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
  settingsFormError: {
    color: colors.danger,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
});

export function folderLabel(path: string | null | undefined): string | null | undefined {
  return (
    String(path || '')
      .split('/')
      .filter(Boolean)
      .pop() || path
  );
}

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

interface AddCourseResult {
  ok: boolean;
  message?: string;
}

async function postAddCourse(form: HTMLFormElement): Promise<AddCourseResult> {
  const response = await fetch('/courses/add', {
    method: 'POST',
    body: new FormData(form),
    headers: { Accept: 'application/json' },
  });
  if (response.ok) return { ok: true };
  let message = `Could not add course (HTTP ${response.status}).`;
  try {
    const parsed = errorBodySchema.safeParse(await response.json());
    if (parsed.success) {
      const detail = parsed.data.detail;
      if (Array.isArray(detail)) {
        message = detail
          .map((item) => {
            const parsedItem = detailItemSchema.safeParse(item);
            return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
          })
          .join('. ');
      } else if (detail) {
        message = String(detail);
      }
    }
  } catch {
    /* non-JSON error body */
  }
  return { ok: false, message };
}

interface AddCourseFeedback {
  ok: boolean;
  message: string;
}

function useAddCourse(data: SettingsPayload) {
  const [busy, setBusy] = React.useState(false);
  const [feedback, setFeedback] = React.useState<AddCourseFeedback | null>(null);
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const courseId = String(formData.get('course_id') || '').trim();
    const name = String(formData.get('name') || '').trim();
    if (!courseId || !name) return;
    const existing = (data.courses || []).find((course) => String(course.id) === courseId);
    if (existing) {
      setFeedback({
        ok: false,
        message: `Course ${courseId} already exists as ${existing.name}. Open it from Courses to rename it.`,
      });
      return;
    }
    setBusy(true);
    setFeedback(null);
    try {
      const result = await postAddCourse(form);
      if (result.ok) {
        navigate('/courses');
        return;
      }
      setFeedback({ ok: false, message: result.message || 'Could not add course.' });
    } catch {
      setFeedback({
        ok: false,
        message: 'Could not reach the server. Check the connection and try again.',
      });
    } finally {
      setBusy(false);
    }
  };
  return { busy, feedback, submit };
}

export interface AddCourseFormProps {
  data: SettingsPayload;
}

function AddCourseFields() {
  return (
    <>
      <div {...stylex.props(styles.settingsMatrix)}>
        <Field
          label="Course ID"
          name="course_id"
          type="number"
          required
          placeholder="161"
          hint="Numeric ID from the eClass URL (INF…), for example 161."
        />
        <Field label="Course name" name="name" required />
      </div>
      <p {...stylex.props(styles.settingsSectionDescription)}>Files are synchronized during the next course check.</p>
    </>
  );
}

export function AddCourseForm({ data }: AddCourseFormProps) {
  const { busy, feedback, submit } = useAddCourse(data);
  const [ready, setReady] = React.useState(false);
  return (
    <form
      method="POST"
      action="/courses/add"
      onSubmit={submit}
      onInput={(event) => {
        const form = event.currentTarget;
        const courseId = String(new FormData(form).get('course_id') || '').trim();
        const name = String(new FormData(form).get('name') || '').trim();
        setReady(Boolean(courseId && name));
      }}
      {...stylex.props(styles.settingsFormGrid)}
    >
      <AddCourseFields />
      {feedback && (
        <p
          {...stylex.props(feedback.ok ? styles.settingsFormSuccess : styles.settingsFormError)}
          role={feedback.ok ? 'status' : 'alert'}
        >
          {feedback.message}
        </p>
      )}
      <button {...stylex.props(buttonStyles.base, buttonStyles.primary)} type="submit" disabled={busy || !ready}>
        {busy ? 'Adding course…' : 'Add course'}
      </button>
    </form>
  );
}
