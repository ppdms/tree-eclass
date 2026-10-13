import type { CoursePracticeQuestion, PracticeOutcome } from './model';
import { Button } from '../ui/controls';
import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { semanticId, styles } from '../roadmap/styles';

const STATE_LABELS = {
  needs_work: 'Needs work',
  unattempted: 'New',
  review: 'One more pass',
  learned: 'Learned',
};

function isKnownPracticeState(state: string): state is keyof typeof STATE_LABELS {
  return Object.hasOwn(STATE_LABELS, state);
}

function practiceStateLabel(state: CoursePracticeQuestion['state']): string {
  return state && isKnownPracticeState(state) ? STATE_LABELS[state] : state || 'New';
}

const GRADE_OPTIONS: { value: PracticeOutcome; label: string }[] = [
  { value: 'correct', label: 'Got it' },
  { value: 'partial', label: 'Partly' },
  { value: 'incorrect', label: 'Not yet' },
  { value: 'skipped', label: 'Skip' },
];

interface CardProps {
  id: string;
  question: CoursePracticeQuestion;
  index: number;
  queueLength: number;
  revealed: boolean;
  busy: boolean;
  disabled: boolean;
  note: string;
  onReveal: () => void;
  onGrade: (outcome: PracticeOutcome) => void;
  onNote: (value: string) => void;
  onNext: () => void;
}

function QuestionMeta({ question }: { question: CoursePracticeQuestion }) {
  const metadata = [
    String(question.response_mode || '').replaceAll('_', ' '),
    question.difficulty,
    question.estimated_minutes ? `~${question.estimated_minutes} min` : '',
    question.attempts ? `${question.attempts} attempt${question.attempts === 1 ? '' : 's'}` : 'never attempted',
  ]
    .filter(Boolean)
    .join(' · ');
  return (
    <View style={styles.stack}>
      <Text style={styles.meta}>
        {question.unit_title || question.unit_key} · {practiceStateLabel(question.state)}
      </Text>
      <Text style={styles.heading}>{question.prompt || ''}</Text>
      <Text style={styles.meta}>{metadata}</Text>
    </View>
  );
}

function AnswerList({ title, items }: { title: string; items?: string[] }) {
  if (!items?.length) return null;
  return (
    <View style={{ ...styles.stack, gap: 7 }}>
      <Text style={styles.heading}>{title}</Text>
      {items.map((item) => (
        <Text key={item} style={styles.body}>
          • {item}
        </Text>
      ))}
    </View>
  );
}

function AnswerControls(props: CardProps) {
  if (!props.revealed)
    return (
      <Button id={`${props.id}:reveal`} onClick={props.onReveal}>
        Show answer
      </Button>
    );
  return (
    <View style={styles.stack}>
      <Text style={styles.body}>{props.question.expected_answer || ''}</Text>
      <AnswerList title="Your answer should contain:" items={props.question.answer_points} />
      <AnswerList title="Common mistakes:" items={props.question.common_mistakes} />
      <Text style={styles.heading}>How did you do?</Text>
      <View style={styles.row}>
        {GRADE_OPTIONS.map(({ value, label }) => (
          <Button
            key={value}
            id={`${props.id}:grade:${value}`}
            disabled={props.busy || props.disabled}
            tone={value === 'correct' ? 'primary' : 'normal'}
            onClick={() => props.onGrade(value)}
          >
            {label}
          </Button>
        ))}
      </View>
      <Field
        id={`${props.id}:note`}
        label="Note"
        value={props.note}
        onChange={props.onNote}
        maxLength={4000}
        placeholder="What did you miss?"
      />
    </View>
  );
}

export function PracticeQuestionCard(props: CardProps) {
  const id = semanticId(props.id, props.question.unit_key, props.question.question_id || props.question.id);
  return (
    <View id={id} style={styles.action}>
      <QuestionMeta question={props.question} />
      <AnswerControls {...props} id={id} />
      <View style={styles.row}>
        <Button id={`${id}:next`} onClick={props.onNext}>
          Next question
        </Button>
        <Text style={styles.meta}>
          {props.index + 1} of {props.queueLength} in the queue
        </Text>
      </View>
    </View>
  );
}
