package messages

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/knowledge"
)

func visible(ctx context.Context, tx pgx.Tx, requested []int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `SELECT id FROM app.courses WHERE hidden=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, err
	}
	if len(requested) == 0 {
		return ids, nil
	}
	result := []int64{}
	for _, id := range requested {
		if !slices.Contains(ids, id) {
			return nil, knowledge.ErrUnavailable
		}
		if !slices.Contains(result, id) {
			result = append(result, id)
		}
	}
	slices.Sort(result)
	return result, nil
}

type CourseStatus struct {
	CourseID      int64   `json:"course_id"`
	Messages      int64   `json:"messages"`
	Conversations int64   `json:"conversations"`
	Sources       int64   `json:"sources"`
	FailedSources int64   `json:"failed_sources"`
	Latest        *string `json:"latest_message_at"`
}
type Status struct {
	Courses   []CourseStatus   `json:"courses"`
	Totals    map[string]int64 `json:"totals"`
	Mapped    []int64          `json:"mapped_courses"`
	Available bool             `json:"available"`
	Latest    *string          `json:"archive_indexed_through"`
	Notice    string           `json:"untrusted_content_notice"`
}

func (s Reader) Status(ctx context.Context, requested []int64) (Status, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Status{}, err
	}
	defer tx.Rollback(ctx)
	ids, err := visible(ctx, tx, requested)
	if err != nil {
		return Status{}, err
	}
	result, err := statusTx(ctx, tx, ids)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func statusTx(ctx context.Context, tx pgx.Tx, ids []int64) (Status, error) {
	result := Status{
		Courses: []CourseStatus{},
		Totals:  map[string]int64{"messages": 0, "conversations": 0, "sources": 0, "failed_sources": 0},
		Mapped:  []int64{},
		Notice:  CommunityNotice,
	}
	rows, err := tx.Query(ctx, `WITH sources AS MATERIALIZED (
 SELECT a.* FROM messages.archive_sources a JOIN app.discord_course_channels m ON m.root_channel_id=a.root_id AND m.course_id=a.course_id WHERE a.course_id=ANY($1::bigint[])
), message_counts AS (
 SELECT m.course_id,count(*) n,max(m.timestamp) latest FROM messages.messages m JOIN sources s ON s.path=m.source_path AND s.course_id=m.course_id AND s.status='ready' GROUP BY m.course_id
), conversation_counts AS (
 SELECT c.course_id,count(*) n FROM messages.conversations c JOIN sources s ON s.path=c.source_path AND s.course_id=c.course_id AND s.root_id=c.root_id::text AND s.status='ready' GROUP BY c.course_id
), source_counts AS (
 SELECT course_id,count(*) n,count(*) FILTER(WHERE status='failed') failed FROM sources GROUP BY course_id
), mapped AS (SELECT DISTINCT course_id FROM app.discord_course_channels WHERE course_id=ANY($1::bigint[]))
 SELECT m.course_id,coalesce(mc.n,0),coalesce(cc.n,0),coalesce(sc.n,0),coalesce(sc.failed,0),mc.latest
 FROM mapped m LEFT JOIN message_counts mc USING(course_id) LEFT JOIN conversation_counts cc USING(course_id) LEFT JOIN source_counts sc USING(course_id) ORDER BY m.course_id`, ids)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var c CourseStatus
		if err = rows.Scan(&c.CourseID, &c.Messages, &c.Conversations, &c.Sources, &c.FailedSources, &c.Latest); err != nil {
			return result, err
		}
		result.Courses = append(result.Courses, c)
		result.Mapped = append(result.Mapped, c.CourseID)
		result.Totals["messages"] += c.Messages
		result.Totals["conversations"] += c.Conversations
		result.Totals["sources"] += c.Sources
		result.Totals["failed_sources"] += c.FailedSources
		if c.Latest != nil && (result.Latest == nil || *c.Latest > *result.Latest) {
			result.Latest = c.Latest
		}
	}
	result.Available = len(result.Mapped) > 0
	return result, rows.Err()
}
