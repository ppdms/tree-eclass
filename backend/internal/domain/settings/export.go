package settings

import (
	"context"
	"io"
	"strings"
	"time"

	"tree-eclass/internal/infrastructure/rdbms"
)

// Export emits one consistent snapshot, keeping only one row or a small metadata
// page in memory. The HTTP layer completes a temporary artifact before delivery.
func (s Service) Export(ctx context.Context, out io.Writer) error {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var version int64
	if err = tx.QueryRow(ctx, `SELECT coalesce(max(version),0) FROM public.tree_go_migrations`).Scan(&version); err != nil {
		return err
	}
	prefs, err := readPreferences(ctx, tx)
	if err != nil {
		return err
	}
	planner, err := ReadPlanner(ctx, tx)
	if err != nil {
		return err
	}
	w := exportWriter{out: out, ctx: ctx, tx: tx}
	w.text(`{"format":"tree-eClass learner data export","version":2,"schema_engine":"goose","schema_version":`)
	w.value(version)
	w.text(`,"exported_at":`)
	w.value(time.Now().UTC().Format(time.RFC3339Nano))
	w.text(`,"courses":`)
	w.table(exportCourses, []string{"id", "name", "webdav_folder", "sort_order", "hidden", "short_name"})
	w.text(`,"settings":{"preferences":`)
	w.value(prefs)
	w.text(`,"planner":`)
	w.value(planner)
	w.text(`,"course_exam_plans":`)
	w.table(exportExamPlans, []string{"course_id", "course_name", "exam_at", "remaining_blocks", "importance", "max_daily_blocks", "enabled", "short_name", "commitment", "target_grade", "planning_notes"})
	w.text(`,"discord_course_channels":`)
	w.table(`SELECT root_channel_id,course_id,updated_at FROM app.discord_course_channels ORDER BY root_channel_id`, []string{"root_channel_id", "course_id", "updated_at"})
	w.text(`},"study":{`)
	for i, table := range learnerTables {
		if i > 0 {
			w.text(",")
		}
		name := table.Name
		if name == "study_annotations" {
			name = "annotations"
		}
		w.value(name)
		w.text(":")
		w.table("SELECT "+table.Columns+" FROM app."+table.Name+" ORDER BY 1", strings.Split(table.Columns, ","))
	}
	w.text(`},"conversations":`)
	w.conversations()
	w.text(
		`,"privacy":{"excluded":["credentials","webhook_config","webdav_config","app_data","discord_export_settings.token","provider_keys"],"note":"Secrets and session cookies are never included in exports."}}`,
	)
	if w.err != nil {
		return w.err
	}
	return tx.Commit(ctx)
}
