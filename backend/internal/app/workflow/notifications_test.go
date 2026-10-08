package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
	"tree-eclass/internal/infrastructure/rdbms"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/services/synchronization"
)

type fakeNotifications struct {
	Calls  int
	Bodies []string
	Fail   bool
	Quota  bool
}

func (s *fakeNotifications) Send(_ context.Context, _ string, content string) (time.Duration, error) {
	s.Calls++
	s.Bodies = append(s.Bodies, content)
	if s.Quota {
		return time.Minute, notifications.Failure{Status: 429}
	}
	if s.Fail {
		return 0, errors.New("synthetic transport failure")
	}
	return 0, nil
}
func TestNativeNotificationPublicationAndDelivery(t *testing.T) {
	t.Parallel()
	c := nativeSharedController(t)
	conn, _ := startTestStorage(t, c)
	defer conn.Close(t.Context())
	ctx := t.Context()
	nativePool, err := pgxpool.New(ctx, c.databaseURL())
	pool := rdbms.WrapPostgres(nativePool)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(101,'Αλγόριθμοι','/101');INSERT INTO app.webhook_config(id,webhook_url) VALUES(1,'https://example.test/private-token');INSERT INTO app.preferences(id,notification_enabled,notification_on_error) VALUES(1,1,0) ON CONFLICT(id) DO UPDATE SET notification_enabled=1,notification_on_error=0`); err != nil {
		t.Fatal(err)
	}
	sync := synchronization.Service{Pool: pool}
	items := []eclass.Announcement{
		{
			ID:          "one",
			Title:       "Ύλη",
			Description: "<p>Δένδρα</p><script>hidden</script>",
			Link:        "https://example.test/announcement",
		},
	}
	if err = sync.SaveAnnouncements(ctx, 101, items); err != nil {
		t.Fatal(err)
	}
	if err = sync.SaveAnnouncements(ctx, 101, items); err != nil {
		t.Fatal(err)
	}
	ex := []eclass.Exercise{{ID: "task", Title: "Εργασία", Link: "https://example.test/exercise"}}
	if err = sync.SaveExercises(ctx, 101, ex); err != nil {
		t.Fatal(err)
	}
	ex[0].Grade = "9"
	ex[0].MaxGrade = "10"
	if err = sync.SaveExercises(ctx, 101, ex); err != nil {
		t.Fatal(err)
	}
	if err = sync.SaveExercises(ctx, 101, ex); err != nil {
		t.Fatal(err)
	}
	if err = sync.Finish(ctx, synchronization.Result{}, errors.New("must not notify")); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = notifications.EnqueueTx(ctx, tx, notifications.Event{Key: "rolled-back", Header: "never send"}); err != nil {
		t.Fatal(err)
	}
	tx.Rollback(ctx)
	var count int64
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.notification_messages`).Scan(&count); err != nil ||
		count != 3 {
		t.Fatal("publication/repeat boundaries", count, err)
	}
	notificationDeliveryChecks(t, pool)
}

func notificationDeliveryChecks(t *testing.T, pool rdbms.Pool) {
	t.Helper()
	ctx := t.Context()
	var err error
	sender := &fakeNotifications{Quota: true}
	service := notifications.Service{Pool: pool, Sender: sender}
	if err = service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err = pool.QueryRow(ctx, `SELECT attempts FROM app.notification_messages WHERE available_at>clock_timestamp()`).Scan(&attempts); err != nil ||
		attempts != 0 {
		t.Fatal("quota spent retry budget", attempts, err)
	}
	if err = service.Tick(ctx); err != nil || sender.Calls != 1 {
		t.Fatal("destination cooldown was bypassed", sender.Calls, err)
	}
	sender.Quota = false
	if _, err = pool.Exec(ctx, `UPDATE app.notification_messages SET available_at='epoch';UPDATE app.notification_limits SET next_at='epoch'`); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err = service.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	status, err := service.Status(ctx)
	if err != nil || status.Counts["sent"] != 3 {
		t.Fatal("delivery completion", status, err)
	}
	if err = service.Tick(ctx); err != nil || sender.Calls != 4 {
		t.Fatal("completed messages resent", sender.Calls, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.notification_messages SET status='pending';UPDATE app.webhook_config SET webhook_url='https://example.test/new-destination'`); err != nil {
		t.Fatal(err)
	}
	if err = service.Tick(ctx); err != nil || sender.Calls != 4 {
		t.Fatal("queued private data sent to new destination", err)
	}
	status, err = service.Status(ctx)
	if err != nil || status.Counts["canceled"] != 3 {
		t.Fatal(status, err)
	}
	notificationRetryChecks(t, pool, service, sender)
}

func notificationRetryChecks(
	t *testing.T,
	pool rdbms.Pool,
	service notifications.Service,
	sender *fakeNotifications,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = notifications.EnqueueTx(ctx, tx, notifications.Event{Key: "retry", Header: "Retry fixture"}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	sender.Fail = true
	for range 5 {
		if err = service.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `UPDATE app.notification_messages SET available_at='epoch' WHERE status='pending'`); err != nil {
			t.Fatal(err)
		}
	}
	status, err := service.Status(ctx)
	if err != nil || status.Counts["failed"] != 1 {
		t.Fatal("retry budget", status, err)
	}
	count, err := service.Retry(ctx)
	if err != nil || count != 1 {
		t.Fatal("explicit retry", count, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.notification_messages SET status='running',attempts=1 WHERE event_key='retry'`); err != nil {
		t.Fatal(err)
	}
	if err = service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	sender.Fail = false
	if err = service.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = service.Status(ctx)
	if err != nil || status.Counts["sent"] != 1 {
		t.Fatal("restart recovery", status, err)
	}
}
