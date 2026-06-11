package repository_test

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"reliable-webhook-platform/internal/repository"
	"reliable-webhook-platform/internal/testutil"
)

func TestCreateEventWithDelivery_Success_Atomic(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	repo := repository.NewEventRepository(db)

	eventID, err := repo.CreateEventWithDelivery(t.Context(), repository.CreateEventParams{
		EventKey:  "atomic-success-1",
		EventType: "order.created",
		Payload:   `{"order_id":1}`,
		TargetURL: "http://example.com/webhook",
	})
	if err != nil {
		t.Fatalf("CreateEventWithDelivery failed: %v", err)
	}
	if eventID <= 0 {
		t.Fatalf("expected positive event_id, got %d", eventID)
	}

	eventCount := testutil.CountRows(t, db, "events")
	deliveryCount := testutil.CountRows(t, db, "deliveries")
	attemptCount := testutil.CountRows(t, db, "delivery_attempts")

	if eventCount != 1 {
		t.Fatalf("events table: want 1 row, got %d", eventCount)
	}
	if deliveryCount != 1 {
		t.Fatalf("deliveries table: want 1 row, got %d (orphan delivery detected)", deliveryCount)
	}
	if attemptCount != 0 {
		t.Fatalf("delivery_attempts table: want 0 (attempts only created by worker), got %d", attemptCount)
	}

	var deliveryStatus, eventStatus string
	db.QueryRow("SELECT status FROM deliveries WHERE event_id = ?", eventID).Scan(&deliveryStatus)
	db.QueryRow("SELECT status FROM events WHERE id = ?", eventID).Scan(&eventStatus)

	if deliveryStatus != "pending" {
		t.Fatalf("delivery status: want pending, got %s", deliveryStatus)
	}
	if eventStatus != "accepted" {
		t.Fatalf("event status: want accepted, got %s", eventStatus)
	}
}

func TestCreateEventWithDelivery_DuplicateEventKey(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	repo := repository.NewEventRepository(db)

	params := repository.CreateEventParams{
		EventKey:  "dedup-key-1",
		EventType: "order.created",
		Payload:   `{"order_id":1}`,
		TargetURL: "http://example.com/webhook",
	}

	eventID1, err := repo.CreateEventWithDelivery(t.Context(), params)
	if err != nil {
		t.Fatalf("first CreateEventWithDelivery failed: %v", err)
	}

	_, err = repo.CreateEventWithDelivery(t.Context(), params)
	if err == nil {
		t.Fatal("second call should return ErrEventKeyConflict, got nil")
	}
	if err != repository.ErrEventKeyConflict {
		t.Fatalf("second call: want ErrEventKeyConflict, got %v", err)
	}

	eventCount := testutil.CountRows(t, db, "events")
	deliveryCount := testutil.CountRows(t, db, "deliveries")

	if eventCount != 1 {
		t.Fatalf("events: want 1 (dedup), got %d", eventCount)
	}
	if deliveryCount != 1 {
		t.Fatalf("deliveries: want 1 (no orphan on dup key), got %d", deliveryCount)
	}

	existingID, err := repo.GetEventIDByKey(t.Context(), "dedup-key-1")
	if err != nil {
		t.Fatalf("GetEventIDByKey failed: %v", err)
	}
	if existingID != eventID1 {
		t.Fatalf("GetEventIDByKey: want %d, got %d", eventID1, existingID)
	}
}

func TestCreateEventWithDelivery_DuplicateNoOrphanDelivery(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	repo := repository.NewEventRepository(db)

	params := repository.CreateEventParams{
		EventKey:  "atomic-dup-2",
		EventType: "test",
		Payload:   `{}`,
		TargetURL: "http://localhost/webhook",
	}

	_, _ = repo.CreateEventWithDelivery(t.Context(), params)

	const parallel = 10
	var wg sync.WaitGroup
	errs := make(chan error, parallel)

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.CreateEventWithDelivery(t.Context(), params)
			if err != nil && err != repository.ErrEventKeyConflict {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		t.Errorf("unexpected error during concurrent duplicate: %v", e)
	}

	if n := testutil.CountRows(t, db, "events"); n != 1 {
		t.Fatalf("events: concurrent dedup failed, got %d rows", n)
	}
	if n := testutil.CountRows(t, db, "deliveries"); n != 1 {
		t.Fatalf("deliveries: concurrent dedup produced orphan, got %d rows", n)
	}
}

func TestTransactionAtomicity_RollbackOnFailure(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = tx.Exec(`
		INSERT INTO events (event_key, event_type, payload, status)
		VALUES (?, ?, ?, ?)
	`, "rollback-key", "test", "{}", "accepted")
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}

	_, err = tx.Exec(`
		INSERT INTO deliveries (event_id, target_url, status, attempt_count, max_attempts, next_retry_at)
		VALUES (?, ?, ?, ?, ?, NOW())
	`, 999999, "http://example.com/webhook", "pending", 0, 3)
	if err == nil {
		t.Fatal("expected FK violation, got nil error")
	}
	tx.Rollback()

	if n := testutil.CountRows(t, db, "events"); n != 0 {
		t.Fatalf("events: rollback should leave 0 rows, got %d", n)
	}
	if n := testutil.CountRows(t, db, "deliveries"); n != 0 {
		t.Fatalf("deliveries: rollback should leave 0 rows, got %d", n)
	}
}

func TestClaimOneReadyPending_Basic(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	_, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"claim-basic", "order.created", `{}`, "http://example.com/webhook",
	)

	repo := repository.NewDeliveryRepository(db)
	lockedUntil := time.Now().Add(30 * time.Second)

	claimed, err := repo.ClaimOneReadyPending(t.Context(), lockedUntil)
	if err != nil {
		t.Fatalf("ClaimOneReadyPending: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed delivery, got nil")
	}
	if claimed.ID != deliveryID {
		t.Fatalf("claimed.ID: want %d, got %d", deliveryID, claimed.ID)
	}
	if claimed.AttemptCount != 1 {
		t.Fatalf("attempt_count: want 1, got %d", claimed.AttemptCount)
	}

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "running" {
		t.Fatalf("delivery status after claim: want running, got %s", status)
	}

	// Debug: inspect DB state before second claim
	var dbStatus string
	db.QueryRow("SELECT status FROM deliveries WHERE id = ?", deliveryID).Scan(&dbStatus)
	var dbNow string
	db.QueryRow("SELECT NOW()").Scan(&dbNow)
	var dbLocked string
	db.QueryRow("SELECT DATE_FORMAT(locked_until, '%Y-%m-%d %H:%i:%s.%f') FROM deliveries WHERE id = ?", deliveryID).Scan(&dbLocked)
	t.Logf("BEFORE second claim: status=%s locked_until=%s NOW=%s", dbStatus, dbLocked, dbNow)

	claimed2, err := repo.ClaimOneReadyPending(t.Context(), lockedUntil)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimed2 != nil {
		t.Fatalf("second claim should return nil (task already running), got id=%d", claimed2.ID)
	}
}

func TestClaimOneReadyPending_Concurrent(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	_, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"claim-race", "order.created", `{}`, "http://example.com/webhook",
	)

	repo := repository.NewDeliveryRepository(db)
	lockedUntil := time.Now().Add(30 * time.Second)

	const concurrency = 10
	results := make(chan *repository.ClaimedDelivery, concurrency)
	ready := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			wg.Done()
			<-ready
			claimed, err := repo.ClaimOneReadyPending(t.Context(), lockedUntil)
			if err != nil {
				t.Logf("claim error: %v", err)
				results <- nil
				return
			}
			results <- claimed
		}()
	}

	wg.Wait()
	close(ready)

	var successCount int
	for i := 0; i < concurrency; i++ {
		c := <-results
		if c != nil {
			successCount++
		}
	}

	if successCount != 1 {
		t.Fatalf("concurrent claim: exactly 1 should succeed, got %d", successCount)
	}

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "running" {
		t.Fatalf("delivery status: want running, got %s", status)
	}

	attemptCount := testutil.QueryDeliveryAttemptCount(t, db, deliveryID)
	if attemptCount != 1 {
		t.Fatalf("attempt_count: want 1 (incremented exactly once), got %d", attemptCount)
	}
}

func TestCreateReplayDelivery_Success(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	eventID, oldDeliveryID := testutil.SeedEventWithDelivery(t, db,
		"replay-src", "order.created", `{}`, "http://example.com/webhook",
	)

	repo := repository.NewDeliveryRepository(db)

	_, err := db.Exec("UPDATE deliveries SET status = 'dead', last_error = 'test: downstream 500' WHERE id = ?", oldDeliveryID)
	if err != nil {
		t.Fatalf("mark old delivery dead: %v", err)
	}

	newDeliveryID, err := repo.CreateReplayDelivery(t.Context(), eventID)
	if err != nil {
		t.Fatalf("CreateReplayDelivery: %v", err)
	}

	if newDeliveryID <= oldDeliveryID {
		t.Fatalf("new delivery id %d should be > old %d", newDeliveryID, oldDeliveryID)
	}

	var newStatus string
	var replayOf sql.NullInt64
	db.QueryRow("SELECT status, replay_of_delivery_id FROM deliveries WHERE id = ?", newDeliveryID).Scan(&newStatus, &replayOf)

	if newStatus != "pending" {
		t.Fatalf("replay delivery status: want pending, got %s", newStatus)
	}
	if !replayOf.Valid || replayOf.Int64 != oldDeliveryID {
		t.Fatalf("replay_of_delivery_id: want %d, got %v", oldDeliveryID, replayOf)
	}

	deliveryCount := testutil.CountRows(t, db, "deliveries")
	if deliveryCount != 2 {
		t.Fatalf("deliveries: want 2 (original + replay), got %d", deliveryCount)
	}
}

func TestCreateReplayDelivery_RejectWhenActive(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	eventID, _ := testutil.SeedEventWithDelivery(t, db,
		"replay-active", "order.created", `{}`, "http://example.com/webhook",
	)

	repo := repository.NewDeliveryRepository(db)

	_, err := repo.CreateReplayDelivery(t.Context(), eventID)
	if err != repository.ErrActiveDeliveryExists {
		t.Fatalf("replay with active delivery: want ErrActiveDeliveryExists, got %v", err)
	}
}

func TestCreateReplayDelivery_NoDelivery(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	_, err := db.Exec(`
		INSERT INTO events (event_key, event_type, payload, status)
		VALUES (?, ?, ?, ?)
	`, "no-delivery", "test", "{}", "accepted")
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}

	repo := repository.NewDeliveryRepository(db)
	_, err = repo.CreateReplayDelivery(t.Context(), 1)
	if err != repository.ErrDeliveryNotFound {
		t.Fatalf("replay without delivery: want ErrDeliveryNotFound, got %v", err)
	}
}
