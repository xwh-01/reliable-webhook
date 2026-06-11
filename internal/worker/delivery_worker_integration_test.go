package worker_test

import (
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"reliable-webhook-platform/internal/repository"
	"reliable-webhook-platform/internal/testutil"
	"reliable-webhook-platform/internal/worker"
)

func setupWorker(db *sql.DB) (*repository.DeliveryRepository, *worker.DeliveryWorker) {
	deliveryRepo := repository.NewDeliveryRepository(db)
	webhookClient := worker.NewWebhookClient()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	w := worker.NewDeliveryWorker(
		deliveryRepo,
		webhookClient,
		logger,
		nil,
		5*time.Second,
	)
	return deliveryRepo, w
}

func TestStateMachine_Success(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	mockSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockSvr.Close()

	eventID, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"sm-success", "order.created", `{"ok":true}`, mockSvr.URL,
	)

	deliveryRepo, deliveryWorker := setupWorker(db)

	claimed, err := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed delivery")
	}

	claimed.Payload = `{"ok":true}`
	deliveryWorker.Process(t.Context(), *claimed)

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "succeeded" {
		t.Fatalf("delivery %d status: want succeeded, got %s", deliveryID, status)
	}

	_ = eventID
	t.Logf("state machine success: delivery %d -> succeeded", deliveryID)
}

func TestStateMachine_NonRetryable4xx(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	mockSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer mockSvr.Close()

	_, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"sm-400", "order.created", `{}`, mockSvr.URL,
	)

	deliveryRepo, deliveryWorker := setupWorker(db)

	claimed, err := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed delivery")
	}

	deliveryWorker.Process(t.Context(), *claimed)

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "dead" {
		t.Fatalf("delivery status after 400: want dead, got %s", status)
	}

	var lastErr sql.NullString
	db.QueryRow("SELECT last_error FROM deliveries WHERE id = ?", deliveryID).Scan(&lastErr)
	if !lastErr.Valid || lastErr.String == "" {
		t.Fatal("last_error should be set for dead delivery")
	}
	t.Logf("state machine 400 -> dead: last_error=%s", lastErr.String)

	var attemptStatus string
	db.QueryRow(
		"SELECT status FROM delivery_attempts WHERE delivery_id = ? ORDER BY id DESC LIMIT 1",
		deliveryID,
	).Scan(&attemptStatus)
	if attemptStatus != "failed" {
		t.Fatalf("delivery_attempts status: want failed, got %s", attemptStatus)
	}
}

func TestStateMachine_RetryThenSucceed(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	callCount := 0
	mockSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockSvr.Close()

	_, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"sm-retry-ok", "order.created", `{}`, mockSvr.URL,
	)

	deliveryRepo, deliveryWorker := setupWorker(db)

	maxLoops := 10
	for i := 0; i < maxLoops; i++ {
		claimed, err := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
		if err != nil {
			t.Fatalf("claim attempt %d: %v", i+1, err)
		}
		if claimed == nil {
			status := testutil.QueryDeliveryStatus(t, db, deliveryID)
			if status == "succeeded" || status == "dead" {
				break
			}
			t.Logf("loop %d: no claimable delivery, status=%s (next_retry_at not reached yet)", i+1, status)
			testutil.SetNextRetryNow(t, db, deliveryID)
			continue
		}

		claimed.Payload = `{}`
		deliveryWorker.Process(t.Context(), *claimed)

		status := testutil.QueryDeliveryStatus(t, db, deliveryID)
		t.Logf("loop %d: attempt=%d, status=%s", i+1, claimed.AttemptCount, status)

		if status == "succeeded" || status == "dead" {
			break
		}
		testutil.SetNextRetryNow(t, db, deliveryID)
	}

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "succeeded" {
		t.Fatalf("final status: want succeeded (retry should recover), got %s", status)
	}

	attemptCount := testutil.QueryDeliveryAttemptCount(t, db, deliveryID)
	if attemptCount < 2 {
		t.Fatalf("attempt_count: want >= 2 (at least one retry), got %d", attemptCount)
	}
	t.Logf("state machine retry->succeed: %d attempts, final=succeeded", attemptCount)
}

func TestStateMachine_RetryToDead(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	mockSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockSvr.Close()

	_, deliveryID := testutil.SeedEventWithDelivery(t, db,
		"sm-retry-dead", "order.created", `{}`, mockSvr.URL,
	)

	deliveryRepo, deliveryWorker := setupWorker(db)

	for {
		claimed, err := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if claimed == nil {
			status := testutil.QueryDeliveryStatus(t, db, deliveryID)
			if status == "dead" || status == "succeeded" {
				break
			}
			testutil.SetNextRetryNow(t, db, deliveryID)
			continue
		}

		claimed.Payload = `{}`
		deliveryWorker.Process(t.Context(), *claimed)

		status := testutil.QueryDeliveryStatus(t, db, deliveryID)
		t.Logf("attempt=%d, max=%d, status=%s", claimed.AttemptCount, claimed.MaxAttempts, status)

		if status == "dead" || status == "succeeded" {
			break
		}
		testutil.SetNextRetryNow(t, db, deliveryID)
	}

	status := testutil.QueryDeliveryStatus(t, db, deliveryID)
	if status != "dead" {
		t.Fatalf("final status: want dead (max retries exhausted), got %s", status)
	}

	attemptCount := testutil.QueryDeliveryAttemptCount(t, db, deliveryID)
	if attemptCount != 3 {
		t.Fatalf("attempt_count: want 3 (max_attempts), got %d", attemptCount)
	}
	t.Logf("state machine retry->dead: %d attempts, final=dead", attemptCount)
}

func TestStateMachine_DeadReplaySuccess(t *testing.T) {
	db := testutil.NewTestDB(t)
	defer db.Close()
	testutil.TruncateAll(t, db)

	mockDeadSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer mockDeadSvr.Close()

	eventID, originalDeliveryID := testutil.SeedEventWithDelivery(t, db,
		"sm-replay", "order.created", `{}`, mockDeadSvr.URL,
	)

	deliveryRepo, deliveryWorker := setupWorker(db)

	claimed, _ := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
	if claimed != nil {
		deliveryWorker.Process(t.Context(), *claimed)
	}

	origStatus := testutil.QueryDeliveryStatus(t, db, originalDeliveryID)
	if origStatus != "dead" {
		t.Fatalf("original delivery should be dead (400), got %s", origStatus)
	}
	t.Logf("original delivery %d -> dead", originalDeliveryID)

	mockOkSvr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockOkSvr.Close()

	_, err := db.Exec("UPDATE deliveries SET target_url = ? WHERE id = ?", mockOkSvr.URL, originalDeliveryID)
	if err != nil {
		t.Fatalf("update target_url: %v", err)
	}

	newDeliveryID, err := deliveryRepo.CreateReplayDelivery(t.Context(), eventID)
	if err != nil {
		t.Fatalf("CreateReplayDelivery: %v", err)
	}
	t.Logf("replay created delivery %d from event %d", newDeliveryID, eventID)

	replayClaimed, err := deliveryRepo.ClaimOneReadyPending(t.Context(), time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("claim replay: %v", err)
	}
	if replayClaimed == nil {
		t.Fatal("expected claimed replay delivery")
	}
	if replayClaimed.ID != newDeliveryID {
		t.Fatalf("claimed replay delivery id: want %d, got %d", newDeliveryID, replayClaimed.ID)
	}

	replayClaimed.Payload = `{}`
	deliveryWorker.Process(t.Context(), *replayClaimed)

	newStatus := testutil.QueryDeliveryStatus(t, db, newDeliveryID)
	if newStatus != "succeeded" {
		t.Fatalf("replay delivery status: want succeeded, got %s", newStatus)
	}

	deliveryCount := testutil.CountRows(t, db, "deliveries")
	if deliveryCount != 2 {
		t.Fatalf("deliveries: want 2 (original dead + replay succeeded), got %d", deliveryCount)
	}
	t.Logf("state machine replay: original=%d dead, replay=%d -> succeeded", originalDeliveryID, newDeliveryID)
}
