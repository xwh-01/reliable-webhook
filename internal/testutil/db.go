package testutil

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"reliable-webhook-platform/internal/repository"
)

func dsnFromEnv() string {
	if v := os.Getenv("TEST_MYSQL_DSN"); v != "" {
		return v
	}
	return "webhook:webhook_pass@tcp(127.0.0.1:3306)/webhook_platform?parseTime=true&loc=Local"
}

func NewTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := dsnFromEnv()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("mysql not available, skipping integration test: %v", err)
	}

	lockDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql lock connection: %v", err)
	}
	if err := lockDB.Ping(); err != nil {
		lockDB.Close()
		t.Fatalf("ping mysql lock connection: %v", err)
	}
	var locked int
	if err := lockDB.QueryRow("SELECT GET_LOCK('reliable_webhook_integration_tests', 30)").Scan(&locked); err != nil {
		lockDB.Close()
		t.Fatalf("acquire mysql test lock: %v", err)
	}
	if locked != 1 {
		lockDB.Close()
		t.Fatalf("acquire mysql test lock: timed out")
	}
	t.Cleanup(func() {
		_, _ = lockDB.Exec("SELECT RELEASE_LOCK('reliable_webhook_integration_tests')")
		_ = lockDB.Close()
	})

	return db
}

func TruncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatalf("disable fk checks: %v", err)
	}
	tables := []string{"delivery_attempts", "deliveries", "events"}
	for _, table := range tables {
		if _, err := db.Exec("TRUNCATE TABLE " + table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatalf("enable fk checks: %v", err)
	}
}

func CountRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func QueryDeliveryStatus(t *testing.T, db *sql.DB, deliveryID int64) string {
	t.Helper()
	var status string
	if err := db.QueryRow("SELECT status FROM deliveries WHERE id = ?", deliveryID).Scan(&status); err != nil {
		t.Fatalf("query delivery %d status: %v", deliveryID, err)
	}
	return status
}

func QueryDeliveryAttemptCount(t *testing.T, db *sql.DB, deliveryID int64) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT attempt_count FROM deliveries WHERE id = ?", deliveryID).Scan(&n); err != nil {
		t.Fatalf("query delivery %d attempt_count: %v", deliveryID, err)
	}
	return n
}

func SeedEventWithDelivery(t *testing.T, db *sql.DB, eventKey, eventType, payload, targetURL string) (int64, int64) {
	t.Helper()
	eventRepo := repository.NewEventRepository(db)

	eventID, err := eventRepo.CreateEventWithDelivery(t.Context(), repository.CreateEventParams{
		EventKey:  eventKey,
		EventType: eventType,
		Payload:   payload,
		TargetURL: targetURL,
	})
	if err != nil {
		t.Fatalf("seed event+delivery: %v", err)
	}

	var deliveryID int64
	if err := db.QueryRow("SELECT id FROM deliveries WHERE event_id = ? LIMIT 1", eventID).Scan(&deliveryID); err != nil {
		t.Fatalf("seed: query delivery id: %v", err)
	}

	return eventID, deliveryID
}

func SetNextRetryNow(t *testing.T, db *sql.DB, deliveryID int64) {
	t.Helper()
	_, err := db.Exec("UPDATE deliveries SET next_retry_at = NOW() WHERE id = ?", deliveryID)
	if err != nil {
		t.Fatalf("set next_retry_at: %v", err)
	}
}

func MustQueryRow(t *testing.T, db *sql.DB, query string, args ...any) *sql.Row {
	t.Helper()
	return db.QueryRow(query, args...)
}

func MustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func AssertEqual(t *testing.T, expected, actual any, msg ...string) {
	t.Helper()
	detail := fmt.Sprintf("\nexpected: %v\n  actual: %v", expected, actual)
	if len(msg) > 0 {
		detail = msg[0] + detail
	}
	if expected != actual {
		t.Fatal(detail)
	}
}
