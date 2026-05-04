package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"reliable-webhook-platform/internal/model"
)

var ErrEventKeyConflict = errors.New("event_key already exists")

type EventRepository struct {
	db *sql.DB
}

func NewEventRepository(db *sql.DB) *EventRepository {
	return &EventRepository{db: db}
}

type CreateEventParams struct {
	EventKey  string
	EventType string
	Payload   string
	TargetURL string
}

func (r *EventRepository) CreateEventWithDelivery(ctx context.Context, p CreateEventParams) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		INSERT INTO events (event_key, event_type, payload, status)
		VALUES (?, ?, ?, ?)
	`, p.EventKey, p.EventType, p.Payload, "accepted")
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return 0, ErrEventKeyConflict
		}
		return 0, fmt.Errorf("insert event failed: %w", err)
	}

	eventID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get event id failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO deliveries (
			event_id, target_url, status, attempt_count, max_attempts, next_retry_at
		)
		VALUES (?, ?, ?, ?, ?, NOW())
	`, eventID, p.TargetURL, "pending", 0, 3)
	if err != nil {
		return 0, fmt.Errorf("insert delivery failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit tx failed: %w", err)
	}

	return eventID, nil
}

func (r *EventRepository) GetEventIDByKey(ctx context.Context, eventKey string) (int64, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id
		FROM events
		WHERE event_key = ?
		LIMIT 1
	`, eventKey)

	var eventID int64
	if err := row.Scan(&eventID); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("get event by key failed: %w", err)
	}
	return eventID, nil
}

func (r *EventRepository) GetEventDetailByID(ctx context.Context, id int64) (*model.EventDetail, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			e.id, e.event_key, e.event_type, e.payload, e.status, e.created_at,
			d.id, d.event_id, d.target_url, d.status, d.last_error,
			d.attempt_count, d.max_attempts, d.replay_of_delivery_id,
			d.created_at, d.updated_at
		FROM events e
		LEFT JOIN deliveries d ON d.id = (
			SELECT d2.id
			FROM deliveries d2
			WHERE d2.event_id = e.id
			ORDER BY d2.id DESC
			LIMIT 1
		)
		WHERE e.id = ?
		LIMIT 1
	`, id)

	var detail model.EventDetail
	var lastError sql.NullString
	var replayOfDeliveryID sql.NullInt64

	err := row.Scan(
		&detail.Event.ID,
		&detail.Event.EventKey,
		&detail.Event.EventType,
		&detail.Event.Payload,
		&detail.Event.Status,
		&detail.Event.CreatedAt,
		&detail.Delivery.ID,
		&detail.Delivery.EventID,
		&detail.Delivery.TargetURL,
		&detail.Delivery.Status,
		&lastError,
		&detail.Delivery.AttemptCount,
		&detail.Delivery.MaxAttempts,
		&replayOfDeliveryID,
		&detail.Delivery.CreatedAt,
		&detail.Delivery.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query event detail failed: %w", err)
	}

	if lastError.Valid {
		detail.Delivery.LastError = &lastError.String
	}
	if replayOfDeliveryID.Valid {
		detail.Delivery.ReplayOfDeliveryID = &replayOfDeliveryID.Int64
	}

	return &detail, nil
}