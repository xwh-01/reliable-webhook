package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrActiveDeliveryExists = errors.New("active delivery exists")
var ErrDeliveryNotFound = errors.New("delivery not found")

type DeliveryRepository struct {
	db *sql.DB
}

func NewDeliveryRepository(db *sql.DB) *DeliveryRepository {
	return &DeliveryRepository{db: db}
}

type ClaimedDelivery struct {
	ID           int64
	EventID      int64
	TargetURL    string
	Payload      string
	AttemptCount int
	MaxAttempts  int
}

func (r *DeliveryRepository) ClaimOneReadyPending(ctx context.Context, lockedUntil time.Time) (*ClaimedDelivery, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT
			d.id,
			d.event_id,
			d.target_url,
			e.payload,
			d.attempt_count,
			d.max_attempts
		FROM deliveries d
		JOIN events e ON e.id = d.event_id
		WHERE (
			d.status = 'pending'
			AND (d.next_retry_at IS NULL OR d.next_retry_at <= NOW())
		) OR (
			d.status = 'running'
			AND d.locked_until IS NOT NULL
			AND d.locked_until <= NOW()
		)
		ORDER BY d.id ASC
		LIMIT 1
		FOR UPDATE
	`)

	var d ClaimedDelivery
	err = row.Scan(
		&d.ID,
		&d.EventID,
		&d.TargetURL,
		&d.Payload,
		&d.AttemptCount,
		&d.MaxAttempts,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claim select failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'running',
		    locked_until = ?,
		    attempt_count = attempt_count + 1
		WHERE id = ?
	`, lockedUntil, d.ID)
	if err != nil {
		return nil, fmt.Errorf("claim update failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim commit failed: %w", err)
	}

	d.AttemptCount++
	return &d, nil
}

func (r *DeliveryRepository) MarkSucceeded(ctx context.Context, deliveryID int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'succeeded',
		    last_error = NULL,
		    locked_until = NULL
		WHERE id = ?
	`, deliveryID)
	if err != nil {
		return fmt.Errorf("mark succeeded failed: %w", err)
	}
	return nil
}

func (r *DeliveryRepository) MarkDead(ctx context.Context, deliveryID int64, lastError string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'dead',
		    last_error = ?,
		    locked_until = NULL
		WHERE id = ?
	`, lastError, deliveryID)
	if err != nil {
		return fmt.Errorf("mark dead failed: %w", err)
	}
	return nil
}

func (r *DeliveryRepository) ScheduleRetry(ctx context.Context, deliveryID int64, lastError string, nextRetryAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE deliveries
		SET status = 'pending',
		    last_error = ?,
		    next_retry_at = ?,
		    locked_until = NULL
		WHERE id = ?
	`, lastError, nextRetryAt, deliveryID)
	if err != nil {
		return fmt.Errorf("schedule retry failed: %w", err)
	}
	return nil
}

func (r *DeliveryRepository) RecordAttempt(
	ctx context.Context,
	deliveryID int64,
	attemptNo int,
	status string,
	errorMessage *string,
	responseStatus *int,
) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO delivery_attempts (
			delivery_id, attempt_no, status, error_message, response_status
		) VALUES (?, ?, ?, ?, ?)
	`, deliveryID, attemptNo, status, errorMessage, responseStatus)
	if err != nil {
		return fmt.Errorf("record attempt failed: %w", err)
	}
	return nil
}

func (r *DeliveryRepository) CreateReplayDelivery(ctx context.Context, eventID int64) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	var activeID int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM deliveries
		WHERE event_id = ?
		  AND status IN ('pending', 'running')
		LIMIT 1
		FOR UPDATE
	`, eventID).Scan(&activeID)
	if err == nil {
		return 0, ErrActiveDeliveryExists
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("check active delivery failed: %w", err)
	}

	var latestDeliveryID int64
	var targetURL string
	err = tx.QueryRowContext(ctx, `
		SELECT id, target_url
		FROM deliveries
		WHERE event_id = ?
		ORDER BY id DESC
		LIMIT 1
		FOR UPDATE
	`, eventID).Scan(&latestDeliveryID, &targetURL)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, ErrDeliveryNotFound
		}
		return 0, fmt.Errorf("get latest delivery failed: %w", err)
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO deliveries (
			event_id,
			target_url,
			status,
			last_error,
			attempt_count,
			max_attempts,
			next_retry_at,
			replay_of_delivery_id
		)
		VALUES (?, ?, 'pending', NULL, 0, 3, NOW(), ?)
	`, eventID, targetURL, latestDeliveryID)
	if err != nil {
		return 0, fmt.Errorf("insert replay delivery failed: %w", err)
	}

	newDeliveryID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get replay delivery id failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit tx failed: %w", err)
	}

	return newDeliveryID, nil
}
