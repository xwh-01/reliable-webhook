// Event 表的数据操作
//
// 核心方法：
//   CreateEventWithDelivery — 事务内同时写入 events + deliveries，保证一致性
//   GetEventIDByKey        — 按 event_key 查 id，用于幂等判断
//   GetEventDetailByID     — 按 id 查事件 + 最新一条 delivery
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"reliable-webhook-platform/internal/model"
)

// ErrEventKeyConflict 事件重复提交（event_key 唯一索引冲突）
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

// CreateEventWithDelivery 在同一个事务中创建 event 和 delivery
//
// 原子性保证：
//   任意一步失败 → Rollback，不会出现"事件收了但没有投递任务"的情况。
//
// 幂等保证：
//   events 表 event_key 有唯一索引 uk_event_key。
//   INSERT 冲突 → MySQL 返回 1062 → 返回 ErrEventKeyConflict。
//   service 层收到这个错误后回查已有 id，返回给调用方。
func (r *EventRepository) CreateEventWithDelivery(ctx context.Context, p CreateEventParams) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil) // nil = 使用默认隔离级别（REPEATABLE-READ）
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback() // 安全网：未 commit 的事务自动回滚

	res, err := tx.ExecContext(ctx, `
		INSERT INTO events (event_key, event_type, payload, status)
		VALUES (?, ?, ?, ?)
	`, p.EventKey, p.EventType, p.Payload, "accepted")
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		// MySQL 错误码 1062 = Duplicate entry（唯一索引冲突）
		// 靠 event_key 唯一索引做幂等判断，不需要应用层先查再插
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return 0, ErrEventKeyConflict
		}
		return 0, fmt.Errorf("insert event failed: %w", err)
	}

	eventID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get event id failed: %w", err)
	}

	// 同事务中创建 delivery
	// next_retry_at = NOW() 让 dispatcher 立即可以捡走
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

// GetEventIDByKey 按 event_key 查 events.id
// 用于幂等场景：CreateEvent 遇到 key 冲突后回查已有 id
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
			return 0, nil // 没找到不算错误
		}
		return 0, fmt.Errorf("get event by key failed: %w", err)
	}
	return eventID, nil
}

// GetEventDetailByID 查询事件详情，带最新一条 delivery
//
// LEFT JOIN 子查询 (SELECT d2.id FROM deliveries d2 WHERE ... ORDER BY d2.id DESC LIMIT 1)
// 拿到最新 delivery 后 JOIN 主表。
// 这样做的好处：event 即使没有 delivery（边界情况），event 行依然返回，delivery 字段为 NULL。
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
	var lastError sql.NullString     // last_error 可为 NULL
	var replayOfDeliveryID sql.NullInt64 // replay_of_delivery_id 可为 NULL

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
			return nil, nil // 没找到不算错误
		}
		return nil, fmt.Errorf("query event detail failed: %w", err)
	}

	// 处理可为 NULL 的字段
	if lastError.Valid {
		detail.Delivery.LastError = &lastError.String
	}
	if replayOfDeliveryID.Valid {
		detail.Delivery.ReplayOfDeliveryID = &replayOfDeliveryID.Int64
	}

	return &detail, nil
}
