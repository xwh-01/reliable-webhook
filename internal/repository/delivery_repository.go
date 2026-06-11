// Delivery 表的数据操作
//
// 核心方法：
//   ClaimOneReadyPending  — dispatcher 轮询领取待投递任务，用 FOR UPDATE 防并发
//   MarkSucceeded         — 投递成功
//   MarkDead              — 标记为永久失败
//   ScheduleRetry         — 投递失败后安排下次重试
//   RecordAttempt         — 记录每次投递尝试到 delivery_attempts
//   CreateReplayDelivery  — 人工重放，事务内创建新 delivery（并发安全靠 FOR UPDATE + 间隙锁）
//   ListByStatus          — 管理后台按状态列投递
//
// 并发安全说明：
//   所有写操作都不需要应用层锁。
//   MySQL 的 FOR UPDATE + REPEATABLE-READ 提供了间隙锁（gap lock），
//   保证两条并发 replay 请求不会同时创建出两条 pending delivery。
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"reliable-webhook-platform/internal/model"
)

var ErrActiveDeliveryExists = errors.New("active delivery exists")
var ErrDeliveryNotFound = errors.New("delivery not found")

type DeliveryRepository struct {
	db *sql.DB
}

func NewDeliveryRepository(db *sql.DB) *DeliveryRepository {
	return &DeliveryRepository{db: db}
}

// ClaimedDelivery dispatcher 认领后传给 worker 的任务结构
type ClaimedDelivery struct {
	ID           int64
	EventID      int64
	TargetURL    string
	Payload      string // 从 events 表 JOIN 来的，worker 直接拼 HTTP body
	AttemptCount int    // 领取前的 attempt_count，worker 里会 +1
	MaxAttempts  int
}

// ClaimOneReadyPending 认领一个可投递的任务
//
// 认领条件（二选一）：
//   1. status='pending' 且 next_retry_at 已到（或为 NULL，表示立即投递）
//   2. status='running' 且 locked_until 已过期（上一个 worker 挂了/卡死了）
//
// 并发安全：
//   SELECT ... FOR UPDATE 锁定扫描到的行。
//   同事务内 UPDATE status='running' + locked_until，防止其他 worker 重复认领。
//   locked_until = now + claimLease，租约到期前此任务独占。
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
			return nil, nil // 没有可投递任务，正常情况
		}
		return nil, fmt.Errorf("claim select failed: %w", err)
	}

	// 标记为 running + 设置租约
	// attempt_count 在这里 +1（此时投递还没执行，只是"即将执行"）
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

	d.AttemptCount++ // 与数据库同步
	return &d, nil
}

// MarkSucceeded 标记投递成功
// 清空 last_error 和 locked_until
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

// MarkDead 标记投递为永久失败（不可重试错误 或 达到最大重试次数）
// 写入 last_error，清空 locked_until，释放租约
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

// ScheduleRetry 投递失败但可重试时调用
// 将状态退回 pending，设置下一次重试时间，释放租约
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

// RecordAttempt 记录一次投递尝试到 delivery_attempts 表（审计用）
// 无论成功失败都会写一条，用于事后排查"某次投递发生了什么"
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

// CreateReplayDelivery 人工重放：为指定事件创建一条全新的 delivery
//
// 事务内三步：
//   Step 1. SELECT ... FOR UPDATE — 查有无活跃 delivery（pending/running）
//           有 → 返回 ErrActiveDeliveryExists（防并发脑裂）
//           无 → FOR UPDATE 在 idx_event_id 索引上加间隙锁，堵住并发 INSERT
//   Step 2. 读最新一条 delivery 拿 target_url（不放参数，防填错地址）
//   Step 3. INSERT 新 delivery（status='pending', next_retry_at=NOW()）
//
// 并发安全：
//   第一次 FOR UPDATE 可能走 idx_status 或 idx_event_id，优化器选择不确定。
//   第二次 FOR UPDATE（查 event_id 取 target_url）确保 idx_event_id 上的事件区间被锁。
//   即使第一次走了 idx_status 没锁住 event_id=100 区间，第二次一定锁住。
//   后到的请求查到时已有一条 pending → 返回 409。
//
// 为什么 INSERT 而不是 UPDATE 旧行：
//   - 保留完整投递历史，旧行和 delivery_attempts 的关联不变
//   - 支持多次 replay（失败后可再次 replay）
//   - replay_of_delivery_id 字段串起投递链，可追溯
func (r *DeliveryRepository) CreateReplayDelivery(ctx context.Context, eventID int64) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback()

	// Step 1: 检查是否有活跃中的投递（同一个事件只能有一个投递在执行）
	// FOR UPDATE 保证并发安全：后到的请求会看到先到的请求插入的 pending 行
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
		// 查到了 → 有活跃投递在进行中，拒绝本次 replay
		return 0, ErrActiveDeliveryExists
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("check active delivery failed: %w", err)
	}

	// Step 2: 拿最新 delivery 的 target_url（不信任调用方传参）
	// FOR UPDATE 确保优化器走 idx_event_id，锁住该事件区间
	// 即使 Step 1 的 FOR UPDATE 走了 idx_status 没锁住这里，这次走 idx_event_id 一定锁住
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
			return 0, ErrDeliveryNotFound // 该事件没有任何 delivery 记录
		}
		return 0, fmt.Errorf("get latest delivery failed: %w", err)
	}

	// Step 3: 创建全新 delivery
	// next_retry_at = NOW() 让 dispatcher 立即可以领取
	// replay_of_delivery_id 指向旧的 delivery，形成投递链
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

// ListByStatus 按状态列投递记录（管理后台用）
// 默认查 status='dead'，运营最常需要这个
// LIMIT 200 防止一次性拉太多
func (r *DeliveryRepository) ListByStatus(ctx context.Context, status string) ([]model.AdminDeliveryInfo, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			d.id,
			d.event_id,
			e.event_key,
			d.target_url,
			d.status,
			d.last_error,
			d.attempt_count,
			d.max_attempts,
			d.created_at,
			d.updated_at
		FROM deliveries d
		JOIN events e ON e.id = d.event_id
		WHERE d.status = ?
		ORDER BY d.updated_at DESC
		LIMIT 200
	`, status)
	if err != nil {
		return nil, fmt.Errorf("list deliveries by status failed: %w", err)
	}
	defer rows.Close()

	var result []model.AdminDeliveryInfo
	for rows.Next() {
		var item model.AdminDeliveryInfo
		var lastError sql.NullString
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventKey,
			&item.TargetURL,
			&item.Status,
			&lastError,
			&item.AttemptCount,
			&item.MaxAttempts,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery row failed: %w", err)
		}
		if lastError.Valid {
			item.LastError = &lastError.String
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery rows failed: %w", err)
	}

	return result, nil
}
