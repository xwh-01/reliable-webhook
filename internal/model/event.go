// 数据模型定义
//
// Event   — 业务事件，创建后几乎不变（status 固定为 "accepted"）
// Delivery — 投递任务，有独立的状态机和生命周期
//           通过 event_id 外键关联事件，形成 1:N 关系
//           replay 时会为同一条 event 创建多条 delivery
//
// 为什么拆成两张表而不是合在一起：
//   1. 1:N 关系 — 一条事件可以被多次投递（replay 场景依赖此结构）
//   2. 写隔离   — Event 只 INSERT 一次不变，Delivery 频繁 UPDATE 状态
//   3. 领域分离 — Event="发生了什么事"，Delivery="通知成功了吗"，不同生命周期
package model

import "time"

// Event 记录一次业务事件（下单、支付、库存变化等）
type Event struct{
	ID int64
	EventKey string   // 业务方定义的唯一标识，有唯一索引保证幂等
	EventType string  // 事件类型，如 "order.created"
	Payload string    // JSON 字符串，事件携带的业务数据
	CreatedAt time.Time
	Status string     // 固定为 "accepted"，不在代码中修改
}

// Delivery 一次投递任务，有独立状态机：
//   pending → running → succeeded / dead
//   dead 后可通过 replay 创建新 delivery
type Delivery struct {
	ID                 int64
	EventID            int64    // 关联 events.id
	TargetURL          string   // 下游 HTTP 地址
	Status             string   // pending / running / succeeded / dead
	LastError          *string  // 最后一次失败的错误信息，成功时为 NULL
	AttemptCount       int      // 已尝试投递次数（含当前）
	MaxAttempts        int      // 最大重试次数，默认 3
	ReplayOfDeliveryID *int64   // 指向被重放的 delivery.id，链式追溯
	CreatedAt          time.Time
	UpdatedAt          time.Time // ON UPDATE CURRENT_TIMESTAMP 自动更新
}

// EventDetail 查询事件详情时的返回结构
// 一对查询串联：events + 最新一条 delivery（LEFT JOIN 子查询取 id DESC LIMIT 1）
type EventDetail struct{
	Event Event
	Delivery
}

// AdminDeliveryInfo 管理后台列表接口的返回结构
// 比 EventDetail 更扁平，输出 delivery 摘要 + event_key 便于运营识别
type AdminDeliveryInfo struct {
	DeliveryID   int64      `json:"delivery_id"`
	EventID      int64      `json:"event_id"`
	EventKey     string     `json:"event_key"`    // 从 events 表 JOIN 获取
	TargetURL    string     `json:"target_url"`
	Status       string     `json:"status"`
	LastError    *string    `json:"last_error"`
	AttemptCount int        `json:"attempt_count"`
	MaxAttempts  int        `json:"max_attempts"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}
