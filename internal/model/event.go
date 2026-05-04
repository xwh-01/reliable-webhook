package model

import "time"

type Event struct{
	ID int64
	EventKey string
	EventType string
	Payload string
	CreatedAt time.Time
	Status string
}

type Delivery struct {
	ID                 int64
	EventID            int64
	TargetURL          string
	Status             string
	LastError          *string
	AttemptCount       int
	MaxAttempts        int
	ReplayOfDeliveryID *int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type EventDetail struct{
	Event Event
	Delivery
}