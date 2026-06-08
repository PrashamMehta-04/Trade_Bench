package telemetry

import (
	"time"
)

type MetricType string

const (
	Latency     MetricType = "LATENCY"
	Throughput  MetricType = "THROUGHPUT"
	Correctness MetricType = "CORRECTNESS"
)

type MetricEvent struct {
	SubmissionID string      `json:"submission_id"`
	BotID        string      `json:"bot_id"`
	Type         MetricType  `json:"type"`
	Value        float64     `json:"value"`
	Timestamp    time.Time   `json:"timestamp"`
	Success      bool        `json:"success"`
	ErrorMessage string      `json:"error_message,omitempty"`
	OrderData    *OrderEvent `json:"order_data,omitempty"`
}

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

type OrderEvent struct {
	OrderID    string  `json:"order_id"`
	Side       Side    `json:"side"`
	Price      float64 `json:"price"`
	Quantity   float64 `json:"quantity"`
	IsResolved bool    `json:"is_resolved"` // True if this was a response from the contestant
	FillPrice  float64 `json:"fill_price,omitempty"`
	FillQty    float64 `json:"fill_qty,omitempty"`
}
