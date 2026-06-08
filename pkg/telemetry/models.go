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
	SubmissionID string    `json:"submission_id"`
	BotID        string    `json:"bot_id"`
	Type         MetricType `json:"type"`
	Value        float64   `json:"value"`
	Timestamp    time.Time `json:"timestamp"`
	Success      bool      `json:"success"`
	ErrorMessage string    `json:"error_message,omitempty"`
}
