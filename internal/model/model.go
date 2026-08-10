package model

import (
	"encoding/json"
	"time"
)

type VolatilityAlert struct {
	Symbol     string    `json:"symbol"`
	Volatility float64   `json:"volatility"`
	Threshold  float64   `json:"threshold"`
	Timestamp  time.Time `json:"timestamp"`
}

type DLQEnvelope struct {
	OriginalOffset int64           `json:"original_offset"`
	OriginalKey    string          `json:"original_key"`
	Payload        json.RawMessage `json:"payload"`
	Error          string          `json:"error"`
	FailedAt       time.Time       `json:"failed_aat"`
}
