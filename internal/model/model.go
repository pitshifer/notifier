package model

import (
	"encoding/json"
	"fmt"
	"strings"
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
	FailedAt       time.Time       `json:"failed_at"`
}

func (va *VolatilityAlert) FormatAlert() string {
	return fmt.Sprintf("⚠ %s\nvolatility: %.2f%% (threshold: %.2f%%)\n%s",
		strings.ToUpper(va.Symbol),
		va.Volatility*100,
		va.Threshold*100,
		va.Timestamp.Format(time.RFC3339),
	)
}

func NewDLQEnvelope(rawPayload []byte, offset int64, key, errMsg string) DLQEnvelope {
	return DLQEnvelope{
		OriginalOffset: offset,
		OriginalKey:    key,
		Payload:        rawPayload,
		Error:          errMsg,
		FailedAt:       time.Now(),
	}
}
