package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

func strictPayload(raw json.RawMessage, target any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("payload has trailing data")
	}
	return nil
}

func Validate(jobType string, payload json.RawMessage) error {
	switch jobType {
	case "checksum":
		var p struct {
			Data *string `json:"data"`
		}
		if err := strictPayload(payload, &p); err != nil {
			return fmt.Errorf("invalid checksum payload: %w", err)
		}
		if p.Data == nil || len(*p.Data) > 16*1024 {
			return errors.New("checksum data is required and must be at most 16384 bytes")
		}
	case "sleep":
		var p struct {
			Milliseconds *int `json:"milliseconds"`
		}
		if err := strictPayload(payload, &p); err != nil {
			return fmt.Errorf("invalid sleep payload: %w", err)
		}
		if p.Milliseconds == nil || *p.Milliseconds < 0 || *p.Milliseconds > 10000 {
			return errors.New("sleep milliseconds must be between 0 and 10000")
		}
	case "unstable":
		var p struct {
			FailUntilAttempt *int `json:"fail_until_attempt"`
		}
		if err := strictPayload(payload, &p); err != nil {
			return fmt.Errorf("invalid unstable payload: %w", err)
		}
		if p.FailUntilAttempt == nil || *p.FailUntilAttempt < 0 || *p.FailUntilAttempt > 20 {
			return errors.New("fail_until_attempt must be between 0 and 20")
		}
	default:
		return errors.New("type must be checksum, sleep or unstable")
	}
	return nil
}

type RetryableError struct{ Message string }

func (e RetryableError) Error() string { return e.Message }

func IsRetryable(err error) bool {
	var target RetryableError
	return errors.As(err, &target)
}

func Execute(ctx context.Context, jobType string, payload json.RawMessage, attempt int) (json.RawMessage, error) {
	if err := Validate(jobType, payload); err != nil {
		return nil, err
	}
	switch jobType {
	case "checksum":
		var p struct {
			Data string `json:"data"`
		}
		_ = json.Unmarshal(payload, &p)
		sum := sha256.Sum256([]byte(p.Data))
		return json.Marshal(map[string]string{"sha256": hex.EncodeToString(sum[:])})
	case "sleep":
		var p struct {
			Milliseconds int `json:"milliseconds"`
		}
		_ = json.Unmarshal(payload, &p)
		timer := time.NewTimer(time.Duration(p.Milliseconds) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return json.Marshal(map[string]int{"slept_ms": p.Milliseconds})
		}
	case "unstable":
		var p struct {
			FailUntilAttempt int `json:"fail_until_attempt"`
		}
		_ = json.Unmarshal(payload, &p)
		if attempt <= p.FailUntilAttempt {
			return nil, RetryableError{Message: fmt.Sprintf("temporary failure on attempt %d", attempt)}
		}
		return json.Marshal(map[string]int{"succeeded_on_attempt": attempt})
	}
	return nil, errors.New("unreachable job type")
}
