package executor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestChecksum(t *testing.T) {
	result, err := Execute(context.Background(), "checksum", json.RawMessage(`{"data":"abc"}`), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") {
		t.Fatalf("unexpected SHA-256 result: %s", result)
	}
}

func TestUnstableRetries(t *testing.T) {
	payload := json.RawMessage(`{"fail_until_attempt":2}`)
	for attempt := 1; attempt <= 2; attempt++ {
		_, err := Execute(context.Background(), "unstable", payload, attempt)
		if !IsRetryable(err) {
			t.Fatalf("attempt %d should be retryable: %v", attempt, err)
		}
	}
	result, err := Execute(context.Background(), "unstable", payload, 3)
	if err != nil || !strings.Contains(string(result), `"succeeded_on_attempt":3`) {
		t.Fatalf("third attempt should succeed: %s %v", result, err)
	}
}

func TestRejectUnboundedSleep(t *testing.T) {
	if err := Validate("sleep", json.RawMessage(`{"milliseconds":10001}`)); err == nil {
		t.Fatal("expected bounded sleep validation error")
	}
}
