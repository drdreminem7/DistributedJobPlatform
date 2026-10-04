package retry

import (
	"testing"
	"time"
)

func TestBounds(t *testing.T) {
	cases := []struct {
		attempt          int
		minimum, maximum time.Duration
	}{
		{1, 100 * time.Millisecond, 200 * time.Millisecond},
		{2, 200 * time.Millisecond, 400 * time.Millisecond},
		{3, 400 * time.Millisecond, 800 * time.Millisecond},
		{20, 15 * time.Second, 30 * time.Second},
	}
	for _, c := range cases {
		minimum, maximum := Bounds(c.attempt)
		if minimum != c.minimum || maximum != c.maximum {
			t.Fatalf("attempt %d: got %v..%v", c.attempt, minimum, maximum)
		}
		for i := 0; i < 100; i++ {
			delay := Delay(c.attempt)
			if delay < minimum || delay > maximum {
				t.Fatalf("attempt %d: jitter %v outside range", c.attempt, delay)
			}
		}
	}
}
