package retry

import (
	"math/rand/v2"
	"time"
)

func Bounds(attempt int) (time.Duration, time.Duration) {
	if attempt < 1 {
		attempt = 1
	}
	maximum := 200 * time.Millisecond
	for i := 1; i < attempt && maximum < 30*time.Second; i++ {
		maximum *= 2
		if maximum > 30*time.Second {
			maximum = 30 * time.Second
		}
	}
	return maximum / 2, maximum
}

func Delay(attempt int) time.Duration {
	minimum, maximum := Bounds(attempt)
	return minimum + time.Duration(rand.Int64N(int64(maximum-minimum)+1))
}
