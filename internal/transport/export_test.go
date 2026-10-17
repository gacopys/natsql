package transport

import (
	"testing"
	"time"
)

// SetFlushTimeout shortens the subscription flush timeout for one test.
func SetFlushTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	saved := natsFlushTimeout
	natsFlushTimeout = d
	t.Cleanup(func() { natsFlushTimeout = saved })
}
