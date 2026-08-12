package dcm

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter parses the Retry-After header value.
// Supports seconds (integer) and HTTP-date (RFC1123) formats per RFC 7231 §7.1.3.
// Returns the duration to wait and whether parsing succeeded.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}

	if t, err := http.ParseTime(value); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
		return 0, false
	}

	return 0, false
}
