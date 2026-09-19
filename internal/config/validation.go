package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

func validateRequired(envName, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s: required but not set or empty", envName)
	}
	return nil
}

func validateDurationRange(envName string, val, min, max time.Duration) error {
	if val < min || val > max {
		return fmt.Errorf("%s: %s is outside valid range [%s, %s]", envName, val, min, max)
	}
	return nil
}

// validateAbsoluteHTTPURL checks that value parses as an absolute URL with
// a nonempty host and an http or https scheme. Used for endpoints that are
// dialed directly (e.g. DCM_AUTH_TOKEN_ENDPOINT), where a malformed,
// relative, hostless, or unsupported-scheme value would otherwise pass
// startup validation and only fail at request time on every attempt.
func validateAbsoluteHTTPURL(envName, value string) error {
	u, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%s: invalid URL %q: %w", envName, value, err)
	}
	if !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("%s: %q must be an absolute URL with a host", envName, value)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s: %q must use the http or https scheme, got %q", envName, value, u.Scheme)
	}
	return nil
}

func isValidCost(cost string) bool {
	switch cost {
	case "low", "medium-low", "medium", "medium-high", "high":
		return true
	default:
		return false
	}
}
