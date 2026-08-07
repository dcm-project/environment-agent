package config

import (
	"fmt"
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

func isValidCost(cost string) bool {
	switch cost {
	case "low", "medium-low", "medium", "medium-high", "high":
		return true
	default:
		return false
	}
}
