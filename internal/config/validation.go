package config

import (
	"fmt"
	"time"
)

func validateDurationRange(envName string, val, min, max time.Duration, rangeDisplay string) error {
	if val < min || val > max {
		return fmt.Errorf("%s: %s is outside valid range %s", envName, val, rangeDisplay)
	}
	return nil
}
