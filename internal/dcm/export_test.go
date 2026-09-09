package dcm

import "time"

// SetExpiryDelta overrides the proactive-refresh safety buffer for tests.
func (c *ClientCredentialsTokenSource) SetExpiryDelta(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expiryDelta = d
}
