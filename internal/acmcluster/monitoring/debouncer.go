package monitoring

import (
	"sync"
	"time"
)

// Debouncer coalesces rapid status events per instance.
type Debouncer struct {
	interval  time.Duration
	publishFn func(StatusEvent)
	mu        sync.Mutex
	wg        sync.WaitGroup
	timers    map[string]*time.Timer
	stopped   bool
}

// NewDebouncer creates a Debouncer with the given interval and publish function.
func NewDebouncer(interval time.Duration, publishFn func(StatusEvent)) *Debouncer {
	return &Debouncer{
		interval:  interval,
		publishFn: publishFn,
		timers:    make(map[string]*time.Timer),
	}
}

// Submit enqueues a status event for debouncing. The event is captured in the
// timer closure — only the last submitted event per key fires after the interval.
func (d *Debouncer) Submit(key string, event StatusEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	if t, ok := d.timers[key]; ok {
		t.Stop()
	}
	d.timers[key] = time.AfterFunc(d.interval, func() {
		d.mu.Lock()
		if d.stopped {
			d.mu.Unlock()
			return
		}
		delete(d.timers, key)
		d.wg.Add(1)
		d.mu.Unlock()
		defer d.wg.Done()
		d.publishFn(event)
	})
}

// Stop halts all pending timers and waits for in-flight callbacks to complete.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	d.stopped = true
	for key, t := range d.timers {
		t.Stop()
		delete(d.timers, key)
	}
	d.mu.Unlock()
	d.wg.Wait()
}
