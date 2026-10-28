package monitoring

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dcm-project/environment-agent/api/database/v1alpha1"
)

// Debouncer coalesces rapid status change events within a time window,
// publishing only the last event once the window elapses.
//
// Submissions are silently dropped until SetReady is called. This allows the
// caller to wait for all informer caches to sync before any event is queued,
// preventing partial-cache snapshots from being published at startup.
//
// Identical repeated submissions are also suppressed: if the incoming event
// matches the one already queued for an instance (preventing timer resets on
// unchanged resyncs) or matches the last published event with nothing pending
// (preventing redundant re-publication on resync), it is dropped.
type Debouncer struct {
	interval      time.Duration
	publishFn     func(StatusEvent) bool
	mu            sync.Mutex
	wg            sync.WaitGroup
	timers        map[string]*time.Timer
	gens          map[string]uint64      // generation counter per instance, guards against superseded timer goroutines
	publishGen    map[string]uint64      // gen of the most recently started in-flight publish per instance
	pendingEvent  map[string]StatusEvent // event captured by the current pending timer, if any
	lastPublished map[string]StatusEvent // event from the last timer that successfully fired
	stopped       bool
	ready         atomic.Bool // gate: Submit is a no-op until SetReady is called
}

// NewDebouncer creates a Debouncer with the given interval and publish callback.
// Call SetReady before submitting events in production; the gate exists so
// partial-cache events during informer startup are never queued.
func NewDebouncer(interval time.Duration, publishFn func(StatusEvent) bool) *Debouncer {
	if publishFn == nil {
		panic("monitoring: publishFn is required")
	}
	return &Debouncer{
		interval:      interval,
		publishFn:     publishFn,
		timers:        make(map[string]*time.Timer),
		gens:          make(map[string]uint64),
		publishGen:    make(map[string]uint64),
		pendingEvent:  make(map[string]StatusEvent),
		lastPublished: make(map[string]StatusEvent),
	}
}

// SetReady opens the submission gate. After this call, Submit will queue
// events normally. It is safe to call concurrently and more than once.
func (d *Debouncer) SetReady() {
	d.ready.Store(true)
}

// Submit queues a status event for the given instance. If another event
// arrives within the debounce window, the previous event is replaced.
// Calls before SetReady are silently dropped.
//
// Identical events are suppressed without resetting the debounce timer:
//   - If the same event is already pending, the existing timer is left alone so
//     it fires on its original schedule (retry path is preserved).
//   - If the same event was the last one published and nothing is pending, the
//     submission is dropped entirely (resync dedup).
func (d *Debouncer) Submit(instanceID string, event StatusEvent) {
	if !d.ready.Load() {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return
	}

	_, hasPending := d.timers[instanceID]

	// Same event already queued: leave the existing timer alone so it fires on
	// schedule rather than being reset by a resync.
	if hasPending && d.pendingEvent[instanceID] == event {
		return
	}

	// Same event already delivered with nothing pending: nothing to do.
	if !hasPending && d.lastPublished[instanceID] == event {
		return
	}

	if hasPending {
		d.timers[instanceID].Stop()
		// t.Stop() may return false if the timer goroutine has already started.
		// The generation counter below ensures that in-flight goroutine exits
		// without publishing the superseded event.
	}

	d.gens[instanceID]++
	gen := d.gens[instanceID]
	d.pendingEvent[instanceID] = event

	d.timers[instanceID] = time.AfterFunc(d.interval, func() {
		d.mu.Lock()
		if d.stopped || d.gens[instanceID] != gen {
			d.mu.Unlock()
			return
		}
		delete(d.timers, instanceID)
		delete(d.pendingEvent, instanceID)
		d.publishGen[instanceID] = gen
		d.wg.Add(1)
		d.mu.Unlock()
		defer d.wg.Done()
		if d.publishFn(event) {
			d.mu.Lock()
			// Only record this as the last published event if no newer publish
			// has started since we released the lock. This prevents a slow retry
			// from overwriting lastPublished after a faster successor already set it.
			if d.publishGen[instanceID] == gen {
				if event.Status == v1alpha1.DELETED {
					// Instance is gone; evict its tracking state so the maps don't
					// grow without bound across the lifetime of the debouncer.
					delete(d.lastPublished, instanceID)
					delete(d.publishGen, instanceID)
				} else {
					d.lastPublished[instanceID] = event
				}
			}
			d.mu.Unlock()
		}
	})
}

// Stop halts the debouncer, preventing new event submissions, flushes any
// events still waiting in pending timers, and waits for all in-flight publish
// callbacks to complete before returning.
//
// Events in cancelled timers are flushed synchronously so that status changes
// submitted just before shutdown are not silently discarded. The publishFn
// should use a context that outlives the monitor's context (e.g. a short-lived
// publish context with a bounded timeout) so the flush can succeed.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	d.stopped = true
	var pending []StatusEvent
	for id, t := range d.timers {
		t.Stop()
		// t.Stop() may return false if the timer goroutine is already executing.
		// The stopped flag ensures those goroutines exit without double-publishing;
		// their events are captured below and flushed once wg.Wait returns.
		delete(d.timers, id)
		if event, ok := d.pendingEvent[id]; ok {
			pending = append(pending, event)
			delete(d.pendingEvent, id)
		}
	}
	d.mu.Unlock()

	// Wait for goroutines that were mid-execution when Stop was called.
	// They will exit early (stopped=true) without publishing, so their events
	// are already captured in pending above.
	d.wg.Wait()

	// Flush events whose timers were cancelled, using the still-valid publishFn.
	// Success is not tracked here — the debouncer is shutting down.
	for _, event := range pending {
		_ = d.publishFn(event)
	}
}
