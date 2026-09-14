package monitoring

import (
	"sync"
	"time"
)

// instanceState holds per-instance debounce state.
type instanceState struct {
	timer    *time.Timer
	pending  StatusEvent
	ver      uint64
	draining bool
}

// Debouncer coalesces rapid status change events within a time window,
// publishing only the last event once the window elapses.
type Debouncer struct {
	interval  time.Duration
	publishFn func(StatusEvent)
	mu        sync.Mutex
	wg        sync.WaitGroup
	instances map[string]*instanceState
	stopped   bool
}

// NewDebouncer creates a Debouncer with the given interval and publish callback.
func NewDebouncer(interval time.Duration, publishFn func(StatusEvent)) *Debouncer {
	if publishFn == nil {
		panic("status debouncer: publishFn must not be nil")
	}
	return &Debouncer{
		interval:  interval,
		publishFn: publishFn,
		instances: make(map[string]*instanceState),
	}
}

// Submit queues a status event for the given instance. If another event
// arrives within the debounce window, the previous event is replaced and the
// window is reset. Only the latest pending event is published when the timer fires.
func (d *Debouncer) Submit(instanceID string, event StatusEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return
	}

	state, exists := d.instances[instanceID]
	if !exists {
		state = &instanceState{}
		d.instances[instanceID] = state
	}

	state.pending = event
	state.ver++

	if state.draining {
		return
	}

	if state.timer != nil {
		state.timer.Stop()
	}

	ver := state.ver
	state.timer = time.AfterFunc(d.interval, func() {
		d.runPublish(instanceID, ver)
	})
}

func (d *Debouncer) runPublish(instanceID string, ver uint64) {
	event, ok := d.beginPublish(instanceID, ver)
	if !ok {
		return
	}

	d.publishFn(event)
	d.endPublish(instanceID, ver)
}

func (d *Debouncer) beginPublish(instanceID string, ver uint64) (StatusEvent, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return StatusEvent{}, false
	}
	state, exists := d.instances[instanceID]
	if !exists || state.ver != ver || state.draining {
		return StatusEvent{}, false
	}

	state.timer = nil
	state.draining = true
	d.wg.Add(1)
	return state.pending, true
}

func (d *Debouncer) endPublish(instanceID string, publishedVer uint64) {
	var followUpVer uint64
	var stopFlush *StatusEvent

	func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		defer d.wg.Done()

		state, exists := d.instances[instanceID]
		if !exists {
			return
		}
		state.draining = false

		if state.ver > publishedVer {
			if d.stopped {
				ev := state.pending
				stopFlush = &ev
				delete(d.instances, instanceID)
			} else {
				followUpVer = state.ver
			}
			return
		}

		delete(d.instances, instanceID)
	}()

	if stopFlush != nil {
		d.publishFn(*stopFlush)
	}
	if followUpVer != 0 {
		d.runPublish(instanceID, followUpVer)
	}
}

// Stop halts the debouncer: new submissions are ignored, pending timers are
// cancelled, and each instance's latest pending event is flushed via publishFn.
// Stop waits for those flushes and any in-flight publishFn callbacks before returning.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	d.stopped = true
	toFlush := make([]StatusEvent, 0, len(d.instances))
	for id, state := range d.instances {
		if state.timer != nil {
			state.timer.Stop()
			state.timer = nil
		}
		if state.draining {
			continue
		}
		toFlush = append(toFlush, state.pending)
		delete(d.instances, id)
	}
	d.mu.Unlock()

	for _, event := range toFlush {
		d.publishFn(event)
	}
	d.wg.Wait()
}
