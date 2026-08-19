package routing_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dcm-project/environment-agent/internal/routing"
)

// TestResourceSet_ConcurrentAddIfAbsentRemove_NoRace stresses the bounded LRU
// set (claimedResourcesSet's underlying type) with concurrent
// Add/AddIfAbsent/Remove/Contains across a small key space, so any future
// refactor that weakens the mutex gets caught by `go test -race`. (UT-RTE-046)
func TestResourceSet_ConcurrentAddIfAbsentRemove_NoRace(t *testing.T) {
	const maxSize = 10
	rs := routing.NewResourceSet(maxSize)
	const goroutines = 50
	const itersPerGoroutine = 500
	const keySpace = 20 // > maxSize, so the LRU eviction path actually runs

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				key := "res-" + string(rune('A'+(g+i)%keySpace))
				switch i % 4 {
				case 0:
					rs.Add(key)
				case 1:
					rs.AddIfAbsent(key)
				case 2:
					rs.Contains(key)
				case 3:
					rs.Remove(key)
				}
			}
		}(g)
	}
	wg.Wait()

	if got := rs.Len(); got > maxSize {
		t.Fatalf("ResourceSet grew past maxSize: len=%d maxSize=%d", got, maxSize)
	}
}

// TestKeyLock_ReverseRaceOrdering_MutualExclusion stresses KeyLock's
// mutual-exclusion invariant (REQ-RTE-210's in-flight double-dispatch guard):
// many goroutines continuously race to claim-then-release the same key. If
// AddIfAbsent/Remove ever let two goroutines believe they both hold the lock
// at once, this test catches it directly (not just via `go test -race`,
// which wouldn't flag a logic bug here). (UT-RTE-047)
func TestKeyLock_ReverseRaceOrdering_MutualExclusion(t *testing.T) {
	kl := routing.NewKeyLock()
	const key = "res-race"
	const goroutines = 50
	const itersPerGoroutine = 500

	var holders int32
	var violations int32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				if !kl.AddIfAbsent(key) {
					continue // lost the race this round; expected under contention
				}
				if atomic.AddInt32(&holders, 1) > 1 {
					atomic.AddInt32(&violations, 1)
				}
				runtime.Gosched() // widen the window so an overlapping holder isn't missed
				atomic.AddInt32(&holders, -1)
				kl.Remove(key)
			}
		}()
	}
	wg.Wait()

	if v := atomic.LoadInt32(&violations); v != 0 {
		t.Fatalf("KeyLock let %d claim(s) overlap for the same key — mutual exclusion violated", v)
	}
	if kl.Len() != 0 {
		t.Fatalf("KeyLock left %d entries held after all goroutines released", kl.Len())
	}
}
