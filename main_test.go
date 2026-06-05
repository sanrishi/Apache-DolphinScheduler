package main

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

// TestRegistryClient_Lifecycle verifies that listeners can be added, removed,
// counted, and bulk-cleaned via Close without leaking entries.
func TestRegistryClient_Lifecycle(t *testing.T) {
	rc := NewRegistryClient()

	if got := rc.ListenerCount(); got != 0 {
		t.Fatalf("expected 0 listeners initially, got %d", got)
	}

	rc.AddListener("a", func() {})
	rc.AddListener("b", func() {})
	rc.AddListener("c", func() {})
	if got := rc.ListenerCount(); got != 3 {
		t.Fatalf("expected 3 listeners after add, got %d", got)
	}

	rc.RemoveListener("b")
	if got := rc.ListenerCount(); got != 2 {
		t.Fatalf("expected 2 listeners after remove, got %d", got)
	}

	rc.Close()
	if got := rc.ListenerCount(); got != 0 {
		t.Fatalf("expected 0 listeners after Close, got %d", got)
	}

	// Verify Close is idempotent
	rc.Close()
	if got := rc.ListenerCount(); got != 0 {
		t.Fatalf("expected 0 listeners after second Close, got %d", got)
	}
}

// TestFailoverWorkflow_Cleanup verifies that after a successful workflow,
// the registry listener has been removed and no resources leak.
func TestFailoverWorkflow_Cleanup(t *testing.T) {
	registry := NewRegistryClient()
	defer registry.Close()

	svc := NewFailoverService(registry)
	ctx := context.Background()

	if err := svc.FailoverWorkflow(ctx, 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if remaining := registry.ListenerCount(); remaining != 0 {
		t.Fatalf("listener was not removed after FailoverWorkflow; %d remaining", remaining)
	}
}

// TestFailoverWorkflow_CancelledContext verifies that when the context is
// already cancelled, FailoverWorkflow returns early without registering
// any listeners or allocating resources.
func TestFailoverWorkflow_CancelledContext(t *testing.T) {
	registry := NewRegistryClient()
	defer registry.Close()

	svc := NewFailoverService(registry)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := svc.FailoverWorkflow(ctx, 99); err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}

	if remaining := registry.ListenerCount(); remaining != 0 {
		t.Fatalf("no listeners should be registered with cancelled context; %d remaining", remaining)
	}
}

// TestConcurrentFailover_MemoryStability runs many workflow instances
// concurrently and verifies that:
//   - All listeners are deregistered after completion
//   - Memory returns to near-baseline levels after GC
//   - No goroutines are leaked (WaitGroup completes)
func TestConcurrentFailover_MemoryStability(t *testing.T) {
	registry := NewRegistryClient()
	defer registry.Close()

	svc := NewFailoverService(registry)
	ctx := context.Background()

	const iterations = 500
	const maxConcurrency = 10

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	for i := 0; i < iterations; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := svc.FailoverWorkflow(ctx, id); err != nil {
				t.Errorf("FailoverWorkflow(%d) error: %v", id, err)
			}
		}(int64(i))
	}
	wg.Wait()

	if remaining := registry.ListenerCount(); remaining != 0 {
		t.Fatalf("%d registry listeners leaked after concurrent workflows", remaining)
	}

	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	ratio := float64(final.Alloc) / float64(baseline.Alloc+1)
	t.Logf("Baseline: %d KiB, Final: %d KiB, Ratio: %.2fx",
		baseline.Alloc/1024, final.Alloc/1024, ratio)

	if ratio > 1.5 {
		t.Fatalf("memory retention ratio %.2fx exceeds threshold (1.5x)", ratio)
	}
}

// TestBoundedConcurrency_Semaphore verifies that the channel-based semaphore
// restricts the number of simultaneously executing goroutines to maxConcurrency.
func TestBoundedConcurrency_Semaphore(t *testing.T) {
	const maxConcurrency = 5
	sem := make(chan struct{}, maxConcurrency)

	var mu sync.Mutex
	var peak int
	var running int

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			mu.Lock()
			running++
			if running > peak {
				peak = running
			}
			mu.Unlock()

			// Simulate work
			runtime.Gosched()

			mu.Lock()
			running--
			mu.Unlock()
		}()
	}
	wg.Wait()

	if peak > maxConcurrency {
		t.Fatalf("peak concurrency %d exceeds max %d", peak, maxConcurrency)
	}
	t.Logf("Max concurrency observed: %d (limit: %d)", peak, maxConcurrency)
}
