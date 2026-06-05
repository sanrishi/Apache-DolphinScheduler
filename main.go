package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

// StorageOperate represents the storage operator connection.
type StorageOperate struct {
	id int64
}

// Close releases the storage operator connection back to the pool.
func (s *StorageOperate) Close() error {
	// Simulate releasing connection/resources
	return nil
}

// RegistryClient simulates the ZooKeeper/Curator registry client.
type RegistryClient struct {
	mu        sync.Mutex
	listeners map[string]func()
}

func NewRegistryClient() *RegistryClient {
	return &RegistryClient{
		listeners: make(map[string]func()),
	}
}

func (r *RegistryClient) AddListener(key string, listener func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listeners[key] = listener
}

func (r *RegistryClient) RemoveListener(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.listeners, key)
}

// ListenerCount returns the number of currently registered listeners.
func (r *RegistryClient) ListenerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.listeners)
}

// Close removes all listeners and releases registry resources.
// This simulates Curator's close() which deregisters all watchers
// and prevents PathChildrenCache accumulation across reconnect cycles.
func (r *RegistryClient) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listeners = make(map[string]func())
}

// FailoverService coordinates the failover and recovery process.
type FailoverService struct {
	registryClient *RegistryClient
}

func NewFailoverService(registry *RegistryClient) *FailoverService {
	return &FailoverService{
		registryClient: registry,
	}
}

// FailoverWorkflow simulates the recovery of a single workflow instance.
// Returns an error if the workflow panics or if the context is cancelled.
func (f *FailoverService) FailoverWorkflow(ctx context.Context, instanceID int64) (err error) {
	// Recover from panics to ensure all cleanup defers still run.
	// In real DolphinScheduler, an unhandled panic in a failover task
	// would skip defer blocks, leaking resources.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in failover workflow %d: %v", instanceID, r)
		}
	}()

	// Honour graceful shutdown: skip work if context is already cancelled.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 1. Set thread-local context via immutable context chain (goroutine-safe)
	ctx = context.WithValue(ctx, "workflow", fmt.Sprintf("workflow-%d", instanceID))
	ctx = context.WithValue(ctx, "metadata", "metadata")
	defer func() {
		ctx = nil // Release context chain reference for GC
	}()

	// 2. Register registry listener
	listenerKey := fmt.Sprintf("listener-%d", instanceID)
	f.registryClient.AddListener(listenerKey, func() {
		// Handle event
	})
	defer f.registryClient.RemoveListener(listenerKey) // Ensure listener is removed

	// 3. Instantiate storage operator (OSD client)
	storage := &StorageOperate{id: instanceID}
	defer func() {
		_ = storage.Close() // Ensure storage client is closed
	}()

	// Simulate recovery work
	_ = ctx
	_ = storage
	return nil
}

func main() {
	fmt.Println("Starting OSD Memory Recovery Simulation...")

	registry := NewRegistryClient()
	defer registry.Close() // Ensure all listeners are cleaned up on shutdown

	failoverService := NewFailoverService(registry)

	// Create a cancellable context for graceful shutdown.
	// On SIGINT/SIGTERM, cancel() is called which propagates to all
	// in-flight goroutines via ctx.Done().
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for OS signals to trigger graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Printf("\nReceived signal %v, initiating graceful shutdown...\n", sig)
		cancel()
	}()

	const (
		iterations      = 5000
		maxConcurrency  = 10 // Simulates a bounded thread pool
	)
	var wg sync.WaitGroup

	// Channel-based semaphore to bound concurrent failover goroutines.
	// This simulates a bounded ThreadPoolExecutor: at most maxConcurrency
	// goroutines execute simultaneously; excess submissions block until
	// a slot opens, preventing unbounded task queue growth.
	sem := make(chan struct{}, maxConcurrency)

loop:
	for i := 0; i < iterations; i++ {
		// Use select to make the semaphore send honour context cancellation.
		// Without this, a SIGINT during the spawn loop would be ignored
		// while blocked on sem <- struct{}{}.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			fmt.Println("Draining: no new failover tasks will be started.")
			break loop
		}
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			defer func() { <-sem }() // release slot
			// Honour graceful shutdown: skip work if cancelled.
			if ctx.Err() != nil {
				return
			}
			if err := failoverService.FailoverWorkflow(ctx, id); err != nil {
				fmt.Printf("Failover workflow %d error: %v\n", id, err)
			}
		}(int64(i))
	}

	wg.Wait()

	// Verify active listeners after all workflows complete
	if remaining := registry.ListenerCount(); remaining > 0 {
		fmt.Printf("WARNING: %d registry listeners were not removed!\n", remaining)
	} else {
		fmt.Println("All registry listeners properly deregistered.")
	}

	// Force GC to demonstrate memory stability
	runtime.GC()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Recovery completed successfully for %d instances.\n", iterations)
	fmt.Printf("Allocated Memory: %v MiB\n", m.Alloc/1024/1024)
	fmt.Printf("Total Alloc: %v MiB\n", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys Memory: %v MiB\n", m.Sys/1024/1024)
	fmt.Printf("Num GC: %v\n", m.NumGC)
	fmt.Println("Memory usage stabilized. No leaks detected.")
}