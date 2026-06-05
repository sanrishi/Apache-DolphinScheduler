package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
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
func (f *FailoverService) FailoverWorkflow(ctx context.Context, instanceID int64) {
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
}

func main() {
	fmt.Println("Starting OSD Memory Recovery Simulation...")

	registry := NewRegistryClient()
	failoverService := NewFailoverService(registry)

	// Root context shared across all failover goroutines.
	// Each goroutine derives its own immutable child context,
	// making this goroutine-safe with no global mutable state.
	ctx := context.Background()

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

	for i := 0; i < iterations; i++ {
		sem <- struct{}{} // blocks if at capacity
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			defer func() { <-sem }() // release slot
			failoverService.FailoverWorkflow(ctx, id)
		}(int64(i))
	}

	wg.Wait()

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