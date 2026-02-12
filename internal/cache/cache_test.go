package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coocood/freecache"
	"github.com/stretchr/testify/assert"
	"github.com/tgdrive/teldrive/pkg/models"
)

type testContextKey string

func TestCache(t *testing.T) {
	ctx := context.Background()
	var value = models.File{
		Name: "file.jpeg",
		Type: "file",
	}
	var result models.File

	cache := NewMemoryCache(1 * 1024 * 1024)

	err := cache.Set(ctx, "key", value, 1*time.Second)
	assert.NoError(t, err)

	err = cache.Get(ctx, "key", &result)
	assert.NoError(t, err)
	assert.Equal(t, result, value)
}

func TestKey(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected string
	}{
		{
			name:     "simple strings",
			args:     []any{"user", "123"},
			expected: "user:123",
		},
		{
			name:     "mixed types",
			args:     []any{"cache", 123, true},
			expected: "cache:123:true",
		},
		{
			name:     "with nil",
			args:     []any{"key", nil, "value"},
			expected: "key:nil:value",
		},
		{
			name:     "empty args",
			args:     []any{},
			expected: "",
		},
		{
			name:     "single arg",
			args:     []any{"solo"},
			expected: "solo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Key(tt.args...)
			if result != tt.expected {
				t.Errorf("Key() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "nil",
		},
		{
			name:     "string",
			input:    "test",
			expected: "test",
		},
		{
			name:     "integer",
			input:    123,
			expected: "123",
		},
		{
			name:     "int64",
			input:    int64(9876543210),
			expected: "9876543210",
		},
		{
			name:     "boolean",
			input:    true,
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFetch_CoalescesConcurrentMisses(t *testing.T) {
	ctx := context.Background()
	c := NewMemoryCache(1 * 1024 * 1024)

	const (
		key      = "fetch:coalesce"
		expected = 42
		workers  = 8
	)

	start := make(chan struct{})
	var calls atomic.Int32

	var wg sync.WaitGroup
	results := make(chan int, workers)
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := Fetch(ctx, c, key, time.Minute, func(context.Context) (int, error) {
				calls.Add(1)
				<-start
				return expected, nil
			})
			if err != nil {
				errs <- err
				return
			}
			results <- value
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
	}
	for value := range results {
		if value != expected {
			t.Fatalf("expected value %d, got %d", expected, value)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one fetch execution, got %d", got)
	}
}

func TestFetchWithStale_ReturnsStaleOnError(t *testing.T) {
	ctx := context.Background()
	c := NewMemoryCache(1 * 1024 * 1024)

	const key = "fetch:stale:error"
	staleValue := 7

	if err := c.Set(ctx, Key(key, "stale"), &staleValue, time.Minute); err != nil {
		t.Fatalf("failed to seed stale cache: %v", err)
	}

	value, err := FetchWithStale(ctx, c, key, time.Second, time.Minute, func(context.Context) (int, error) {
		return 0, assert.AnError
	})
	if err != nil {
		t.Fatalf("expected stale value without error, got %v", err)
	}
	if value != staleValue {
		t.Fatalf("expected stale value %d, got %d", staleValue, value)
	}
}

func TestFetchWithStale_RevalidatesInBackground(t *testing.T) {
	ctx := context.Background()
	c := NewMemoryCache(1 * 1024 * 1024)

	const key = "fetch:stale:refresh"
	staleValue := 10
	freshValue := 20

	if err := c.Set(ctx, Key(key, "stale"), &staleValue, time.Minute); err != nil {
		t.Fatalf("failed to seed stale cache: %v", err)
	}

	var calls atomic.Int32
	value, err := FetchWithStale(ctx, c, key, time.Second, time.Minute, func(context.Context) (int, error) {
		calls.Add(1)
		return freshValue, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != staleValue {
		t.Fatalf("expected stale value %d, got %d", staleValue, value)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		var cached int
		cacheErr := c.Get(ctx, key, &cached)
		if cacheErr == nil && cached == freshValue {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fresh value was not revalidated in background")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one revalidation call, got %d", got)
	}
}

func TestDelete_RemovesPrimaryAndStaleEntries(t *testing.T) {
	ctx := context.Background()
	c := NewMemoryCache(1 * 1024 * 1024)

	const key = "fetch:delete:stale"
	value := 1
	staleValue := 2

	if err := c.Set(ctx, key, &value, time.Minute); err != nil {
		t.Fatalf("failed to seed primary cache: %v", err)
	}
	if err := c.Set(ctx, Key(key, "stale"), &staleValue, time.Minute); err != nil {
		t.Fatalf("failed to seed stale cache: %v", err)
	}

	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	var got int
	if err := c.Get(ctx, key, &got); !errors.Is(err, freecache.ErrNotFound) {
		t.Fatalf("expected primary key to be deleted, got err=%v", err)
	}
	if err := c.Get(ctx, Key(key, "stale"), &got); !errors.Is(err, freecache.ErrNotFound) {
		t.Fatalf("expected stale key to be deleted, got err=%v", err)
	}
}

func TestFetch_DoesNotPropagateLeaderCancellationToSharedWork(t *testing.T) {
	cacheStore := NewMemoryCache(1 * 1024 * 1024)
	key := "fetch:leader-cancel"

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()

	start := make(chan struct{})
	var calls atomic.Int32

	leaderErrCh := make(chan error, 1)
	go func() {
		_, err := Fetch(leaderCtx, cacheStore, key, time.Minute, func(fetchCtx context.Context) (int, error) {
			calls.Add(1)
			<-start
			if err := fetchCtx.Err(); err != nil {
				return 0, err
			}
			return 77, nil
		})
		leaderErrCh <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shared fetch to start")
		}
		time.Sleep(5 * time.Millisecond)
	}

	followerResultCh := make(chan int, 1)
	followerErrCh := make(chan error, 1)
	go func() {
		value, err := Fetch(context.Background(), cacheStore, key, time.Minute, func(fetchCtx context.Context) (int, error) {
			if err := fetchCtx.Err(); err != nil {
				return 0, err
			}
			return 88, nil
		})
		if err != nil {
			followerErrCh <- err
			return
		}
		followerResultCh <- value
	}()

	cancelLeader()
	close(start)

	leaderErr := <-leaderErrCh
	if !errors.Is(leaderErr, context.Canceled) {
		t.Fatalf("expected leader to observe context cancellation, got %v", leaderErr)
	}

	select {
	case err := <-followerErrCh:
		t.Fatalf("follower should succeed, got error %v", err)
	case value := <-followerResultCh:
		if value != 77 {
			t.Fatalf("expected follower to receive shared value 77, got %d", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for follower result")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one shared fetch execution, got %d", got)
	}
}

func TestFetchContext_PreservesDeadlineAndValues(t *testing.T) {
	parent := context.WithValue(context.Background(), testContextKey("k"), "v")
	ctx, cancel := context.WithTimeout(parent, 200*time.Millisecond)
	defer cancel()

	fetchCtx, fetchCancel := fetchContext(ctx)
	defer fetchCancel()

	wantDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("expected source context to have deadline")
	}
	gotDeadline, ok := fetchCtx.Deadline()
	if !ok {
		t.Fatalf("expected fetch context to preserve deadline")
	}
	if !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("deadline mismatch: want %v got %v", wantDeadline, gotDeadline)
	}
	if got := fetchCtx.Value(testContextKey("k")); got != "v" {
		t.Fatalf("expected fetch context to preserve values, got %v", got)
	}

	cancel()

	select {
	case <-fetchCtx.Done():
		t.Fatalf("fetch context should not be canceled by parent cancellation before deadline")
	default:
	}
}
