package reader

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/cache"
)

func TestChunkSource_CachesLocationLookupFailures(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	var locationCalls atomic.Int32
	getLocation = func(context.Context, *tg.Client, int64, int64) (*tg.InputDocumentFileLocation, error) {
		locationCalls.Add(1)
		return nil, stderrors.New("rpcDoRequest: rpc error code -503: Timeout")
	}
	getChunk = func(context.Context, *tg.Client, tg.InputFileLocationClass, int64, int64) ([]byte, error) {
		t.Fatalf("chunk fetch should not run when location lookup fails")
		return nil, nil
	}

	src := &chunkSource{
		channelId: 1,
		partId:    100,
		client:    nil,
		key:       "loc-failure-key",
		cache:     cache.NewMemoryCache(1 << 20),
	}

	if _, err := src.Chunk(context.Background(), 0, 1024); err == nil {
		t.Fatalf("expected first chunk read to fail")
	}
	if _, err := src.Chunk(context.Background(), 0, 1024); err == nil {
		t.Fatalf("expected second chunk read to fail from negative cache")
	}

	if got := locationCalls.Load(); got != 1 {
		t.Fatalf("expected one upstream location lookup, got %d", got)
	}
}

func TestChunkSource_UsesCachedLocationAfterFirstLookup(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	var (
		locationCalls atomic.Int32
		chunkCalls    atomic.Int32
	)

	getLocation = func(context.Context, *tg.Client, int64, int64) (*tg.InputDocumentFileLocation, error) {
		locationCalls.Add(1)
		return &tg.InputDocumentFileLocation{
			ID:            55,
			AccessHash:    66,
			FileReference: []byte{1},
		}, nil
	}
	getChunk = func(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, offset int64, limit int64) ([]byte, error) {
		chunkCalls.Add(1)
		loc, ok := location.(*tg.InputDocumentFileLocation)
		if !ok {
			t.Fatalf("expected InputDocumentFileLocation, got %T", location)
		}
		if loc.ID != 55 {
			t.Fatalf("expected cached location id 55, got %d", loc.ID)
		}
		return []byte("ok"), nil
	}

	src := &chunkSource{
		channelId: 2,
		partId:    200,
		client:    nil,
		key:       "loc-success-key",
		cache:     cache.NewMemoryCache(1 << 20),
	}

	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatalf("first chunk read failed: %v", err)
	}
	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatalf("second chunk read failed: %v", err)
	}

	if got := locationCalls.Load(); got != 1 {
		t.Fatalf("expected one upstream location lookup, got %d", got)
	}
	if got := chunkCalls.Load(); got != 2 {
		t.Fatalf("expected two chunk fetches, got %d", got)
	}
}

func TestChunkSource_CachedLocationFailurePreservesDeadlineExceeded(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	var locationCalls atomic.Int32
	getLocation = func(context.Context, *tg.Client, int64, int64) (*tg.InputDocumentFileLocation, error) {
		locationCalls.Add(1)
		return nil, context.DeadlineExceeded
	}
	getChunk = func(context.Context, *tg.Client, tg.InputFileLocationClass, int64, int64) ([]byte, error) {
		t.Fatalf("chunk fetch should not run when location lookup fails")
		return nil, nil
	}

	src := &chunkSource{
		channelId: 1,
		partId:    300,
		client:    nil,
		key:       "loc-timeout-key",
		cache:     cache.NewMemoryCache(1 << 20),
	}

	if _, err := src.Chunk(context.Background(), 0, 1024); err == nil {
		t.Fatalf("expected first chunk read to fail")
	}

	_, err := src.Chunk(context.Background(), 0, 1024)
	if err == nil {
		t.Fatalf("expected second chunk read to fail from negative cache")
	}
	if !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cached error to preserve deadline exceeded type, got %v", err)
	}

	if got := locationCalls.Load(); got != 1 {
		t.Fatalf("expected one upstream location lookup, got %d", got)
	}
}

type errorOnGetCache struct {
	cache.Cacher
	key string
}

func (c *errorOnGetCache) Get(ctx context.Context, key string, value any) error {
	if key == c.key {
		return stderrors.New("redis: connection reset by peer")
	}
	return c.Cacher.Get(ctx, key, value)
}

func TestChunkSource_TreatsCacheErrorsAsMisses(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	var (
		locationCalls atomic.Int32
		chunkCalls    atomic.Int32
	)
	lookupStarted := make(chan struct{}, 1)
	releaseLookup := make(chan struct{})

	getLocation = func(context.Context, *tg.Client, int64, int64) (*tg.InputDocumentFileLocation, error) {
		locationCalls.Add(1)
		select {
		case lookupStarted <- struct{}{}:
		default:
		}
		<-releaseLookup
		return &tg.InputDocumentFileLocation{
			ID:            999,
			AccessHash:    123,
			FileReference: []byte{1, 2, 3},
		}, nil
	}
	getChunk = func(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, offset int64, limit int64) ([]byte, error) {
		chunkCalls.Add(1)
		loc, ok := location.(*tg.InputDocumentFileLocation)
		if !ok {
			t.Fatalf("expected InputDocumentFileLocation, got %T", location)
		}
		if loc.ID != 999 {
			t.Fatalf("expected fallback location id 999, got %d", loc.ID)
		}
		return []byte("ok"), nil
	}

	cacheKey := "loc-cache-error-key"
	src := &chunkSource{
		channelId: 3,
		partId:    400,
		client:    nil,
		key:       cacheKey,
		cache: &errorOnGetCache{
			Cacher: cache.NewMemoryCache(1 << 20),
			key:    cacheKey,
		},
	}

	const workers = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := src.Chunk(context.Background(), 0, 1024)
			errs <- err
		}()
	}

	close(start)
	<-lookupStarted
	// Give waiting callers a short window to join the in-flight singleflight call.
	time.Sleep(25 * time.Millisecond)
	close(releaseLookup)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("expected successful chunk read despite cache errors, got %v", err)
		}
	}

	if got := locationCalls.Load(); got != 1 {
		t.Fatalf("expected singleflight to coalesce fallback lookups, got %d calls", got)
	}
	if got := chunkCalls.Load(); got != workers {
		t.Fatalf("expected %d chunk fetches, got %d", workers, got)
	}
}

func TestChunkSource_PrefersPositiveCacheOverNegativeMarker(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	getLocationCalls := atomic.Int32{}
	getLocation = func(context.Context, *tg.Client, int64, int64) (*tg.InputDocumentFileLocation, error) {
		getLocationCalls.Add(1)
		t.Fatalf("getLocation should not be called when positive cache entry exists")
		return nil, nil
	}

	chunkCalls := atomic.Int32{}
	getChunk = func(ctx context.Context, client *tg.Client, location tg.InputFileLocationClass, offset int64, limit int64) ([]byte, error) {
		chunkCalls.Add(1)
		loc, ok := location.(*tg.InputDocumentFileLocation)
		if !ok {
			t.Fatalf("expected InputDocumentFileLocation, got %T", location)
		}
		if loc.ID != 1234 {
			t.Fatalf("expected cached location id 1234, got %d", loc.ID)
		}
		return []byte("ok"), nil
	}

	cacheStore := cache.NewMemoryCache(1 << 20)
	key := "loc-positive-and-negative"
	location := tg.InputDocumentFileLocation{
		ID:            1234,
		AccessHash:    5678,
		FileReference: []byte{1, 2, 3},
	}

	if err := cacheStore.Set(context.Background(), key, &location, locationCacheTTL); err != nil {
		t.Fatalf("failed to seed positive location cache: %v", err)
	}
	negative := locationLookupFailure{Message: "transient timeout", DeadlineExceeded: true}
	if err := cacheStore.Set(context.Background(), cache.Key(key, "neg"), &negative, locationNegativeCacheTTL); err != nil {
		t.Fatalf("failed to seed negative cache marker: %v", err)
	}

	src := &chunkSource{
		channelId: 9,
		partId:    900,
		client:    nil,
		key:       key,
		cache:     cacheStore,
	}

	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatalf("expected chunk read to succeed using positive cache, got %v", err)
	}
	if got := chunkCalls.Load(); got != 1 {
		t.Fatalf("expected one chunk fetch, got %d", got)
	}
	if got := getLocationCalls.Load(); got != 0 {
		t.Fatalf("expected zero location lookups, got %d", got)
	}
}
