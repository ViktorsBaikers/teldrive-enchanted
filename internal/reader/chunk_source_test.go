package reader

import (
	"context"
	stderrors "errors"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
)

func TestChunkSource_LocationLookupFailurePropagates(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	var locationCalls atomic.Int32
	getLocation = func(context.Context, *tg.Client, cache.Cacher, int64, int64) (*tg.InputDocumentFileLocation, error) {
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
		t.Fatalf("expected second chunk read to fail")
	}

	// Without negative caching, each call retries the upstream lookup
	if got := locationCalls.Load(); got != 2 {
		t.Fatalf("expected two upstream location lookups, got %d", got)
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

	getLocation = func(context.Context, *tg.Client, cache.Cacher, int64, int64) (*tg.InputDocumentFileLocation, error) {
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

func TestChunkSource_LocationFailurePreservesDeadlineExceeded(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	getLocation = func(context.Context, *tg.Client, cache.Cacher, int64, int64) (*tg.InputDocumentFileLocation, error) {
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

	_, err := src.Chunk(context.Background(), 0, 1024)
	if err == nil {
		t.Fatalf("expected chunk read to fail")
	}
	if !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected error to preserve deadline exceeded type, got %v", err)
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

	getLocation = func(context.Context, *tg.Client, cache.Cacher, int64, int64) (*tg.InputDocumentFileLocation, error) {
		locationCalls.Add(1)
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

	// Each call should fall through to getLocation since cache.Get fails
	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatalf("expected successful chunk read despite cache errors, got %v", err)
	}
	if _, err := src.Chunk(context.Background(), 0, 1024); err != nil {
		t.Fatalf("expected successful chunk read despite cache errors, got %v", err)
	}

	// Without singleflight, each call hits getLocation independently
	if got := locationCalls.Load(); got != 2 {
		t.Fatalf("expected two location lookups (no singleflight), got %d", got)
	}
	if got := chunkCalls.Load(); got != 2 {
		t.Fatalf("expected two chunk fetches, got %d", got)
	}
}

func TestChunkSource_PositiveCacheHitSkipsLookup(t *testing.T) {
	origGetLocation := getLocation
	origGetChunk := getChunk
	defer func() {
		getLocation = origGetLocation
		getChunk = origGetChunk
	}()

	getLocationCalls := atomic.Int32{}
	getLocation = func(context.Context, *tg.Client, cache.Cacher, int64, int64) (*tg.InputDocumentFileLocation, error) {
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
	key := "loc-positive-cache"
	location := tg.InputDocumentFileLocation{
		ID:            1234,
		AccessHash:    5678,
		FileReference: []byte{1, 2, 3},
	}

	if err := cacheStore.Set(context.Background(), key, &location, locationCacheTTL); err != nil {
		t.Fatalf("failed to seed positive location cache: %v", err)
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
