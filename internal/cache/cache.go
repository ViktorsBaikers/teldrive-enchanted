package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

type Cacher interface {
	Get(ctx context.Context, key string, value any) error
	Set(ctx context.Context, key string, value any, expiration time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	DeletePattern(ctx context.Context, pattern string) error
}

type MemoryCache struct {
	cache  *freecache.Cache
	prefix string
	mu     sync.RWMutex
}

var fetchGroup singleflight.Group

const staleRefreshTimeout = 10 * time.Second

// NewCache creates a new cache instance.
// If redisClient is provided, uses Redis; otherwise falls back to in-memory cache.
func NewCache(ctx context.Context, maxSize int, redisClient *redis.Client, lg *zap.Logger) Cacher {
	if redisClient != nil {
		lg.Debug("using cache", zap.String("type", "redis"))
		return NewRedisCache(redisClient)
	}
	lg.Debug("using cache", zap.String("type", "memory"), zap.Int("size", maxSize))
	return NewMemoryCache(maxSize)
}

func NewMemoryCache(size int) *MemoryCache {
	return &MemoryCache{
		cache:  freecache.NewCache(size),
		prefix: "teldrive:",
	}
}

func (m *MemoryCache) Get(ctx context.Context, key string, value any) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key = m.prefix + key
	data, err := m.cache.Get([]byte(key))
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(data, value)
}

func (m *MemoryCache) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key = m.prefix + key
	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	return m.cache.Set([]byte(key), data, int(expiration.Seconds()))
}

func (m *MemoryCache) Delete(ctx context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		m.cache.Del([]byte(m.prefix + key))
		m.cache.Del([]byte(m.prefix + Key(key, "stale")))
	}
	return nil
}

func (m *MemoryCache) DeletePattern(ctx context.Context, pattern string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pattern = m.prefix + pattern
	iter := m.cache.NewIterator()
	for {
		entry := iter.Next()
		if entry == nil {
			break
		}
		key := string(entry.Key)
		if matched, _ := filepath.Match(pattern, key); matched {
			m.cache.Del(entry.Key)
		}
	}
	return nil
}

type RedisCache struct {
	client *redis.Client
	prefix string
}

func NewRedisCache(client *redis.Client) *RedisCache {
	return &RedisCache{
		client: client,
		prefix: "teldrive:",
	}
}

func (r *RedisCache) Get(ctx context.Context, key string, value any) error {
	key = r.prefix + key
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(data, value)
}

func (r *RedisCache) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	key = r.prefix + key
	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, expiration).Err()
}

func (r *RedisCache) Delete(ctx context.Context, keys ...string) error {
	redisKeys := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		redisKeys = append(redisKeys, r.prefix+key, r.prefix+Key(key, "stale"))
	}
	return r.client.Del(ctx, redisKeys...).Err()
}

func (r *RedisCache) DeletePattern(ctx context.Context, pattern string) error {
	pattern = r.prefix + pattern
	iter := r.client.Scan(ctx, 0, pattern, 0).Iterator()
	var errs []error
	for iter.Next(ctx) {
		if err := r.client.Del(ctx, iter.Val()).Err(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := iter.Err(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func Fetch[T any](ctx context.Context, cache Cacher, key string, expiration time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero, value T
	err := cache.Get(ctx, key, &value)
	if err != nil {
		if isNotFound(err) {
			resultCh := fetchGroup.DoChan(key, func() (interface{}, error) {
				sharedCtx, cancel := fetchContext(ctx)
				defer cancel()

				var cachedValue T
				if getErr := cache.Get(sharedCtx, key, &cachedValue); getErr == nil {
					return cachedValue, nil
				}
				freshValue, execErr := fn(sharedCtx)
				if execErr != nil {
					return nil, execErr
				}
				_ = cache.Set(sharedCtx, key, &freshValue, expiration)
				return freshValue, nil
			})
			result, fetchErr := waitSingleflight(ctx, resultCh)
			if fetchErr != nil {
				return zero, fetchErr
			}
			typedValue, ok := result.(T)
			if !ok {
				return zero, fmt.Errorf("cache fetch type mismatch for key %s", key)
			}
			return typedValue, nil
		}
		return zero, err
	}
	return value, nil
}

func FetchWithStale[T any](
	ctx context.Context,
	cache Cacher,
	key string,
	expiration time.Duration,
	staleExpiration time.Duration,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero, value T
	if err := cache.Get(ctx, key, &value); err == nil {
		return value, nil
	} else if !isNotFound(err) {
		return zero, err
	}

	staleKey := Key(key, "stale")
	var staleValue T
	staleHit := cache.Get(ctx, staleKey, &staleValue) == nil

	if staleHit {
		go refreshStaleValue(cache, key, staleKey, expiration, staleExpiration, fn)
		return staleValue, nil
	}

	resultCh := fetchGroup.DoChan(key, func() (interface{}, error) {
		sharedCtx, cancel := fetchContext(ctx)
		defer cancel()

		var cachedValue T
		if getErr := cache.Get(sharedCtx, key, &cachedValue); getErr == nil {
			return cachedValue, nil
		}

		freshValue, execErr := fn(sharedCtx)
		if execErr != nil {
			return nil, execErr
		}

		_ = cache.Set(sharedCtx, key, &freshValue, expiration)
		_ = cache.Set(sharedCtx, staleKey, &freshValue, maxDuration(expiration, staleExpiration))

		return freshValue, nil
	})
	result, fetchErr := waitSingleflight(ctx, resultCh)

	if fetchErr != nil {
		return zero, fetchErr
	}

	typedValue, ok := result.(T)
	if !ok {
		return zero, fmt.Errorf("cache fetch type mismatch for key %s", key)
	}

	return typedValue, nil
}

func refreshStaleValue[T any](
	cache Cacher,
	key string,
	staleKey string,
	expiration time.Duration,
	staleExpiration time.Duration,
	fn func(context.Context) (T, error),
) {
	_, _, _ = fetchGroup.Do(Key(key, "stale", "refresh"), func() (interface{}, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), staleRefreshTimeout)
		defer cancel()

		var cachedValue T
		if getErr := cache.Get(refreshCtx, key, &cachedValue); getErr == nil {
			return cachedValue, nil
		}

		freshValue, err := fn(refreshCtx)
		if err != nil {
			return nil, err
		}

		_ = cache.Set(refreshCtx, key, &freshValue, expiration)
		_ = cache.Set(refreshCtx, staleKey, &freshValue, maxDuration(expiration, staleExpiration))
		return freshValue, nil
	})
}

func isNotFound(err error) bool {
	return errors.Is(err, freecache.ErrNotFound) || errors.Is(err, redis.Nil)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a >= b {
		return a
	}
	return b
}

func fetchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.Background(), func() {}
	}

	base := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(base, deadline)
	}
	return base, func() {}
}

func waitSingleflight(ctx context.Context, ch <-chan singleflight.Result) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		return result.Val, result.Err
	}
}

func FetchArg[T any, A any](
	ctx context.Context,
	cache Cacher,
	key string,
	expiration time.Duration,
	fn func(a A) (T, error), a A) (T, error) {
	return Fetch(ctx, cache, key, expiration, func(context.Context) (T, error) {
		return fn(a)
	})
}

func Key(args ...any) string {
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = formatValue(arg)
	}
	return strings.Join(parts, ":")
}

func formatValue(v any) string {
	if v == nil {
		return "nil"
	}

	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Pointer:
		if val.IsNil() {
			return "nil"
		}
		return formatValue(val.Elem().Interface())
	case reflect.Array, reflect.Slice:
		parts := make([]string, val.Len())
		for i := 0; i < val.Len(); i++ {
			parts[i] = formatValue(val.Index(i).Interface())
		}
		return fmt.Sprintf("[%s]", strings.Join(parts, ","))
	case reflect.Map:
		parts := make([]string, 0, val.Len())
		for _, key := range val.MapKeys() {
			k := formatValue(key.Interface())
			v := formatValue(val.MapIndex(key).Interface())
			parts = append(parts, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(parts)
		return fmt.Sprintf("{%s}", strings.Join(parts, ","))
	case reflect.Struct:
		return fmt.Sprintf("%+v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
