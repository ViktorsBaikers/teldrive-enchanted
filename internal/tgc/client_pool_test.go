package tgc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ViktorsBaikers/teldrive/internal/config"
)

func TestClientPoolCreation(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	assert.NotNil(t, pool)
	assert.Equal(t, 0, pool.Len())
	assert.Equal(t, LeastConnections, pool.Strategy())
}

func TestClientPoolWithStrategy(t *testing.T) {
	pool := NewClientPool(nil, nil, &config.TGConfig{PoolRoutingStrategy: "round_robin"})
	assert.Equal(t, RoundRobin, pool.Strategy())
}

func TestSelectRoundRobin(t *testing.T) {
	pool := NewClientPool(nil, nil, &config.TGConfig{PoolRoutingStrategy: "round_robin"})

	clients := []*PooledClient{
		{Key: "c1"},
		{Key: "c2"},
		{Key: "c3"},
	}

	assert.Equal(t, "c1", pool.selectClient(clients).Key)
	assert.Equal(t, "c2", pool.selectClient(clients).Key)
	assert.Equal(t, "c3", pool.selectClient(clients).Key)
	assert.Equal(t, "c1", pool.selectClient(clients).Key)
}

func TestSelectLeastConnections(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	clients := []*PooledClient{
		{Key: "c1", Connections: 10},
		{Key: "c2", Connections: 5},
		{Key: "c3", Connections: 8},
	}

	selected := pool.selectClient(clients)
	assert.Equal(t, "c2", selected.Key)
}

func TestAcquireRelease(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	pool.clients.Store("client1", &PooledClient{})

	pool.Acquire("client1")
	assert.Equal(t, int64(1), pool.TotalConnections())

	pool.Acquire("client1")
	assert.Equal(t, int64(2), pool.TotalConnections())

	pool.Release("client1")
	assert.Equal(t, int64(1), pool.TotalConnections())

	pool.Release("client1")
	assert.Equal(t, int64(0), pool.TotalConnections())
}

func TestPoolStats(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	stats := pool.Stats()
	assert.Equal(t, 0, stats.TotalClients)
	assert.Equal(t, int64(0), stats.TotalConns)

	pool.clients.Store("test:bot", &PooledClient{Key: "test:bot", Connections: 5})

	// Acquire to increment both totalConns and pc.Connections
	pool.Acquire("test:bot")

	stats = pool.Stats()
	assert.Equal(t, 1, stats.TotalClients)
	assert.Equal(t, int64(1), stats.TotalConns)
	// Connections was 5, then Acquire added 1 atomically = 6
	assert.Equal(t, int64(6), stats.ClientStats["test:bot"])
}

func TestCloseEmptyPool(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	err := pool.Close()
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Len())
}

func TestClosePoolWithClients(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	pool.clients.Store("test", &PooledClient{
		stop: func() error { return nil },
	})

	err := pool.Close()
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Len())
}

func TestPoolIdleTimeout(t *testing.T) {
	pool := NewClientPool(nil, nil, &config.TGConfig{
		PoolIdleTimeout: 1 * time.Minute,
	})
	assert.Equal(t, 1*time.Minute, pool.idleTimeout)

	pool2 := NewClientPool(nil, nil, nil)
	assert.Equal(t, 30*time.Minute, pool2.idleTimeout)
}

func TestClientPool_UserIsolation(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	userIDs := []int64{1, 2, 3}
	bots := []string{"sharedBot"}

	for _, userID := range userIDs {
		key := fmt.Sprintf("user:%d:bot:%s", userID, bots[0])
		pool.clients.Store(key, &PooledClient{
			Key: key,
		})
	}

	assert.Equal(t, 3, pool.Len())

	keys := make([]string, 0, 3)
	pool.clients.Range(func(k, v any) bool {
		pc := v.(*PooledClient)
		keys = append(keys, pc.Key)
		return true
	})

	assert.Equal(t, 3, len(keys))
}

func TestPooledClient_Struct(t *testing.T) {
	pc := &PooledClient{
		Key:         "test",
		Connections: 5,
	}

	assert.Equal(t, "test", pc.Key)
	assert.Equal(t, int64(5), pc.Connections)
	assert.Nil(t, pc.Client)
	assert.Nil(t, pc.stop)
}

func TestRoutingStrategy_Values(t *testing.T) {
	assert.Equal(t, RoutingStrategy("round_robin"), RoundRobin)
	assert.Equal(t, RoutingStrategy("least_connections"), LeastConnections)
}

func TestClientPoolLen(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	assert.Equal(t, 0, pool.Len())

	pool.clients.Store("a", &PooledClient{})
	pool.clients.Store("b", &PooledClient{})

	assert.Equal(t, 2, pool.Len())
}

func TestClientPoolTotalConnections(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	assert.Equal(t, int64(0), pool.TotalConnections())

	pool.clients.Store("a", &PooledClient{Connections: 3})
	pool.clients.Store("b", &PooledClient{Connections: 7})

	// TotalConnections tracks Acquire/Release, not static values
	pool.Acquire("a")
	pool.Acquire("b")

	assert.Equal(t, int64(2), pool.TotalConnections())
}

func TestClientPoolConcurrentAccess(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.clients.Store("test", &PooledClient{Key: "test"})

			pool.Acquire("test")
			pool.Release("test")
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, pool.Len())
}

func TestGetBotClient_SkipsOpenCircuitBot(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	keyA := "user:1:bot:tokenA"
	keyB := "user:1:bot:tokenB"

	pool.clients.Store(keyA, &PooledClient{Key: keyA, IsReady: 1})
	pool.clients.Store(keyB, &PooledClient{Key: keyB, IsReady: 1})

	for i := 0; i < defaultBotCircuitFailureThreshold; i++ {
		pool.RecordBotFailure(keyA, fmt.Errorf("timeout"))
	}

	_, selectedKey, err := pool.GetBotClient(1, []string{"tokenA", "tokenB"})
	assert.NoError(t, err)
	assert.Equal(t, keyB, selectedKey)
}

func TestRecordBotSuccess_DoesNotCloseOpenCircuit(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	key := "user:1:bot:tokenA"

	for i := 0; i < defaultBotCircuitFailureThreshold; i++ {
		pool.RecordBotFailure(key, fmt.Errorf("timeout"))
	}
	assert.False(t, pool.isBotClientAvailable(key, time.Now()))

	pool.RecordBotSuccess(key)
	assert.False(t, pool.isBotClientAvailable(key, time.Now()))

	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.BotSuccesses[key])
	assert.Equal(t, int64(defaultBotCircuitFailureThreshold), stats.BotFailures[key])
	assert.Equal(t, int64(1), stats.BotCircuitTrips[key])
	assert.True(t, stats.BotCircuitOpen[key])
}

func TestRecordBotSuccess_ClosesCircuitAfterCooldownElapsed(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	key := "user:1:bot:tokenA"

	for i := 0; i < defaultBotCircuitFailureThreshold; i++ {
		pool.RecordBotFailure(key, fmt.Errorf("timeout"))
	}

	state := pool.getBotCircuitState(key)
	atomic.StoreInt64(&state.openUntilUnixNano, time.Now().Add(-time.Second).UnixNano())

	pool.RecordBotSuccess(key)
	assert.True(t, pool.isBotClientAvailable(key, time.Now()))
}

func TestGetBotClient_ReturnsErrorWhenAllBotCircuitsOpen(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	keyA := "user:1:bot:tokenA"
	keyB := "user:1:bot:tokenB"

	pool.clients.Store(keyA, &PooledClient{Key: keyA, IsReady: 1})
	pool.clients.Store(keyB, &PooledClient{Key: keyB, IsReady: 1})

	for i := 0; i < defaultBotCircuitFailureThreshold; i++ {
		pool.RecordBotFailure(keyA, fmt.Errorf("timeout"))
		pool.RecordBotFailure(keyB, fmt.Errorf("timeout"))
	}

	_, _, err := pool.GetBotClient(1, []string{"tokenA", "tokenB"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "temporarily unavailable")
}

func TestRemoveBotCandidate(t *testing.T) {
	candidates := []*PooledClient{
		{Key: "user:1:bot:a"},
		{Key: "user:1:bot:b"},
		{Key: "user:1:bot:c"},
	}
	remaining := removeBotCandidate(candidates, "user:1:bot:b")
	assert.Len(t, remaining, 2)
	assert.Equal(t, "user:1:bot:a", remaining[0].Key)
	assert.Equal(t, "user:1:bot:c", remaining[1].Key)
}

func TestRecordBotFailure_IgnoresContextCanceled(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	key := "user:1:bot:tokenA"

	for i := 0; i < defaultBotCircuitFailureThreshold+1; i++ {
		pool.RecordBotFailure(key, context.Canceled)
	}

	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.BotFailures[key])
	assert.Equal(t, int64(0), stats.BotCircuitTrips[key])
	assert.False(t, stats.BotCircuitOpen[key])
}

func TestRedactBotClientKey(t *testing.T) {
	assert.Equal(t, "user:1:bot:12345:***", redactBotClientKey("user:1:bot:12345:abcdef"))
	assert.Equal(t, "user:1", redactBotClientKey("user:1"))
}

func TestGetBotClient_CreateFailureFallsBackToNextCandidate(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)

	keyBad := "user:1:bot:bad"
	keyGood := "user:1:bot:good"

	pool.clients.Store(keyBad, &PooledClient{Key: keyBad})
	pool.clients.Store(keyGood, &PooledClient{Key: keyGood})

	pool.createBotClientFn = func(key, _ string) error {
		if key == keyBad {
			return fmt.Errorf("create failed")
		}
		return nil
	}

	_, selectedKey, err := pool.GetBotClient(1, []string{"bad", "good"})
	assert.NoError(t, err)
	assert.Equal(t, keyGood, selectedKey)

	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.BotFailures[keyBad])
}

func TestIsBotClientAvailable_ReopensAfterCooldown(t *testing.T) {
	pool := NewClientPool(nil, nil, nil)
	key := "user:1:bot:tokenA"

	for i := 0; i < defaultBotCircuitFailureThreshold; i++ {
		pool.RecordBotFailure(key, fmt.Errorf("timeout"))
	}
	assert.False(t, pool.isBotClientAvailable(key, time.Now()))

	state := pool.getBotCircuitState(key)
	atomic.StoreInt64(&state.openUntilUnixNano, time.Now().Add(-time.Second).UnixNano())

	assert.True(t, pool.isBotClientAvailable(key, time.Now()))
}

func TestClientPool_UsesConfiguredBotCircuitThreshold(t *testing.T) {
	pool := NewClientPool(nil, nil, &config.TGConfig{BotCircuitFailures: 2})
	key := "user:1:bot:tokenA"

	pool.RecordBotFailure(key, fmt.Errorf("timeout-1"))
	assert.True(t, pool.isBotClientAvailable(key, time.Now()))

	pool.RecordBotFailure(key, fmt.Errorf("timeout-2"))
	assert.False(t, pool.isBotClientAvailable(key, time.Now()))
}

func TestClientPool_UsesConfiguredBotCircuitCooldown(t *testing.T) {
	pool := NewClientPool(nil, nil, &config.TGConfig{
		BotCircuitFailures: 1,
		BotCircuitCooldown: 50 * time.Millisecond,
	})
	key := "user:1:bot:tokenB"

	pool.RecordBotFailure(key, fmt.Errorf("timeout"))
	assert.False(t, pool.isBotClientAvailable(key, time.Now()))

	time.Sleep(60 * time.Millisecond)
	assert.True(t, pool.isBotClientAvailable(key, time.Now()))
}
