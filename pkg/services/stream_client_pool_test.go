package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	tgpool "github.com/ViktorsBaikers/teldrive/internal/pool"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"gorm.io/gorm"
)

type fakeTelegramClientPool struct {
	userCalls int
	botCalls  int

	gotUserSession *models.Session
	gotBotUserID   int64
	gotBotToken    string
	botTokens      []string

	returnKey  string
	returnKeys map[string]string
	botErrors  map[string]error

	released []string
}

func (p *fakeTelegramClientPool) GetUserTelegramClient(ctx context.Context, session *models.Session) (*telegram.Client, string, error) {
	p.userCalls++
	p.gotUserSession = session
	return nil, p.returnKey, nil
}

func (p *fakeTelegramClientPool) GetBotTelegramClient(ctx context.Context, userID int64, token string) (*telegram.Client, string, error) {
	p.botCalls++
	p.gotBotUserID = userID
	p.gotBotToken = token
	p.botTokens = append(p.botTokens, token)
	if err, ok := p.botErrors[token]; ok {
		return nil, "", err
	}
	if key, ok := p.returnKeys[token]; ok {
		return nil, key, nil
	}
	return nil, p.returnKey, nil
}

func (p *fakeTelegramClientPool) Release(key string) {
	p.released = append(p.released, key)
}

type fakeInvokerPool struct {
	client *tg.Client
	closed bool
}

func (p *fakeInvokerPool) Client(context.Context, int) *tg.Client { return p.client }
func (p *fakeInvokerPool) Default(context.Context) *tg.Client     { return p.client }
func (p *fakeInvokerPool) Close() error {
	p.closed = true
	return nil
}

func TestStreamClientFromPool_UsesBotTokenAndReleases(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	var gotPoolSize int64
	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, size int64, _ ...telegram.Middleware) tgpool.Pool {
		gotPoolSize = size
		return invokerPool
	}

	clientPool := &fakeTelegramClientPool{returnKey: "bot-key"}
	svc := &apiService{
		cnf: &config.ServerCmdConfig{
			TG: config.TGConfig{
				PoolSize: 8,
				Stream: config.TGStream{
					Concurrency: 4,
				},
			},
		},
		clientPool: clientPool,
	}

	session := &models.Session{UserId: 123, Session: "sess"}

	client, cleanup, err := svc.streamClientFromPool(context.Background(), session, "tokenA")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client != invokerPool.client {
		t.Fatalf("expected returned tg client to match invoker pool client")
	}
	if gotPoolSize != 4 {
		t.Fatalf("expected pool size 4, got %d", gotPoolSize)
	}
	if clientPool.botCalls != 1 || clientPool.userCalls != 0 {
		t.Fatalf("expected bot calls=1 user calls=0, got bot=%d user=%d", clientPool.botCalls, clientPool.userCalls)
	}
	if clientPool.gotBotUserID != 123 || clientPool.gotBotToken != "tokenA" {
		t.Fatalf("unexpected bot args: userID=%d token=%q", clientPool.gotBotUserID, clientPool.gotBotToken)
	}

	cleanup()
	if !invokerPool.closed {
		t.Fatalf("expected invoker pool to be closed")
	}
	if len(clientPool.released) != 1 || clientPool.released[0] != "bot-key" {
		t.Fatalf("expected Release to be called with bot-key, got %v", clientPool.released)
	}
}

func TestStreamClientFromPool_UsesUserSessionAndReleases(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	clientPool := &fakeTelegramClientPool{returnKey: "user-key"}
	svc := &apiService{
		cnf:        &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool: clientPool,
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	_, cleanup, err := svc.streamClientFromPool(context.Background(), session, "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if clientPool.userCalls != 1 || clientPool.botCalls != 0 {
		t.Fatalf("expected user calls=1 bot calls=0, got user=%d bot=%d", clientPool.userCalls, clientPool.botCalls)
	}
	if clientPool.gotUserSession != session {
		t.Fatalf("expected user session arg to match")
	}

	cleanup()
	if len(clientPool.released) != 1 || clientPool.released[0] != "user-key" {
		t.Fatalf("expected Release to be called with user-key, got %v", clientPool.released)
	}
}

func TestShouldFallbackToDirectStreamClientAllowsBotCooldown(t *testing.T) {
	if !shouldFallbackToDirectStreamClient("tokenA", tgc.ErrBotClientTemporarilyUnavailable) {
		t.Fatal("expected bot cooldown error to allow direct fallback")
	}
}

func TestShouldFallbackToDirectStreamClientRejectsCapacityExceeded(t *testing.T) {
	if shouldFallbackToDirectStreamClient("tokenA", tgc.ErrBotStreamCapacityExceeded) {
		t.Fatal("expected capacity error to block direct fallback")
	}
}

func TestShouldFallbackToDirectStreamClientAllowsOtherErrors(t *testing.T) {
	if !shouldFallbackToDirectStreamClient("tokenA", errors.New("pool create failed")) {
		t.Fatal("expected non-circuit bot error to allow direct fallback")
	}
	if !shouldFallbackToDirectStreamClient("", tgc.ErrBotClientTemporarilyUnavailable) {
		t.Fatal("expected user-session path to allow direct fallback")
	}
}

func TestStreamClientFromPoolWithBotRetry_TriesAnotherBotAfterCooldown(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	clientPool := &fakeTelegramClientPool{
		returnKeys: map[string]string{"tokenB": "bot-key-b"},
		botErrors:  map[string]error{"tokenA": tgc.ErrBotClientTemporarilyUnavailable},
	}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewMemoryBotSelector(),
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	client, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client != invokerPool.client {
		t.Fatalf("expected returned tg client to match invoker pool client")
	}
	if token != "tokenB" {
		t.Fatalf("expected retry to choose tokenB, got %q", token)
	}
	if clientPool.botCalls != 2 {
		t.Fatalf("expected two bot attempts, got %d", clientPool.botCalls)
	}
	if len(clientPool.botTokens) != 2 || clientPool.botTokens[0] != "tokenA" || clientPool.botTokens[1] != "tokenB" {
		t.Fatalf("expected retry order [tokenA tokenB], got %v", clientPool.botTokens)
	}

	cleanup()
	if len(clientPool.released) != 1 || clientPool.released[0] != "bot-key-b" {
		t.Fatalf("expected Release to be called with bot-key-b, got %v", clientPool.released)
	}
}

func TestStreamClientFromPoolWithBotRetry_TriesAnotherBotAfterStartupError(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	clientPool := &fakeTelegramClientPool{
		returnKeys: map[string]string{"tokenB": "bot-key-b"},
		botErrors:  map[string]error{"tokenA": errors.New("startup failed")},
	}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewMemoryBotSelector(),
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	client, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client != invokerPool.client {
		t.Fatalf("expected returned tg client to match invoker pool client")
	}
	if token != "tokenB" {
		t.Fatalf("expected retry to choose tokenB, got %q", token)
	}
	if clientPool.botCalls != 2 {
		t.Fatalf("expected two bot attempts, got %d", clientPool.botCalls)
	}
	if len(clientPool.botTokens) != 2 || clientPool.botTokens[0] != "tokenA" || clientPool.botTokens[1] != "tokenB" {
		t.Fatalf("expected retry order [tokenA tokenB], got %v", clientPool.botTokens)
	}

	cleanup()
	if len(clientPool.released) != 1 || clientPool.released[0] != "bot-key-b" {
		t.Fatalf("expected Release to be called with bot-key-b, got %v", clientPool.released)
	}
}

func TestStreamClientFromPoolWithBotRetry_RecordsFailedAcquireInBotHealth(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	health := tgc.NewBotHealth(1, time.Hour)
	clientPool := &fakeTelegramClientPool{
		returnKeys: map[string]string{"tokenB": "bot-key-b"},
		botErrors:  map[string]error{"tokenA": errors.New("startup failed")},
	}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health),
		botHealth:   health,
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	_, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected first call to succeed, got %v", err)
	}
	if token != "tokenB" {
		t.Fatalf("expected fallback tokenB, got %q", token)
	}
	cleanup()

	_, cleanup, token, err = svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected second call to succeed, got %v", err)
	}
	if token != "tokenB" {
		t.Fatalf("expected second call to stay on tokenB, got %q", token)
	}
	cleanup()

	if len(clientPool.botTokens) != 3 || clientPool.botTokens[0] != "tokenA" || clientPool.botTokens[1] != "tokenB" || clientPool.botTokens[2] != "tokenB" {
		t.Fatalf("expected tokenA to be skipped after first failed acquire, got %v", clientPool.botTokens)
	}
}

func TestStreamClientFromPoolWithBotRetry_DoesNotCountPoolCooldownAgainstBotHealth(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	health := tgc.NewBotHealth(1, time.Hour)
	clientPool := &fakeTelegramClientPool{
		returnKeys: map[string]string{"tokenB": "bot-key-b"},
		botErrors:  map[string]error{"tokenA": tgc.ErrBotClientTemporarilyUnavailable},
	}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health),
		botHealth:   health,
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	_, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected first call to succeed, got %v", err)
	}
	if token != "tokenB" {
		t.Fatalf("expected fallback tokenB, got %q", token)
	}
	cleanup()

	_, cleanup, token, err = svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if err != nil {
		t.Fatalf("expected second call to succeed, got %v", err)
	}
	if token != "tokenB" {
		t.Fatalf("expected second call to fall back to tokenB, got %q", token)
	}
	cleanup()

	if len(clientPool.botTokens) != 4 || clientPool.botTokens[0] != "tokenA" || clientPool.botTokens[1] != "tokenB" || clientPool.botTokens[2] != "tokenA" || clientPool.botTokens[3] != "tokenB" {
		t.Fatalf("expected tokenA cooldown to stay out of shared bot health, got %v", clientPool.botTokens)
	}
}

func TestStreamClientFromPoolWithBotRetry_PrefersStartupErrorOverLaterCooldown(t *testing.T) {
	startupErr := errors.New("startup failed")
	clientPool := &fakeTelegramClientPool{
		botErrors: map[string]error{
			"tokenA": startupErr,
			"tokenB": tgc.ErrBotClientTemporarilyUnavailable,
		},
	}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewMemoryBotSelector(),
	}
	session := &models.Session{UserId: 42, Session: "sess"}

	client, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), session, []string{"tokenA", "tokenB"})
	if !errors.Is(err, startupErr) {
		t.Fatalf("expected startup error, got %v", err)
	}
	if client != nil || cleanup != nil {
		t.Fatalf("expected no client or cleanup on error")
	}
	if token != "tokenB" {
		t.Fatalf("expected last attempted token tokenB, got %q", token)
	}
	if clientPool.botCalls != 2 {
		t.Fatalf("expected two bot attempts, got %d", clientPool.botCalls)
	}
	if len(clientPool.botTokens) != 2 || clientPool.botTokens[0] != "tokenA" || clientPool.botTokens[1] != "tokenB" {
		t.Fatalf("expected retry order [tokenA tokenB], got %v", clientPool.botTokens)
	}
}

func TestReserveStreamBot_PrefersLowestLoadToken(t *testing.T) {
	health := tgc.NewBotHealth(3, time.Second)
	selector := tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health)
	svc := &apiService{
		botSelector: selector,
		botHealth:   health,
	}

	if !health.TryAcquireStream("tokenA") || !health.TryAcquireStream("tokenA") {
		t.Fatal("expected tokenA load to be reserved twice")
	}
	if !health.TryAcquireStream("tokenB") {
		t.Fatal("expected tokenB load to be reserved once")
	}

	reserved, err := svc.reserveStreamBot(context.Background(), 42, []string{"tokenA", "tokenB", "tokenC"}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if reserved.token != "tokenC" {
		t.Fatalf("expected lowest-load tokenC, got %q", reserved.token)
	}
	if got := health.ActiveStreams("tokenC"); got != 1 {
		t.Fatalf("expected tokenC active streams to be 1, got %d", got)
	}

	reserved.cleanup()
	if got := health.ActiveStreams("tokenC"); got != 0 {
		t.Fatalf("expected tokenC active streams to return to 0, got %d", got)
	}
}

func TestReserveStreamBot_ReturnsCapacityExceededWhenAllBotsAtBudget(t *testing.T) {
	health := tgc.NewBotHealth(3, time.Second)
	health.SetStreamBudget(1)
	selector := tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health)
	svc := &apiService{
		botSelector: selector,
		botHealth:   health,
	}

	if !health.TryAcquireStream("tokenA") || !health.TryAcquireStream("tokenB") {
		t.Fatal("expected initial stream reservations to succeed")
	}

	_, err := svc.reserveStreamBot(context.Background(), 42, []string{"tokenA", "tokenB"}, nil)
	if !errors.Is(err, tgc.ErrBotStreamCapacityExceeded) {
		t.Fatalf("expected capacity exceeded, got %v", err)
	}
}

func TestReserveStreamBot_SkipsTriedLowestLoadToken(t *testing.T) {
	health := tgc.NewBotHealth(3, time.Second)
	selector := tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health)
	svc := &apiService{
		botSelector: selector,
		botHealth:   health,
	}

	if !health.TryAcquireStream("tokenB") {
		t.Fatal("expected tokenB load reservation to succeed")
	}

	tried := map[string]struct{}{"tokenA": {}}
	reserved, err := svc.reserveStreamBot(context.Background(), 42, []string{"tokenA", "tokenB"}, tried)
	if err != nil {
		t.Fatalf("expected reserve to try tokenB, got %v", err)
	}
	if reserved.token != "tokenB" {
		t.Fatalf("expected tried token to be excluded and tokenB selected, got %q", reserved.token)
	}
	if got := health.ActiveStreams("tokenB"); got != 2 {
		t.Fatalf("expected tokenB active streams to be 2 after reserve, got %d", got)
	}

	reserved.cleanup()
	if got := health.ActiveStreams("tokenB"); got != 1 {
		t.Fatalf("expected tokenB active streams to return to 1 after cleanup, got %d", got)
	}
}

func TestStreamClientFromPoolWithBotRetry_ReleasesStreamReservationOnCleanup(t *testing.T) {
	origNewPool := newStreamInvokerPool
	defer func() { newStreamInvokerPool = origNewPool }()

	invokerPool := &fakeInvokerPool{client: tg.NewClient(nil)}
	newStreamInvokerPool = func(_ *telegram.Client, _ int64, _ ...telegram.Middleware) tgpool.Pool {
		return invokerPool
	}

	health := tgc.NewBotHealth(3, time.Second)
	clientPool := &fakeTelegramClientPool{returnKey: "bot-key-a"}
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{TG: config.TGConfig{PoolSize: 1}},
		clientPool:  clientPool,
		botSelector: tgc.NewHealthAwareBotSelector(tgc.NewMemoryBotSelector(), health),
		botHealth:   health,
	}

	_, cleanup, token, err := svc.streamClientFromPoolWithBotRetry(context.Background(), &models.Session{UserId: 42, Session: "sess"}, []string{"tokenA"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if token != "tokenA" {
		t.Fatalf("expected tokenA, got %q", token)
	}
	if got := health.ActiveStreams("tokenA"); got != 1 {
		t.Fatalf("expected tokenA active streams to be 1 before cleanup, got %d", got)
	}

	cleanup()
	if got := health.ActiveStreams("tokenA"); got != 0 {
		t.Fatalf("expected tokenA active streams to return to 0 after cleanup, got %d", got)
	}
}

func TestStreamDirectBotClientWithRetry_TriesAllBotsAfterFailures(t *testing.T) {
	origNewBotClient := newBotClientForStream
	defer func() { newBotClientForStream = origNewBotClient }()

	var calls []string
	newBotClientForStream = func(
		_ context.Context,
		_ *gorm.DB,
		_ cache.Cacher,
		_ *config.TGConfig,
		token string,
		_ ...telegram.Middleware,
	) (*telegram.Client, error) {
		calls = append(calls, token)
		if token == "tokenC" {
			return &telegram.Client{}, nil
		}
		return nil, errors.New("startup failed")
	}

	health := tgc.NewBotHealth(3, time.Second)
	svc := &apiService{
		cnf:         &config.ServerCmdConfig{},
		botSelector: tgc.NewMemoryBotSelector(),
		botHealth:   health,
	}

	client, cleanup, token, err := svc.streamDirectBotClientWithRetry(context.Background(), &models.Session{UserId: 42}, []string{"tokenA", "tokenB", "tokenC"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client == nil {
		t.Fatal("expected direct bot client")
	}
	if token != "tokenC" {
		t.Fatalf("expected tokenC, got %q", token)
	}
	if len(calls) < 2 || calls[0] != "tokenA" || calls[len(calls)-1] != "tokenC" {
		t.Fatalf("expected retries to start at tokenA and end at tokenC, got %v", calls)
	}
	seen := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if _, ok := seen[call]; ok {
			t.Fatalf("expected each retry candidate to be unique, got %v", calls)
		}
		seen[call] = struct{}{}
	}
	if cleanup == nil {
		t.Fatal("expected cleanup")
	}
	cleanup()
	if got := health.ActiveStreams("tokenC"); got != 0 {
		t.Fatalf("expected tokenC active streams to return to 0, got %d", got)
	}
}
