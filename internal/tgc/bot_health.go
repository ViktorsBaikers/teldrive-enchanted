package tgc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ViktorsBaikers/teldrive/internal/logging"
	"go.uber.org/zap"
)

var ErrBotStreamCapacityExceeded = errors.New("bot stream capacity is temporarily unavailable")

// botHealthState tracks per-bot circuit breaker state using atomics for lock-free access.
type botHealthState struct {
	consecutiveFailures int64
	totalFailures       int64
	totalSuccesses      int64
	circuitTrips        int64
	openUntilUnixNano   int64
	activeStreams       int64
	lastError           atomic.Value // stores string
}

// BotHealthStats exposes a snapshot of a single bot's health state.
type BotHealthStats struct {
	Token               string
	Available           bool
	ConsecutiveFailures int64
	TotalFailures       int64
	TotalSuccesses      int64
	CircuitTrips        int64
	LastError           string
	OpenUntil           time.Time // zero if circuit closed
}

// BotHealth is a standalone circuit breaker for bot tokens, decoupled from ClientPool.
// It tracks consecutive failures per bot and opens the circuit after a configurable
// threshold, preventing unhealthy bots from being selected during the cooldown period.
type BotHealth struct {
	states           sync.Map
	failureThreshold int64
	cooldown         time.Duration
	streamBudget     int64
	logger           *zap.Logger
}

// NewBotHealth creates a BotHealth with the given failure threshold and cooldown.
// These values typically come from TGConfig.BotCircuitFailures and TGConfig.BotCircuitCooldown.
func NewBotHealth(failureThreshold int, cooldown time.Duration) *BotHealth {
	threshold := int64(failureThreshold)
	if threshold <= 0 {
		threshold = defaultBotCircuitFailureThreshold
	}
	if cooldown <= 0 {
		cooldown = defaultBotCircuitCooldown
	}
	return &BotHealth{
		failureThreshold: threshold,
		cooldown:         cooldown,
		logger:           logging.Component("TG"),
	}
}

func (h *BotHealth) getState(token string) *botHealthState {
	actual, _ := h.states.LoadOrStore(token, &botHealthState{})
	return actual.(*botHealthState)
}

// RecordFailure increments consecutive failures for the given bot token.
// context.Canceled errors are ignored (normal shutdown, not a bot problem).
// When consecutive failures reach the threshold, the circuit opens for the cooldown duration.
func (h *BotHealth) RecordFailure(token string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}

	state := h.getState(token)
	state.lastError.Store(err.Error())
	atomic.AddInt64(&state.totalFailures, 1)
	failures := atomic.AddInt64(&state.consecutiveFailures, 1)

	if failures < h.failureThreshold {
		return
	}

	openUntil := time.Now().Add(h.cooldown).UnixNano()
	atomic.StoreInt64(&state.openUntilUnixNano, openUntil)
	atomic.StoreInt64(&state.consecutiveFailures, 0)
	atomic.AddInt64(&state.circuitTrips, 1)

	h.logger.Warn("bot_health.circuit_open",
		zap.String("token", redactToken(token)),
		zap.Time("open_until", time.Unix(0, openUntil)),
		zap.Error(err))
}

// RecordSuccess records a successful operation for the bot token.
// If the cooldown has expired, it resets consecutive failures and closes the circuit.
func (h *BotHealth) RecordSuccess(token string) {
	state := h.getState(token)
	atomic.AddInt64(&state.totalSuccesses, 1)

	for {
		openUntil := atomic.LoadInt64(&state.openUntilUnixNano)
		if openUntil == 0 {
			atomic.StoreInt64(&state.consecutiveFailures, 0)
			return
		}

		now := time.Now().UnixNano()
		if now < openUntil {
			return
		}

		if atomic.CompareAndSwapInt64(&state.openUntilUnixNano, openUntil, 0) {
			atomic.StoreInt64(&state.consecutiveFailures, 0)
			return
		}
	}
}

// IsAvailable returns true if the bot's circuit is closed (healthy) or the cooldown has elapsed.
func (h *BotHealth) IsAvailable(token string) bool {
	state := h.getState(token)
	openUntil := atomic.LoadInt64(&state.openUntilUnixNano)
	if openUntil == 0 {
		return true
	}

	if time.Now().UnixNano() >= openUntil {
		if atomic.CompareAndSwapInt64(&state.openUntilUnixNano, openUntil, 0) {
			atomic.StoreInt64(&state.consecutiveFailures, 0)
		}
		return true
	}

	return false
}

// FilterHealthy returns the subset of tokens whose circuits are closed.
func (h *BotHealth) FilterHealthy(tokens []string) []string {
	healthy := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if h.IsAvailable(t) {
			healthy = append(healthy, t)
		}
	}
	return healthy
}

// Stats returns a snapshot of health state for each given token.
// Tokens are redacted in the output for safe external exposure.
func (h *BotHealth) Stats(tokens []string) []BotHealthStats {
	now := time.Now()
	stats := make([]BotHealthStats, 0, len(tokens))
	for _, token := range tokens {
		state := h.getState(token)
		openNano := atomic.LoadInt64(&state.openUntilUnixNano)
		var openUntil time.Time
		available := true
		if openNano > 0 && now.UnixNano() < openNano {
			openUntil = time.Unix(0, openNano)
			available = false
		}

		var lastErr string
		if v := state.lastError.Load(); v != nil {
			lastErr = v.(string)
		}

		stats = append(stats, BotHealthStats{
			Token:               redactToken(token),
			Available:           available,
			ConsecutiveFailures: atomic.LoadInt64(&state.consecutiveFailures),
			TotalFailures:       atomic.LoadInt64(&state.totalFailures),
			TotalSuccesses:      atomic.LoadInt64(&state.totalSuccesses),
			CircuitTrips:        atomic.LoadInt64(&state.circuitTrips),
			LastError:           lastErr,
			OpenUntil:           openUntil,
		})
	}
	return stats
}

// FailureThreshold returns the configured failure threshold.
func (h *BotHealth) FailureThreshold() int64 {
	return h.failureThreshold
}

// Cooldown returns the configured cooldown duration.
func (h *BotHealth) Cooldown() time.Duration {
	return h.cooldown
}

// SetStreamBudget configures the maximum number of concurrent streams per bot.
// A value of 0 disables budget enforcement.
func (h *BotHealth) SetStreamBudget(limit int) {
	if limit <= 0 {
		atomic.StoreInt64(&h.streamBudget, 0)
		return
	}
	atomic.StoreInt64(&h.streamBudget, int64(limit))
}

// StreamBudget returns the configured per-bot concurrent stream budget.
// A value of 0 means disabled.
func (h *BotHealth) StreamBudget() int64 {
	return atomic.LoadInt64(&h.streamBudget)
}

// ActiveStreams returns the number of currently reserved stream slots for a bot.
func (h *BotHealth) ActiveStreams(token string) int64 {
	return atomic.LoadInt64(&h.getState(token).activeStreams)
}

// TryAcquireStream reserves a stream slot for the given bot token.
// When the stream budget is disabled, every acquire succeeds.
func (h *BotHealth) TryAcquireStream(token string) bool {
	state := h.getState(token)
	budget := h.StreamBudget()
	if budget <= 0 {
		atomic.AddInt64(&state.activeStreams, 1)
		return true
	}

	for {
		current := atomic.LoadInt64(&state.activeStreams)
		if current >= budget {
			return false
		}
		if atomic.CompareAndSwapInt64(&state.activeStreams, current, current+1) {
			return true
		}
	}
}

// ReleaseStream releases one reserved stream slot for the given bot token.
func (h *BotHealth) ReleaseStream(token string) {
	state := h.getState(token)
	for {
		current := atomic.LoadInt64(&state.activeStreams)
		if current <= 0 {
			return
		}
		if atomic.CompareAndSwapInt64(&state.activeStreams, current, current-1) {
			return
		}
	}
}

func (h *BotHealth) lowestLoadTokens(tokens []string) []string {
	if len(tokens) == 0 {
		return nil
	}

	budget := h.StreamBudget()
	var (
		eligible     []string
		fallback     []string
		eligibleLoad int64
		fallbackLoad int64
		haveEligible bool
		haveFallback bool
	)

	for _, token := range tokens {
		load := h.ActiveStreams(token)
		if !haveFallback || load < fallbackLoad {
			fallback = []string{token}
			fallbackLoad = load
			haveFallback = true
		} else if load == fallbackLoad {
			fallback = append(fallback, token)
		}

		if budget > 0 && load >= budget {
			continue
		}
		if !haveEligible || load < eligibleLoad {
			eligible = []string{token}
			eligibleLoad = load
			haveEligible = true
		} else if load == eligibleLoad {
			eligible = append(eligible, token)
		}
	}

	if len(eligible) > 0 {
		return eligible
	}
	if budget > 0 {
		return nil
	}
	return fallback
}

func redactToken(token string) string {
	if len(token) <= 10 {
		return token
	}
	return token[:10] + "..."
}

// HealthAwareBotSelector decorates any BotSelector with health-aware filtering.
// It filters out unhealthy bots before delegating to the inner selector.
// If all bots are unhealthy, it falls back to the full list (circuit half-open probe).
type HealthAwareBotSelector struct {
	inner  BotSelector
	health *BotHealth
}

// NewHealthAwareBotSelector wraps a BotSelector with health-aware filtering.
func NewHealthAwareBotSelector(inner BotSelector, health *BotHealth) *HealthAwareBotSelector {
	return &HealthAwareBotSelector{inner: inner, health: health}
}

// Next selects the next bot, preferring healthy bots. Falls back to all bots
// if none are healthy (allows probing a tripped bot rather than failing entirely).
func (s *HealthAwareBotSelector) Next(ctx context.Context, op BotOp, userID int64, bots []string) (string, int, error) {
	healthy := s.health.FilterHealthy(bots)
	if len(healthy) > 0 {
		candidates := healthy
		if op == BotOpStream {
			candidates = s.health.lowestLoadTokens(healthy)
			if len(candidates) == 0 {
				if len(healthy) == len(bots) {
					return "", 0, ErrBotStreamCapacityExceeded
				}
				return s.inner.Next(ctx, op, userID, bots)
			}
		}
		return s.inner.Next(ctx, op, userID, candidates)
	}
	// All bots unhealthy — fall back to full list so at least one gets probed
	return s.inner.Next(ctx, op, userID, bots)
}

// Health returns the underlying BotHealth so callers can record failures/successes.
func (s *HealthAwareBotSelector) Health() *BotHealth {
	return s.health
}
