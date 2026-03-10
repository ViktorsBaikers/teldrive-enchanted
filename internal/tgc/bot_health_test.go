package tgc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestBotHealth_CircuitOpensAfterThreshold(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	token := "123:ABC"

	h.RecordFailure(token, errors.New("flood"))
	h.RecordFailure(token, errors.New("flood"))
	if !h.IsAvailable(token) {
		t.Fatal("circuit should still be closed after 2 failures")
	}

	h.RecordFailure(token, errors.New("flood"))
	if h.IsAvailable(token) {
		t.Fatal("circuit should be open after 3 failures")
	}
}

func TestBotHealth_ContextCanceledIgnored(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	token := "123:ABC"

	h.RecordFailure(token, context.Canceled)
	if !h.IsAvailable(token) {
		t.Fatal("context.Canceled should not count as a failure")
	}
}

func TestBotHealth_WrappedContextCanceledIgnored(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	token := "123:ABC"

	wrapped := fmt.Errorf("operation aborted: %w", context.Canceled)
	h.RecordFailure(token, wrapped)
	if !h.IsAvailable(token) {
		t.Fatal("wrapped context.Canceled should not count as a failure")
	}

	stats := h.Stats([]string{token})
	if stats[0].TotalFailures != 0 {
		t.Fatalf("expected 0 total failures for wrapped canceled, got %d", stats[0].TotalFailures)
	}
}

func TestBotHealth_SuccessResetsCounter(t *testing.T) {
	h := NewBotHealth(3, time.Millisecond)
	token := "123:ABC"

	h.RecordFailure(token, errors.New("err"))
	h.RecordFailure(token, errors.New("err"))
	// 2 failures, below threshold

	// Wait for any potential cooldown to pass
	time.Sleep(5 * time.Millisecond)

	h.RecordSuccess(token)
	// Success should reset consecutive failures

	h.RecordFailure(token, errors.New("err"))
	h.RecordFailure(token, errors.New("err"))
	// 2 more failures — should still be below threshold since counter was reset
	if !h.IsAvailable(token) {
		t.Fatal("circuit should be closed — success should have reset the counter")
	}
}

func TestBotHealth_CooldownExpiryReopensCircuit(t *testing.T) {
	h := NewBotHealth(1, 10*time.Millisecond)
	token := "123:ABC"

	h.RecordFailure(token, errors.New("err"))
	if h.IsAvailable(token) {
		t.Fatal("circuit should be open immediately after trip")
	}

	time.Sleep(20 * time.Millisecond)

	if !h.IsAvailable(token) {
		t.Fatal("circuit should re-close after cooldown expires")
	}
}

func TestBotHealth_FilterHealthy(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	tokens := []string{"bot1", "bot2", "bot3"}

	// Trip bot2
	h.RecordFailure("bot2", errors.New("err"))

	healthy := h.FilterHealthy(tokens)
	if len(healthy) != 2 {
		t.Fatalf("expected 2 healthy bots, got %d", len(healthy))
	}
	for _, tok := range healthy {
		if tok == "bot2" {
			t.Fatal("bot2 should be filtered out")
		}
	}
}

func TestHealthAwareBotSelector_SkipsUnhealthyBots(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	tokens := []string{"bot1", "bot2", "bot3"}

	// Trip bot1
	h.RecordFailure("bot1", errors.New("err"))

	token, _, err := sel.Next(context.Background(), BotOpStream, 1, tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token == "bot1" {
		t.Fatal("should not select unhealthy bot1")
	}
}

func TestHealthAwareBotSelector_FallbackWhenAllUnhealthy(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	tokens := []string{"bot1", "bot2"}

	h.RecordFailure("bot1", errors.New("err"))
	h.RecordFailure("bot2", errors.New("err"))

	// When all bots are unhealthy, should fall back to full list
	token, _, err := sel.Next(context.Background(), BotOpStream, 1, tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "bot1" && token != "bot2" {
		t.Fatalf("expected one of the bots, got %q", token)
	}
}

func TestHealthAwareBotSelector_HealthAccessor(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	if sel.Health() != h {
		t.Fatal("Health() should return the underlying BotHealth")
	}
}

func TestHealthAwareBotSelector_StreamPrefersLowestLoadHealthyBot(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	if !h.TryAcquireStream("bot1") || !h.TryAcquireStream("bot1") {
		t.Fatal("expected to acquire two stream slots for bot1")
	}
	if !h.TryAcquireStream("bot2") {
		t.Fatal("expected to acquire one stream slot for bot2")
	}

	token, _, err := sel.Next(context.Background(), BotOpStream, 1, []string{"bot1", "bot2", "bot3"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "bot3" {
		t.Fatalf("expected lowest-load bot3, got %q", token)
	}
}

func TestHealthAwareBotSelector_StreamSkipsBotsAtBudget(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	h.SetStreamBudget(1)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	if !h.TryAcquireStream("bot1") {
		t.Fatal("expected to acquire initial stream slot for bot1")
	}

	token, _, err := sel.Next(context.Background(), BotOpStream, 1, []string{"bot1", "bot2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "bot2" {
		t.Fatalf("expected selector to skip budgeted bot1, got %q", token)
	}
}

func TestHealthAwareBotSelector_StreamReturnsCapacityErrorWhenAllHealthyBotsAreAtBudget(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	h.SetStreamBudget(1)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	if !h.TryAcquireStream("bot1") || !h.TryAcquireStream("bot2") {
		t.Fatal("expected initial stream acquisitions to succeed")
	}

	_, _, err := sel.Next(context.Background(), BotOpStream, 1, []string{"bot1", "bot2"})
	if !errors.Is(err, ErrBotStreamCapacityExceeded) {
		t.Fatalf("expected ErrBotStreamCapacityExceeded, got %v", err)
	}
}

func TestHealthAwareBotSelector_StreamFallsBackToFullSetWhenOnlyUnhealthyBotsRemain(t *testing.T) {
	h := NewBotHealth(1, time.Hour)
	h.SetStreamBudget(1)
	inner := NewMemoryBotSelector()
	sel := NewHealthAwareBotSelector(inner, h)

	if !h.TryAcquireStream("bot1") {
		t.Fatal("expected initial stream acquisition to succeed")
	}
	h.RecordFailure("bot2", errors.New("trip unhealthy bot2"))

	token, _, err := sel.Next(context.Background(), BotOpStream, 1, []string{"bot1", "bot2"})
	if err != nil {
		t.Fatalf("expected fallback probe instead of capacity error, got %v", err)
	}
	if token != "bot1" && token != "bot2" {
		t.Fatalf("expected a fallback token from full set, got %q", token)
	}
}

func TestMemoryBotSelector_AdaptsToSmallerCandidateSet(t *testing.T) {
	sel := NewMemoryBotSelector()
	userID := int64(1)

	token, _, err := sel.Next(context.Background(), BotOpStream, userID, []string{"bot1", "bot2"})
	if err != nil {
		t.Fatalf("unexpected error on first selection: %v", err)
	}
	if token != "bot1" {
		t.Fatalf("expected first selection bot1, got %q", token)
	}

	token, _, err = sel.Next(context.Background(), BotOpStream, userID, []string{"bot2"})
	if err != nil {
		t.Fatalf("unexpected error on smaller candidate set: %v", err)
	}
	if token != "bot2" {
		t.Fatalf("expected smaller candidate set to return bot2, got %q", token)
	}
}

func TestBotHealth_StreamBudgetZeroDisablesLimit(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	h.SetStreamBudget(0)

	for i := 0; i < 5; i++ {
		if !h.TryAcquireStream("bot1") {
			t.Fatalf("expected acquire %d to succeed with disabled budget", i+1)
		}
	}

	if got := h.ActiveStreams("bot1"); got != 5 {
		t.Fatalf("expected 5 active streams, got %d", got)
	}
}

func TestBotHealth_ReleaseStreamDoesNotGoNegative(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)

	h.ReleaseStream("bot1")
	if got := h.ActiveStreams("bot1"); got != 0 {
		t.Fatalf("expected 0 active streams, got %d", got)
	}

	if !h.TryAcquireStream("bot1") {
		t.Fatal("expected acquire to succeed")
	}
	h.ReleaseStream("bot1")
	h.ReleaseStream("bot1")
	if got := h.ActiveStreams("bot1"); got != 0 {
		t.Fatalf("expected 0 active streams after double release, got %d", got)
	}
}

func TestBotHealth_Stats_ReturnsCorrectData(t *testing.T) {
	h := NewBotHealth(3, 5*time.Second)
	tokens := []string{"1234567890:ABCDEFGHIJ_KLMNOPQRSTUVWXYZ12345", "0987654321:ZYXWVUTSRQ_PONMLKJIHGFEDCBA54321"}

	// Record some activity on first bot
	h.RecordSuccess(tokens[0])
	h.RecordSuccess(tokens[0])
	h.RecordFailure(tokens[0], errors.New("timeout"))

	stats := h.Stats(tokens)
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(stats))
	}

	// First bot should have activity
	s0 := stats[0]
	if s0.Token != "1234567890..." {
		t.Fatalf("expected redacted token '1234567890...', got %q", s0.Token)
	}
	if !s0.Available {
		t.Fatal("first bot should be available (below threshold)")
	}
	if s0.TotalSuccesses != 2 {
		t.Fatalf("expected 2 successes, got %d", s0.TotalSuccesses)
	}
	if s0.TotalFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", s0.TotalFailures)
	}
	if s0.LastError != "timeout" {
		t.Fatalf("expected lastError 'timeout', got %q", s0.LastError)
	}

	// Second bot should be pristine
	s1 := stats[1]
	if s1.TotalSuccesses != 0 || s1.TotalFailures != 0 {
		t.Fatalf("expected zero activity on second bot, got %d successes, %d failures", s1.TotalSuccesses, s1.TotalFailures)
	}
	if s1.LastError != "" {
		t.Fatalf("expected empty lastError for pristine bot, got %q", s1.LastError)
	}
}

func TestBotHealth_Stats_CircuitOpenState(t *testing.T) {
	h := NewBotHealth(1, 5*time.Second)
	token := "1234567890:ABCDEFGHIJ_KLMNOPQRSTUVWXYZ12345"

	h.RecordFailure(token, errors.New("flood_wait"))

	stats := h.Stats([]string{token})
	if len(stats) != 1 {
		t.Fatalf("expected 1 stat, got %d", len(stats))
	}

	s := stats[0]
	if s.Available {
		t.Fatal("bot should be unavailable after circuit trip")
	}
	if s.CircuitTrips != 1 {
		t.Fatalf("expected 1 circuit trip, got %d", s.CircuitTrips)
	}
	if s.OpenUntil.IsZero() {
		t.Fatal("OpenUntil should be set when circuit is open")
	}
	if s.LastError != "flood_wait" {
		t.Fatalf("expected lastError 'flood_wait', got %q", s.LastError)
	}
}

func TestBotHealth_LastError_UpdatedOnEachFailure(t *testing.T) {
	h := NewBotHealth(5, 5*time.Second)
	token := "bot1"

	h.RecordFailure(token, errors.New("first error"))
	h.RecordFailure(token, errors.New("second error"))

	stats := h.Stats([]string{token})
	if stats[0].LastError != "second error" {
		t.Fatalf("expected lastError to be most recent, got %q", stats[0].LastError)
	}
}

func TestBotHealth_DefaultValues(t *testing.T) {
	h := NewBotHealth(0, 0)
	if h.failureThreshold != defaultBotCircuitFailureThreshold {
		t.Fatalf("expected default threshold %d, got %d", defaultBotCircuitFailureThreshold, h.failureThreshold)
	}
	if h.cooldown != defaultBotCircuitCooldown {
		t.Fatalf("expected default cooldown %v, got %v", defaultBotCircuitCooldown, h.cooldown)
	}
}
