package tgc

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"

	"github.com/ViktorsBaikers/teldrive/internal/config"
)

type noopInvoker struct{}

func (n noopInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error {
	return nil
}

func TestWithRateLimit_UsesPerMinuteConfig(t *testing.T) {
	cfg := &config.TGConfig{
		RateLimit: true,
		Rate:      60,
		RateBurst: 1,
	}

	middlewares := NewMiddleware(cfg, WithRateLimit())
	if len(middlewares) != 1 {
		t.Fatalf("expected one middleware, got %d", len(middlewares))
	}

	wrapped := middlewares[0].Handle(tg.Invoker(noopInvoker{}))
	if err := wrapped.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected first invoke to pass, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := wrapped.Invoke(ctx, nil, nil); err == nil {
		t.Fatalf("expected invoke to be rate-limited by middleware")
	}
}

func TestRatePerSecond(t *testing.T) {
	if got := ratePerSecond(120); got != rate.Limit(2) {
		t.Fatalf("expected 2 req/s, got %v", got)
	}
	if got := ratePerSecond(60); got != rate.Limit(1) {
		t.Fatalf("expected 1 req/s, got %v", got)
	}
}
