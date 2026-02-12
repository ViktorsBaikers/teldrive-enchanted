package tgc

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/ViktorsBaikers/teldrive/internal/config"
)

type noopInvoker struct{}

func (n noopInvoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error {
	return nil
}

func TestWithRateLimit_UsesMillisecondConfig(t *testing.T) {
	// Rate=100 means "one request every 100ms" = 10 req/s
	// With burst=1, the second immediate request should be rate-limited
	cfg := &config.TGConfig{
		RateLimit: true,
		Rate:      100,
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

	// With Rate=100 (100ms between requests), a 50ms timeout should not allow
	// a second request to pass
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := wrapped.Invoke(ctx, nil, nil); err == nil {
		t.Fatalf("expected invoke to be rate-limited by middleware")
	}
}
