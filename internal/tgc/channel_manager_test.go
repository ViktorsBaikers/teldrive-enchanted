package tgc

import (
	"context"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/config"
	"gorm.io/gorm"
)

func TestChannelLimitReached_UsesFreshCountPerCall(t *testing.T) {
	originalLoader := loadChannelPartCount
	t.Cleanup(func() {
		loadChannelPartCount = originalLoader
	})

	counts := []int64{9, 11}
	callCount := 0
	loadChannelPartCount = func(_ context.Context, _ *gorm.DB, _ int64) (int64, error) {
		count := counts[callCount]
		callCount++
		return count, nil
	}

	cm := &ChannelManager{
		cnf: &config.TGConfig{ChannelLimit: 10},
	}

	if got := cm.ChannelLimitReached(context.Background(), 123); got {
		t.Fatalf("expected first call to be below limit")
	}
	if got := cm.ChannelLimitReached(context.Background(), 123); !got {
		t.Fatalf("expected second call to be at or above limit")
	}
	if callCount != 2 {
		t.Fatalf("expected loader to be called for each check, got %d calls", callCount)
	}
}
