package services

import (
	"bytes"
	"context"
	stderrors "errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
)

func newStreamTestService() *extendedService {
	cfg := &config.ServerCmdConfig{}
	cfg.TG.SessionInstance = "test-instance"

	c := cache.NewMemoryCache(1 << 20)
	pool := tgc.NewClientPool(nil, c, &cfg.TG)
	return &extendedService{
		api: &apiService{
			cnf:        cfg,
			cache:      c,
			clientPool: pool,
		},
	}
}

func TestStreamWithTGReader_GetPartsErrorRecordsBotFailure(t *testing.T) {
	svc := newStreamTestService()
	clientKey := "user:1:bot:tokenA"

	origFetch := fetchPartsForStream
	origNewReader := newReaderForStream
	defer func() {
		fetchPartsForStream = origFetch
		newReaderForStream = origNewReader
	}()

	fetchPartsForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, string, string) ([]types.Part, error) {
		return nil, stderrors.New("parts fetch failed")
	}

	recorder := httptest.NewRecorder()
	svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-1"},
		0,
		2,
		3,
		"tokenA",
		clientKey,
	)

	stats := svc.api.clientPool.Stats()
	if stats.BotFailures[clientKey] != 1 {
		t.Fatalf("expected one bot failure, got %d", stats.BotFailures[clientKey])
	}
}

func TestStreamWithTGReader_SuccessRecordsBotSuccess(t *testing.T) {
	svc := newStreamTestService()
	clientKey := "user:1:bot:tokenB"

	origFetch := fetchPartsForStream
	origNewReader := newReaderForStream
	defer func() {
		fetchPartsForStream = origFetch
		newReaderForStream = origNewReader
	}()

	fetchPartsForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, string, string) ([]types.Part, error) {
		return []types.Part{{ID: 1, Size: 3}}, nil
	}
	newReaderForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, []types.Part, int64, int64, *config.TGConfig, string, func(error)) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("abc"))), nil
	}

	recorder := httptest.NewRecorder()
	svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-2"},
		0,
		2,
		3,
		"tokenB",
		clientKey,
	)

	stats := svc.api.clientPool.Stats()
	if stats.BotSuccesses[clientKey] != 1 {
		t.Fatalf("expected one bot success, got %d", stats.BotSuccesses[clientKey])
	}
}

func TestStreamWithTGReader_CopyErrorDoesNotRecordSuccess(t *testing.T) {
	svc := newStreamTestService()
	clientKey := "user:1:bot:tokenC"

	origFetch := fetchPartsForStream
	origNewReader := newReaderForStream
	defer func() {
		fetchPartsForStream = origFetch
		newReaderForStream = origNewReader
	}()

	fetchPartsForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, string, string) ([]types.Part, error) {
		return []types.Part{{ID: 1, Size: 3}}, nil
	}
	newReaderForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, []types.Part, int64, int64, *config.TGConfig, string, func(error)) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("a"))), nil
	}

	recorder := httptest.NewRecorder()
	svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-3"},
		0,
		2,
		3,
		"tokenC",
		clientKey,
	)

	stats := svc.api.clientPool.Stats()
	if stats.BotSuccesses[clientKey] != 0 {
		t.Fatalf("expected no bot success on copy error, got %d", stats.BotSuccesses[clientKey])
	}
}
