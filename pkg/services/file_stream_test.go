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
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
)

func newStreamTestService() *extendedService {
	cfg := &config.ServerCmdConfig{}
	cfg.TG.SessionInstance = "test-instance"

	c := cache.NewMemoryCache(1 << 20)
	return &extendedService{
		api: &apiService{
			cnf:   cfg,
			cache: c,
		},
	}
}

func TestStreamWithTGReader_GetPartsErrorReturnsHTTPError(t *testing.T) {
	svc := newStreamTestService()

	origFetch := fetchPartsForStream
	defer func() { fetchPartsForStream = origFetch }()

	fetchPartsForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, string, string) ([]types.Part, error) {
		return nil, stderrors.New("parts fetch failed")
	}

	recorder := httptest.NewRecorder()
	err := svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-1"},
		0, 2, 3,
		"botA",
		nil,
	)

	if err == nil {
		t.Fatal("expected error from streamWithTGReader")
	}
	if recorder.Code != 500 {
		t.Fatalf("expected HTTP 500, got %d", recorder.Code)
	}
}

func TestStreamWithTGReader_SuccessStreamsData(t *testing.T) {
	svc := newStreamTestService()

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
	err := svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-2"},
		0, 2, 3,
		"botB",
		nil,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorder.Body.String() != "abc" {
		t.Fatalf("expected body 'abc', got %q", recorder.Body.String())
	}
}

func TestStreamWithTGReader_ReaderCreateErrorReturnsHTTPError(t *testing.T) {
	svc := newStreamTestService()

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
		return nil, stderrors.New("reader create failed")
	}

	recorder := httptest.NewRecorder()
	err := svc.streamWithTGReader(
		context.Background(),
		recorder,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-3"},
		0, 2, 3,
		"botC",
		nil,
	)

	if err == nil {
		t.Fatal("expected error from streamWithTGReader")
	}
	if recorder.Code != 500 {
		t.Fatalf("expected HTTP 500, got %d", recorder.Code)
	}
}
