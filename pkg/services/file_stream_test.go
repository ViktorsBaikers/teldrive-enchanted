package services

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

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
		http.StatusOK,
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
		http.StatusOK,
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
		http.StatusOK,
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

func newFilesStreamStatusTestService(t *testing.T) *extendedService {
	t.Helper()

	dbName := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE files (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			mime_type TEXT NOT NULL,
			size INTEGER,
			category TEXT,
			encrypted NUMERIC,
			user_id INTEGER NOT NULL,
			status TEXT,
			parent_id TEXT,
			parts JSON,
			channel_id INTEGER,
			hash TEXT,
			created_at DATETIME,
			updated_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create files table: %v", err)
	}
	if err := db.Exec(`
		CREATE TABLE bots (
			token TEXT PRIMARY KEY,
			user_id INTEGER,
			bot_id INTEGER
		)
	`).Error; err != nil {
		t.Fatalf("create bots table: %v", err)
	}

	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-1",
		Name:      "test.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := db.Create(&models.Bot{Token: "tokenA", UserId: 1, BotId: 1}).Error; err != nil {
		t.Fatalf("create bot: %v", err)
	}

	cfg := &config.ServerCmdConfig{}
	c := cache.NewMemoryCache(1 << 20)
	return &extendedService{
		api: &apiService{
			db:             db,
			cnf:            cfg,
			cache:          c,
			botSelector:    tgc.NewMemoryBotSelector(),
			channelManager: tgc.NewChannelManager(db, c, &cfg.TG),
		},
	}
}

func TestFilesStream_PoolCooldownFallsBackToDirectClient(t *testing.T) {
	svc := newFilesStreamStatusTestService(t)
	svc.api.clientPool = &fakeTelegramClientPool{
		botErrors: map[string]error{"tokenA": tgc.ErrBotClientTemporarilyUnavailable},
	}
	origNewBotClient := newBotClientForStream
	defer func() { newBotClientForStream = origNewBotClient }()
	newBotClientForStream = func(context.Context, *gorm.DB, cache.Cacher, *config.TGConfig, string, ...telegram.Middleware) (*telegram.Client, error) {
		return nil, stderrors.New("direct fallback failed")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/files/file-1", nil)
	recorder := httptest.NewRecorder()

	svc.FilesStream(recorder, req, "file-1", 1)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected HTTP 500 after direct fallback attempt, got %d with body %q", recorder.Code, recorder.Body.String())
	}
}

func TestFilesStream_StreamCapacityReturns503BeforeSuccessStatus(t *testing.T) {
	svc := newFilesStreamStatusTestService(t)
	svc.api.botHealth = tgc.NewBotHealth(1, time.Hour)
	svc.api.botHealth.SetStreamBudget(1)
	if !svc.api.botHealth.TryAcquireStream("tokenA") {
		t.Fatal("expected to reserve tokenA stream slot")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/files/file-1", nil)
	recorder := httptest.NewRecorder()

	svc.FilesStream(recorder, req, "file-1", 1)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503, got %d with body %q", recorder.Code, recorder.Body.String())
	}
}
