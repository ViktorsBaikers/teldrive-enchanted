package services

import (
	"context"
	"errors"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type recordingBotSelector struct {
	op    tgc.BotOp
	token string
}

func (s *recordingBotSelector) Next(ctx context.Context, op tgc.BotOp, userID int64, bots []string) (string, int, error) {
	s.op = op
	for i, bot := range bots {
		if bot == s.token {
			return bot, i, nil
		}
	}
	if len(bots) == 0 {
		return "", 0, errors.New("no bots")
	}
	return bots[0], 0, nil
}

func TestRemainingUploadResolutionBotsExcludesTriedBots(t *testing.T) {
	tokens := []string{"tokenA", "tokenB", "tokenC"}
	tried := map[string]struct{}{
		"tokenB": {},
	}

	remaining := remainingUploadResolutionBots(tokens, tried)
	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining bots, got %d", len(remaining))
	}
	if remaining[0] != "tokenA" || remaining[1] != "tokenC" {
		t.Fatalf("expected remaining bots [tokenA tokenC], got %v", remaining)
	}
}

func TestWithUploadResolutionClientFallsBackToUserSessionAfterBotFailures(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:upload-resolution-fallback-user-session?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
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
	if err := db.Create(&models.Bot{Token: "tokenA", UserId: 1}).Error; err != nil {
		t.Fatalf("create bot tokenA: %v", err)
	}
	if err := db.Create(&models.Bot{Token: "tokenB", UserId: 1}).Error; err != nil {
		t.Fatalf("create bot tokenB: %v", err)
	}

	originalBotClient := newBotClientForStream
	originalAuthClient := newAuthClientForUploadResolution
	originalRunWithAuth := runWithAuthForUploadResolution
	originalGetJWTUser := getJWTUserForUploadResolution
	defer func() {
		newBotClientForStream = originalBotClient
		newAuthClientForUploadResolution = originalAuthClient
		runWithAuthForUploadResolution = originalRunWithAuth
		getJWTUserForUploadResolution = originalGetJWTUser
	}()

	newBotClientForStream = func(context.Context, *gorm.DB, cache.Cacher, *config.TGConfig, string, ...telegram.Middleware) (*telegram.Client, error) {
		return nil, errors.New("bot failed")
	}
	newAuthClientForUploadResolution = func(context.Context, *config.TGConfig, string, ...telegram.Middleware) (*telegram.Client, error) {
		return &telegram.Client{}, nil
	}
	getJWTUserForUploadResolution = func(context.Context) *types.JWTClaims {
		return &types.JWTClaims{TgSession: "user-session"}
	}

	var usedToken string
	runWithAuthForUploadResolution = func(ctx context.Context, client *telegram.Client, token string, fn func(context.Context) error) error {
		usedToken = token
		return fn(ctx)
	}

	svc := &apiService{
		db:             db,
		cnf:            &config.ServerCmdConfig{},
		cache:          cache.NewMemoryCache(1 << 20),
		botSelector:    tgc.NewMemoryBotSelector(),
		channelManager: tgc.NewChannelManager(db, cache.NewMemoryCache(1<<20), &config.TGConfig{}),
	}

	err = svc.withUploadResolutionClient(context.Background(), 1, func(context.Context, *telegram.Client, string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("withUploadResolutionClient returned error: %v", err)
	}
	if usedToken != "" {
		t.Fatalf("expected user-session fallback token to be empty, got %q", usedToken)
	}
}

func TestWithUploadResolutionClientUsesUploadBotOp(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:upload-resolution-bot-op?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
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
	if err := db.Create(&models.Bot{Token: "tokenA", UserId: 1}).Error; err != nil {
		t.Fatalf("create bot tokenA: %v", err)
	}

	originalBotClient := newBotClientForStream
	originalRunWithAuth := runWithAuthForUploadResolution
	defer func() {
		newBotClientForStream = originalBotClient
		runWithAuthForUploadResolution = originalRunWithAuth
	}()

	newBotClientForStream = func(context.Context, *gorm.DB, cache.Cacher, *config.TGConfig, string, ...telegram.Middleware) (*telegram.Client, error) {
		return &telegram.Client{}, nil
	}
	runWithAuthForUploadResolution = func(ctx context.Context, client *telegram.Client, token string, fn func(context.Context) error) error {
		return fn(ctx)
	}

	selector := &recordingBotSelector{token: "tokenA"}
	svc := &apiService{
		db:             db,
		cnf:            &config.ServerCmdConfig{},
		cache:          cache.NewMemoryCache(1 << 20),
		botSelector:    selector,
		channelManager: tgc.NewChannelManager(db, cache.NewMemoryCache(1<<20), &config.TGConfig{}),
	}

	err = svc.withUploadResolutionClient(context.Background(), 1, func(context.Context, *telegram.Client, string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("withUploadResolutionClient returned error: %v", err)
	}
	if selector.op != tgc.BotOpUpload {
		t.Fatalf("expected BotOpUpload, got %q", selector.op)
	}
}

func TestResolveAmbiguousUploadBackedFileSizeMatchesMessagesByPartID(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:upload-resolution-message-order?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
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

	originalGetMessages := getMessagesForUploadResolution
	originalAuthClient := newAuthClientForUploadResolution
	originalRunWithAuth := runWithAuthForUploadResolution
	originalGetJWTUser := getJWTUserForUploadResolution
	defer func() {
		getMessagesForUploadResolution = originalGetMessages
		newAuthClientForUploadResolution = originalAuthClient
		runWithAuthForUploadResolution = originalRunWithAuth
		getJWTUserForUploadResolution = originalGetJWTUser
	}()

	getMessagesForUploadResolution = func(context.Context, *tg.Client, []int, int64) ([]tg.MessageClass, error) {
		return []tg.MessageClass{
			&tg.Message{ID: 22, Media: &tg.MessageMediaDocument{Document: &tg.Document{Size: 100}}},
			&tg.Message{ID: 11, Media: &tg.MessageMediaDocument{Document: &tg.Document{Size: 80}}},
		}, nil
	}
	newAuthClientForUploadResolution = func(context.Context, *config.TGConfig, string, ...telegram.Middleware) (*telegram.Client, error) {
		return &telegram.Client{}, nil
	}
	getJWTUserForUploadResolution = func(context.Context) *types.JWTClaims {
		return &types.JWTClaims{TgSession: "user-session"}
	}
	runWithAuthForUploadResolution = func(ctx context.Context, client *telegram.Client, token string, fn func(context.Context) error) error {
		return fn(ctx)
	}

	svc := &apiService{
		db:             db,
		cnf:            &config.ServerCmdConfig{},
		cache:          cache.NewMemoryCache(1 << 20),
		botSelector:    tgc.NewMemoryBotSelector(),
		channelManager: tgc.NewChannelManager(db, cache.NewMemoryCache(1<<20), &config.TGConfig{}),
	}

	size, err := svc.resolveAmbiguousUploadBackedFileSize(context.Background(), 1, []models.Upload{
		{ChannelId: 7, PartId: 11, Size: 50},
		{ChannelId: 7, PartId: 22, Size: 100},
	})
	if err != nil {
		t.Fatalf("resolveAmbiguousUploadBackedFileSize returned error: %v", err)
	}
	if size != 100 {
		t.Fatalf("expected logical size 100, got %d", size)
	}
}
