package services

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/api"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/crypt"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/tgdrive/teldrive/internal/utils"
	"github.com/tgdrive/teldrive/pkg/models"
	"github.com/tgdrive/teldrive/pkg/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func getParts(ctx context.Context, client *tg.Client, c cache.Cacher, file *models.File, sessionInstance string, botID string) ([]types.Part, error) {
	return cache.Fetch(ctx, c, cache.KeyFileMessages(file.ID), 60*time.Minute, func(fetchCtx context.Context) ([]types.Part, error) {
		messages, err := tgc.GetMessages(fetchCtx, client, utils.Map(*file.Parts, func(part api.Part) int {
			return part.ID
		}), *file.ChannelId)

		if err != nil {
			return nil, err
		}
		parts := buildPartsAndPrimeLocations(fetchCtx, c, file, messages, sessionInstance, botID)
		if len(parts) != len(*file.Parts) {
			logger := logging.Component("FILE")
			logger.Error("parts.mismatch",
				zap.String("file_id", file.ID),
				zap.String("file_name", file.Name),
				zap.Int("expected", len(*file.Parts)),
				zap.Int("actual", len(parts)))
			return nil, errors.New("file parts mismatch")
		}
		return parts, nil
	})
}

func buildPartsAndPrimeLocations(
	ctx context.Context,
	c cache.Cacher,
	file *models.File,
	messages []tg.MessageClass,
	sessionInstance string,
	botID string,
) []types.Part {
	parts := make([]types.Part, 0, len(messages))
	for i, message := range messages {
		item, ok := message.(*tg.Message)
		if !ok {
			continue
		}
		media, ok := item.Media.(*tg.MessageMediaDocument)
		if !ok {
			continue
		}
		document, ok := media.Document.(*tg.Document)
		if !ok {
			continue
		}
		part := types.Part{
			ID:   int64((*file.Parts)[i].ID),
			Size: document.Size,
			Salt: (*file.Parts)[i].Salt.Value,
		}
		if *file.Encrypted {
			part.DecryptedSize, _ = crypt.DecryptedSize(document.Size)
		}
		parts = append(parts, part)

		location := document.AsInputDocumentFileLocation()
		if location != nil {
			key := cache.KeyFileLocation(sessionInstance, botID, file.ID, part.ID)
			_ = c.Set(ctx, key, location, 30*time.Minute)
		}
	}
	return parts
}

func resolvePathID(db *gorm.DB, path string, userId int64) (*string, error) {
	if !strings.HasPrefix(path, "/root") {
		path = "/root/" + strings.Trim(path, "/")
	}
	var id string
	query := `
	WITH RECURSIVE path_parts AS (
		SELECT ordinality as depth, part as name
		FROM unnest(string_to_array(trim(both '/' from ?), '/')) WITH ORDINALITY as part
	),
	max_depth AS (
		SELECT max(depth) as val FROM path_parts
	),
	hierarchy AS (
		SELECT f.id, 1 as depth
		FROM teldrive.files f
		JOIN path_parts p ON p.depth = 1 AND f.name = p.name
		WHERE f.user_id = ? AND f.parent_id IS NULL AND f.status = 'active'
		UNION ALL
		SELECT child.id, h.depth + 1
		FROM teldrive.files child
		JOIN hierarchy h ON child.parent_id = h.id
		JOIN path_parts p ON p.depth = h.depth + 1 AND child.name = p.name
		JOIN max_depth md ON h.depth < md.val
		WHERE child.status = 'active'
	)
	SELECT id FROM hierarchy WHERE depth = (SELECT val FROM max_depth) LIMIT 1;
	`
	if err := db.Raw(query, path, userId).Scan(&id).Error; err != nil {
		return nil, err
	}
	if id == "" {
		return nil, gorm.ErrRecordNotFound
	}
	return &id, nil
}
