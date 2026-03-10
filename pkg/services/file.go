package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/internal/auth"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/category"
	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"github.com/ViktorsBaikers/teldrive/internal/database"
	"github.com/ViktorsBaikers/teldrive/internal/events"
	"github.com/ViktorsBaikers/teldrive/internal/hash"
	"github.com/ViktorsBaikers/teldrive/internal/http_range"
	"github.com/ViktorsBaikers/teldrive/internal/logging"
	"github.com/ViktorsBaikers/teldrive/internal/md5"
	tgpool "github.com/ViktorsBaikers/teldrive/internal/pool"
	"github.com/ViktorsBaikers/teldrive/internal/reader"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"github.com/ViktorsBaikers/teldrive/internal/utils"
	"github.com/ViktorsBaikers/teldrive/pkg/mapper"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
	"github.com/google/uuid"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/sync/errgroup"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrorStreamAbandoned                 = errors.New("stream abandoned")
	defaultContentType                   = "application/octet-stream"
	fileMetadataCacheTTL                 = 30 * time.Second
	fileMetadataStaleTTL                 = 5 * time.Minute
	fetchPartsForStream                  = getParts
	newReaderForStream                   = reader.NewReader
	newStreamInvokerPool                 = tgpool.NewPool
	newBotClientForStream                = tgc.BotClient
	newAuthClientForUploadResolution     = tgc.AuthClient
	runWithAuthForUploadResolution       = tgc.RunWithAuth
	getJWTUserForUploadResolution        = auth.GetJWTUser
	getMessagesForUploadResolution       = tgc.GetMessages
	resolveAmbiguousUploadBackedFileSize = func(a *apiService, ctx context.Context, userID int64, uploads []models.Upload) (int64, error) {
		return a.resolveAmbiguousUploadBackedFileSize(ctx, userID, uploads)
	}
)

func isUUID(str string) bool {
	_, err := uuid.Parse(str)
	return err == nil
}

type reservedStreamBot struct {
	token   string
	release func()
}

func (r *reservedStreamBot) cleanup() {
	if r == nil || r.release == nil {
		return
	}
	release := r.release
	r.release = nil
	release()
}

func remainingUploadResolutionBots(tokens []string, tried map[string]struct{}) []string {
	available := make([]string, 0, len(tokens)-len(tried))
	for _, token := range tokens {
		if _, ok := tried[token]; ok {
			continue
		}
		available = append(available, token)
	}
	return available
}

func (a *apiService) resolveAmbiguousUploadBackedFileSize(ctx context.Context, userID int64, uploads []models.Upload) (int64, error) {
	if len(uploads) == 0 {
		return 0, nil
	}

	partIDs := utils.Map(uploads, func(upload models.Upload) int { return upload.PartId })
	channelID := uploads[0].ChannelId
	var messages []tg.MessageClass

	if err := a.withUploadResolutionClient(ctx, userID, func(runCtx context.Context, client *telegram.Client, token string) error {
		var err error
		messages, err = getMessagesForUploadResolution(runCtx, client.API(), partIDs, channelID)
		return err
	}); err != nil {
		return 0, err
	}

	if len(messages) != len(uploads) {
		return 0, fmt.Errorf("%w: expected=%d actual=%d", ErrInvalidUploadPart, len(uploads), len(messages))
	}

	messageByID := make(map[int]*tg.Message, len(messages))
	for _, message := range messages {
		item, ok := message.(*tg.Message)
		if !ok {
			return 0, fmt.Errorf("%w: unexpected message type", ErrInvalidUploadPart)
		}
		messageByID[item.ID] = item
	}

	var logicalSize int64
	for _, upload := range uploads {
		item, ok := messageByID[upload.PartId]
		if !ok {
			return 0, fmt.Errorf("%w: missing message for part id %d", ErrInvalidUploadPart, upload.PartId)
		}
		document, ok := msgDocument(item)
		if !ok {
			return 0, fmt.Errorf("%w: missing document for part id %d", ErrInvalidUploadPart, upload.PartId)
		}

		if document.Size == upload.Size {
			decryptedSize, err := crypt.DecryptedSize(document.Size)
			if err != nil {
				return 0, err
			}
			logicalSize += decryptedSize
			continue
		}

		logicalSize += upload.Size
	}

	return logicalSize, nil
}

func (a *apiService) withUploadResolutionClient(
	ctx context.Context,
	userID int64,
	fn func(context.Context, *telegram.Client, string) error,
) error {
	tokens, err := a.channelManager.BotTokens(ctx, userID)
	if err != nil {
		return err
	}

	var botErr error
	if len(tokens) > 0 {
		tried := make(map[string]struct{}, len(tokens))
		for len(tried) < len(tokens) {
			available := remainingUploadResolutionBots(tokens, tried)
			if len(available) == 0 {
				break
			}

			token, _, err := a.botSelector.Next(ctx, tgc.BotOpUpload, userID, available)
			if err != nil {
				if botErr != nil {
					break
				}
				botErr = err
				break
			}
			tried[token] = struct{}{}

			client, err := newBotClientForStream(ctx, a.db, a.cache, &a.cnf.TG, token, a.newMiddlewares(ctx, 5)...)
			if err != nil {
				botErr = err
				continue
			}
			err = runWithAuthForUploadResolution(ctx, client, token, func(runCtx context.Context) error {
				return fn(runCtx, client, token)
			})
			if err == nil {
				return nil
			}
			botErr = err
		}
	}

	jwtUser := getJWTUserForUploadResolution(ctx)
	if jwtUser == nil || jwtUser.TgSession == "" {
		if botErr != nil {
			return botErr
		}
		return errors.New("telegram session required to resolve upload size")
	}
	client, err := newAuthClientForUploadResolution(ctx, &a.cnf.TG, jwtUser.TgSession, a.newMiddlewares(ctx, 5)...)
	if err != nil {
		return err
	}
	return runWithAuthForUploadResolution(ctx, client, "", func(runCtx context.Context) error {
		return fn(runCtx, client, "")
	})
}

func (a *apiService) FilesCategoryStats(ctx context.Context) ([]api.CategoryStats, error) {
	userId := auth.GetUser(ctx)
	var stats []api.CategoryStats
	if err := a.db.Model(&models.File{}).Select("category", "COUNT(*) as total_files", "coalesce(SUM(size),0) as total_size").
		Where(&models.File{UserId: userId, Type: "file", Status: "active"}).
		Order("category ASC").Group("category").Find(&stats).Error; err != nil {
		return nil, &apiError{err: err}
	}

	return stats, nil
}

func (a *apiService) FilesCopy(ctx context.Context, req *api.FileCopy, params api.FilesCopyParams) (*api.File, error) {
	userId := auth.GetUser(ctx)

	client, _ := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.newMiddlewares(ctx, 5)...)

	var res []models.File

	if err := a.db.Model(&models.File{}).Where("id = ?", params.ID).Find(&res).Error; err != nil {
		return nil, &apiError{err: err}
	}
	if len(res) == 0 {
		return nil, &apiError{err: errors.New("file not found"), code: 404}
	}

	file := res[0]

	newIds := []api.Part{}

	channelId, err := a.channelManager.CurrentChannel(ctx, userId)
	if err != nil {
		return nil, &apiError{err: err}
	}

	err = tgc.RunWithAuth(ctx, client, "", func(ctx context.Context) error {

		ids := utils.Map(*file.Parts, func(part api.Part) int { return part.ID })
		messages, err := tgc.GetMessages(ctx, client.API(), ids, *file.ChannelId)

		if err != nil {
			return err
		}

		channel, err := tgc.GetChannelById(ctx, client.API(), channelId)

		if err != nil {
			return err
		}
		results := make([]api.Part, len(messages))
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(4)
		for i, message := range messages {
			i, message := i, message
			g.Go(func() error {
				item, ok := message.(*tg.Message)
				if !ok {
					return fmt.Errorf("unexpected message type at index %d", i)
				}
				media, ok := item.Media.(*tg.MessageMediaDocument)
				if !ok || media == nil {
					return fmt.Errorf("unexpected media type at index %d", i)
				}
				document, ok := media.Document.(*tg.Document)
				if !ok || document == nil {
					return fmt.Errorf("unexpected document type at index %d", i)
				}

				id, _ := client.RandInt64()
				request := tg.MessagesSendMediaRequest{
					Silent:   true,
					Peer:     &tg.InputPeerChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
					Media:    &tg.InputMediaDocument{ID: document.AsInput()},
					RandomID: id,
				}
				res, err := client.API().MessagesSendMedia(gCtx, &request)
				if err != nil {
					return err
				}

				updates, ok := res.(*tg.Updates)
				if !ok {
					return fmt.Errorf("unexpected response type from SendMedia at index %d", i)
				}

				var msg *tg.Message
				for _, update := range updates.Updates {
					channelMsg, ok := update.(*tg.UpdateNewChannelMessage)
					if ok {
						if m, ok := channelMsg.Message.(*tg.Message); ok {
							msg = m
						}
						break
					}
				}
				if msg == nil {
					return fmt.Errorf("no channel message in send response at index %d", i)
				}
				p := api.Part{ID: msg.ID}
				if (*file.Parts)[i].Salt.Value != "" {
					p.Salt = (*file.Parts)[i].Salt
				}
				results[i] = p
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		newIds = append(newIds, results...)
		return nil
	})

	if err != nil {
		return nil, &apiError{err: err}
	}

	if len(newIds) != len(*file.Parts) {
		return nil, &apiError{err: errors.New("failed to copy all file parts")}
	}

	var parentId string
	if !isUUID(req.Destination) {
		var destRes []models.File
		if err := a.db.Raw("select * from teldrive.create_directories(?, ?)", userId, req.Destination).
			Scan(&destRes).Error; err != nil {
			return nil, &apiError{err: err}
		}
		parentId = destRes[0].ID
	} else {
		parentId = req.Destination
	}

	dbFile := models.File{}

	dbFile.Name = req.NewName.Or(file.Name)
	dbFile.Size = file.Size
	dbFile.Type = file.Type
	dbFile.MimeType = file.MimeType
	if len(newIds) > 0 {
		dbFile.Parts = utils.Ptr(datatypes.NewJSONSlice(newIds))
	}
	dbFile.UserId = userId
	dbFile.Status = "active"
	dbFile.ParentId = utils.Ptr(parentId)
	dbFile.ChannelId = &channelId
	dbFile.Encrypted = file.Encrypted
	dbFile.Category = file.Category
	dbFile.Hash = file.Hash // Preserve hash during copy (content is identical)
	if req.UpdatedAt.IsSet() && !req.UpdatedAt.Value.IsZero() {
		dbFile.UpdatedAt = utils.Ptr(req.UpdatedAt.Value)
	} else {
		dbFile.UpdatedAt = utils.Ptr(time.Now().UTC())
	}

	if err := a.db.Create(&dbFile).Error; err != nil {
		return nil, &apiError{err: err}
	}

	a.events.Record(events.OpCopy, userId, &models.Source{
		ID:       dbFile.ID,
		Type:     dbFile.Type,
		Name:     dbFile.Name,
		ParentID: parentId,
	})
	return mapper.ToFileOut(dbFile), nil
}

func (a *apiService) FilesCreate(ctx context.Context, fileIn *api.File) (*api.File, error) {
	userId := auth.GetUser(ctx)

	var (
		fileDB             models.File
		parentID           *string
		err                error
		path               string
		channelId          int64
		uploadId           string
		uploads            []models.Upload
		effectiveEncrypted = fileIn.Encrypted.Value
	)

	if fileIn.Path.Value == "" && fileIn.ParentId.Value == "" {
		return nil, &apiError{err: errors.New("parent id or path is required"), code: 409}
	}

	if fileIn.Path.Value != "" {
		path = strings.ReplaceAll(fileIn.Path.Value, "//", "/")

	}

	if path != "" && fileIn.ParentId.Value == "" {
		parentID, err = resolvePathID(a.db, path, userId)
		if err != nil {
			return nil, &apiError{err: err, code: 404}
		}
		fileDB.ParentId = parentID

	} else if fileIn.ParentId.Value != "" {
		fileDB.ParentId = utils.Ptr(fileIn.ParentId.Value)
	}

	switch fileIn.Type {
	case api.FileTypeFolder:
		fileDB.MimeType = "drive/folder"
		fileDB.Parts = nil
	case api.FileTypeFile:
		if fileIn.ChannelId.Value == 0 {
			channelId, err = a.channelManager.CurrentChannel(ctx, userId)
			if err != nil {
				return nil, &apiError{err: err}
			}
		} else {
			channelId = fileIn.ChannelId.Value
		}
		fileDB.ChannelId = &channelId
		fileDB.MimeType = fileIn.MimeType.Value
		fileDB.Category = utils.Ptr(string(category.GetCategory(fileIn.Name)))

		// Handle parts - either from direct input or fetch by uploadId
		var parts []api.Part
		if len(fileIn.Parts) > 0 {
			parts = fileIn.Parts
		} else if fileIn.UploadId.Value != "" {
			uploadId = fileIn.UploadId.Value
			// Fetch only active upload parts within retention window.
			if err := a.db.Where("upload_id = ?", uploadId).
				Where("created_at > ?", time.Now().UTC().Add(-a.cnf.TG.Uploads.Retention)).
				Order("part_no").
				Find(&uploads).Error; err != nil {
				return nil, &apiError{err: err}
			}

			effectiveEncrypted = inferUploadBackedEncryption(uploads, fileIn.Encrypted.Value, fileIn.Encrypted.IsSet())
			logicalSize, err := validateUploadBackedFile(uploads, fileIn.Size.Value, effectiveEncrypted)
			if errors.Is(err, ErrAmbiguousUploadPartSize) {
				logicalSize, err = resolveAmbiguousUploadBackedFileSize(a, ctx, userId, uploads)
				if err != nil {
					return nil, &apiError{err: err}
				}
				if fileIn.Size.Value != 0 && logicalSize != fileIn.Size.Value {
					err = fmt.Errorf("%w: declared=%d uploaded=%d", ErrUploadedPartsSizeMismatch, fileIn.Size.Value, logicalSize)
				}
			}
			if err != nil {
				return nil, &apiError{err: err, code: 400}
			}
			if fileIn.Size.Value == 0 {
				fileIn.Size.SetTo(logicalSize)
			}

			// Convert uploads to parts
			for _, upload := range uploads {
				parts = append(parts, api.Part{
					ID:   upload.PartId,
					Salt: api.NewOptString(upload.Salt),
				})
			}
		}

		if len(parts) > 0 {
			fileDB.Parts = utils.Ptr(datatypes.NewJSONSlice(mapParts(parts)))
		}

		// Compute BLAKE3 tree hash from block hashes if uploadId is provided
		if uploadId != "" && len(uploads) > 0 {
			totalLen := 0
			for _, upload := range uploads {
				totalLen += len(upload.BlockHashes)
			}
			allBlockHashes := make([]byte, 0, totalLen)
			for _, upload := range uploads {
				allBlockHashes = append(allBlockHashes, upload.BlockHashes...)
			}

			if len(allBlockHashes) > 0 {
				treeHashBytes := hash.ComputeTreeHash(allBlockHashes)
				treeHash := hash.SumToHex(treeHashBytes)
				fileDB.Hash = &treeHash
			}
		} else if fileIn.Size.Value == 0 {
			// For zero-length files, compute hash of empty data
			treeHashBytes := hash.ComputeTreeHash([]byte{})
			treeHash := hash.SumToHex(treeHashBytes)
			fileDB.Hash = &treeHash
		}

		fileDB.Size = utils.Ptr(fileIn.Size.Value)
	}
	fileDB.Name = fileIn.Name
	fileDB.Type = string(fileIn.Type)
	fileDB.UserId = userId
	fileDB.Status = "active"
	fileDB.Encrypted = utils.Ptr(effectiveEncrypted)
	if fileIn.UpdatedAt.IsSet() && !fileIn.UpdatedAt.Value.IsZero() {
		fileDB.UpdatedAt = utils.Ptr(fileIn.UpdatedAt.Value)
	} else {
		fileDB.UpdatedAt = utils.Ptr(time.Now().UTC())
	}

	// Use transaction to ensure file creation and upload cleanup are atomic
	err = a.db.Transaction(func(tx *gorm.DB) error {
		//For some reason, gorm conflict clauses are not working with partial index so using raw query
		if err := tx.Raw(`
			INSERT INTO teldrive.files (
				name, parent_id, user_id, mime_type, category, parts,
				size, type, encrypted, updated_at, channel_id, status, hash
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (name, COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid), user_id)
			WHERE status = 'active'
			DO UPDATE SET
				mime_type = EXCLUDED.mime_type,
				category = EXCLUDED.category,
				parts = EXCLUDED.parts,
				size = EXCLUDED.size,
				type = EXCLUDED.type,
				encrypted = EXCLUDED.encrypted,
				updated_at = EXCLUDED.updated_at,
				channel_id = EXCLUDED.channel_id,
				status = EXCLUDED.status,
				hash = EXCLUDED.hash
			RETURNING *
		`,
			fileDB.Name, fileDB.ParentId, fileDB.UserId, fileDB.MimeType,
			fileDB.Category, fileDB.Parts, fileDB.Size, fileDB.Type,
			fileDB.Encrypted, fileDB.UpdatedAt, fileDB.ChannelId, fileDB.Status,
			fileDB.Hash,
		).Scan(&fileDB).Error; err != nil {
			return err
		}

		// Delete uploads after successful file creation
		if uploadId != "" {
			if err := tx.Where("upload_id = ?", uploadId).Delete(&models.Upload{}).Error; err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, &apiError{err: err}
	}

	if fileDB.ParentId != nil {
		parentID = fileDB.ParentId
	}

	a.events.Record(events.OpCreate, userId, &models.Source{
		ID:       fileDB.ID,
		Type:     fileDB.Type,
		Name:     fileDB.Name,
		ParentID: *parentID,
	})
	return mapper.ToFileOut(fileDB), nil
}

func (a *apiService) FilesCreateShare(ctx context.Context, req *api.FileShareCreate, params api.FilesCreateShareParams) error {
	userId := auth.GetUser(ctx)

	var fileShare models.FileShare

	if req.Password.Value != "" {
		bytes, err := bcrypt.GenerateFromPassword([]byte(req.Password.Value), bcrypt.MinCost)
		if err != nil {
			return &apiError{err: err}
		}
		fileShare.Password = utils.Ptr(string(bytes))
	}

	fileShare.FileId = params.ID
	if req.ExpiresAt.IsSet() {
		fileShare.ExpiresAt = utils.Ptr(req.ExpiresAt.Value)
	}
	fileShare.UserId = userId

	if err := a.db.Create(&fileShare).Error; err != nil {
		return &apiError{err: err}
	}

	return nil
}

func (a *apiService) deleteFilesBulk(db *gorm.DB, fileIds []string, userId int64) error {
	query := `
	WITH RECURSIVE target_folders AS (
		SELECT id FROM teldrive.files WHERE id IN (?) AND user_id = ?
		UNION ALL
		SELECT f.id FROM teldrive.files f JOIN target_folders tf ON f.parent_id = tf.id
	),
	mark_deleted AS (
		UPDATE teldrive.files SET status = 'pending_deletion'
		WHERE (parent_id IN (SELECT id FROM target_folders) OR id IN (?))
		AND type = 'file'
	)
	DELETE FROM teldrive.files WHERE id IN (SELECT id FROM target_folders) AND type = 'folder';
	`
	return db.Exec(query, fileIds, userId, fileIds).Error
}

func (a *apiService) getFullPath(db *gorm.DB, fileID string) (string, error) {
	var path string
	query := `
	WITH RECURSIVE path_tree AS (
		SELECT id, parent_id, name, 0 as lvl FROM teldrive.files WHERE id = ?
		UNION ALL
		SELECT f.id, f.parent_id, f.name, pt.lvl + 1
		FROM teldrive.files f JOIN path_tree pt ON f.id = pt.parent_id
	)
	SELECT string_agg(name, '/' ORDER BY lvl DESC) FROM path_tree;
	`
	err := db.Raw(query, fileID).Scan(&path).Error
	if path != "" {
		path = "/" + path
	}
	return strings.TrimPrefix(path, "/root"), err
}

func invalidateAllFilePathCache(ctx context.Context, cacher cache.Cacher) {
	_ = cacher.DeletePattern(ctx, cache.Key("files", "path", "*"))
	_ = cacher.DeletePattern(ctx, cache.Key("files", "path", "*", "stale"))
}

func shouldInvalidateDescendantPathCache(file models.File, req *api.FileUpdate) bool {
	if file.Type != "folder" {
		return false
	}
	return req.Name.IsSet() || req.ParentId.IsSet()
}

func (a *apiService) FilesDelete(ctx context.Context, req *api.FileDelete) error {
	userId := auth.GetUser(ctx)

	if len(req.Ids) == 0 {
		return &apiError{err: errors.New("ids should not be empty"), code: 409}
	}

	var fileDB models.File

	if err := a.db.Model(&models.File{}).Select("id", "name", "type", "parent_id").
		Where("id = ?", req.Ids[0]).Where("user_id = ?", userId).
		First(&fileDB).Error; err != nil {
		return &apiError{err: err}
	}

	if err := a.deleteFilesBulk(a.db, req.Ids, userId); err != nil {
		return &apiError{err: err}
	}

	keys := []string{}
	for _, id := range req.Ids {
		keys = append(keys, cache.KeyFile(id), cache.KeyFileMessages(id), cache.KeyFilePath(id))
	}
	if len(keys) > 0 {
		a.cache.Delete(ctx, keys...)
	}

	var parentID string
	if fileDB.ParentId != nil {
		parentID = *fileDB.ParentId
	}

	a.events.Record(events.OpDelete, userId, &models.Source{
		ID:       fileDB.ID,
		Type:     fileDB.Type,
		Name:     fileDB.Name,
		ParentID: parentID,
	})

	return nil
}

func (a *apiService) FilesDeleteShare(ctx context.Context, params api.FilesDeleteShareParams) error {
	userId := auth.GetUser(ctx)

	var deletedShare models.FileShare

	if err := a.db.Clauses(clause.Returning{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Delete(&deletedShare).Error; err != nil {
		return &apiError{err: err}
	}
	if deletedShare.ID != "" {
		a.cache.Delete(ctx, cache.KeyShare(deletedShare.ID))
	}

	return nil
}

func (a *apiService) FilesEditShare(ctx context.Context, req *api.FileShareCreate, params api.FilesEditShareParams) error {
	userId := auth.GetUser(ctx)

	var fileShareUpdate models.FileShare

	if req.Password.Value != "" {
		bytes, err := bcrypt.GenerateFromPassword([]byte(req.Password.Value), bcrypt.MinCost)
		if err != nil {
			return &apiError{err: err}
		}
		fileShareUpdate.Password = utils.Ptr(string(bytes))
	}
	if req.ExpiresAt.IsSet() {
		fileShareUpdate.ExpiresAt = utils.Ptr(req.ExpiresAt.Value)
	}

	if err := a.db.Model(&models.FileShare{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Updates(fileShareUpdate).Error; err != nil {
		return &apiError{err: err}
	}

	return nil
}

func (a *apiService) FilesGetById(ctx context.Context, params api.FilesGetByIdParams) (*api.File, error) {
	file, err := cache.FetchWithStale(ctx, a.cache, cache.KeyFile(params.ID), fileMetadataCacheTTL, fileMetadataStaleTTL, func(fetchCtx context.Context) (*models.File, error) {
		var result models.File
		if dbErr := a.db.WithContext(fetchCtx).Model(&models.File{}).Where("id = ?", params.ID).First(&result).Error; dbErr != nil {
			if errors.Is(dbErr, gorm.ErrRecordNotFound) {
				return nil, &apiError{err: errors.New("file not found"), code: 404}
			}
			return nil, &apiError{err: dbErr}
		}
		return &result, nil
	})
	if err != nil {
		return nil, err
	}

	path, err := cache.FetchWithStale(ctx, a.cache, cache.KeyFilePath(params.ID), fileMetadataCacheTTL, fileMetadataStaleTTL, func(fetchCtx context.Context) (string, error) {
		return a.getFullPath(a.db, params.ID)
	})
	if err != nil {
		return nil, &apiError{err: err}
	}

	res := mapper.ToFileOut(*file)
	res.Path = api.NewOptString(path)
	if file.ChannelId != nil {
		res.ChannelId = api.NewOptInt64(*file.ChannelId)
	}

	return res, nil
}

func (a *apiService) FilesList(ctx context.Context, params api.FilesListParams) (*api.FileList, error) {
	userId := auth.GetUser(ctx)

	queryBuilder := &fileQueryBuilder{db: a.db}

	return queryBuilder.execute(&params, userId)
}

func (a *apiService) FilesMkdir(ctx context.Context, req *api.FileMkDir) error {
	userId := auth.GetUser(ctx)

	if err := a.db.Exec("select * from teldrive.create_directories(?, ?)", userId, req.Path).Error; err != nil {
		return &apiError{err: err}
	}
	return nil
}

func (a *apiService) FilesMove(ctx context.Context, req *api.FileMove) error {
	userId := auth.GetUser(ctx)

	var destParentID *string

	if !isUUID(req.DestinationParent) {
		r, err := resolvePathID(a.db, req.DestinationParent, userId)
		if err != nil {
			return &apiError{err: err}
		}
		destParentID = r

	} else {
		destParentID = &req.DestinationParent
	}

	err := a.db.Transaction(func(tx *gorm.DB) error {
		var srcFile models.File
		if err := tx.Where("id = ? AND user_id = ?", req.Ids[0], userId).First(&srcFile).Error; err != nil {
			return err
		}
		if len(req.Ids) == 1 && req.DestinationName.Value != "" {
			var existing models.File
			query := tx.Where("name = ? AND user_id = ? AND status = 'active'",
				req.DestinationName.Value, userId)
			if destParentID == nil {
				query = query.Where("parent_id IS NULL")
			} else {
				query = query.Where("parent_id = ?", *destParentID)
			}

			if err := query.First(&existing).Error; err == nil {
				if srcFile.Type == "folder" && existing.Type == "folder" {
					if err := tx.Model(&models.File{}).
						Where("parent_id = ? AND status = 'active'", existing.ID).
						Where("name NOT IN (?)",
							tx.Model(&models.File{}).
								Select("name").
								Where("parent_id = ? AND status = 'active'", srcFile.ID),
						).
						Update("parent_id", srcFile.ID).Error; err != nil {
						return err
					}
				}
				if err := a.deleteFilesBulk(tx, []string{existing.ID}, userId); err != nil {
					return err
				}
			}
			return tx.Model(&models.File{}).
				Where("id = ? AND user_id = ?", req.Ids[0], userId).
				Updates(map[string]any{
					"parent_id": destParentID,
					"name":      req.DestinationName.Value,
				}).Error
		}
		items := pgtype.Array[string]{
			Elements: req.Ids,
			Valid:    true,
			Dims:     []pgtype.ArrayDimension{{Length: int32(len(req.Ids)), LowerBound: 1}},
		}
		if err := a.db.Model(&models.File{}).Where("id = any(?)", items).Where("user_id = ?", userId).
			Update("parent_id", destParentID).Error; err != nil {
			return err
		}

		var parentID string
		if srcFile.ParentId != nil {
			parentID = *srcFile.ParentId
		}

		var destParentIDStr string
		if destParentID != nil {
			destParentIDStr = *destParentID
		}

		a.events.Record(events.OpMove, userId, &models.Source{
			ID:           destParentIDStr,
			Type:         srcFile.Type,
			Name:         srcFile.Name,
			ParentID:     parentID,
			DestParentID: destParentIDStr,
		})
		return nil

	})
	if err != nil {
		return &apiError{err: err}
	}

	invalidateAllFilePathCache(ctx, a.cache)
	return nil

}

func (a *apiService) FilesShareByid(ctx context.Context, params api.FilesShareByidParams) (*api.FileShare, error) {
	userId := auth.GetUser(ctx)
	var result []models.FileShare

	notFoundErr := &apiError{err: errors.New("invalid share"), code: 404}
	if err := a.db.Model(&models.FileShare{}).Where("file_id = ?", params.ID).Where("user_id = ?", userId).
		Find(&result).Error; err != nil {
		if database.IsRecordNotFoundErr(err) {
			return nil, notFoundErr
		}
		return nil, &apiError{err: err}
	}

	if len(result) == 0 {
		return nil, notFoundErr
	}
	res := &api.FileShare{
		ID: result[0].ID,
	}
	if result[0].Password != nil {
		res.Protected = true
	}
	if result[0].ExpiresAt != nil {
		res.ExpiresAt = api.NewOptDateTime(*result[0].ExpiresAt)
	}
	return res, nil
}

func (a *apiService) FilesUpdate(ctx context.Context, req *api.FileUpdate, params api.FilesUpdateParams) (*api.File, error) {

	userId := auth.GetUser(ctx)

	updateDb := models.File{}
	isContentUpdate := false
	uploadId := ""
	effectiveEncrypted := req.Encrypted.Value
	var uploads []models.Upload

	if req.UploadId.IsSet() && req.UploadId.Value != "" {
		uploadId = req.UploadId.Value
		if err := a.db.Where("upload_id = ?", uploadId).
			Where("created_at > ?", time.Now().UTC().Add(-a.cnf.TG.Uploads.Retention)).
			Order("part_no").
			Find(&uploads).Error; err != nil {
			return nil, &apiError{err: err}
		}
		effectiveEncrypted = inferUploadBackedEncryption(uploads, req.Encrypted.Value, req.Encrypted.IsSet())
		logicalSize, err := validateUploadBackedFile(uploads, req.Size.Value, effectiveEncrypted)
		if errors.Is(err, ErrAmbiguousUploadPartSize) {
			logicalSize, err = resolveAmbiguousUploadBackedFileSize(a, ctx, userId, uploads)
			if err != nil {
				return nil, &apiError{err: err}
			}
			if req.Size.Value != 0 && logicalSize != req.Size.Value {
				err = fmt.Errorf("%w: declared=%d uploaded=%d", ErrUploadedPartsSizeMismatch, req.Size.Value, logicalSize)
			}
		}
		if err != nil {
			return nil, &apiError{err: err, code: 400}
		}
		for _, u := range uploads {
			req.Parts = append(req.Parts, api.Part{
				ID:   u.PartId,
				Salt: api.NewOptString(u.Salt),
			})
		}
		if req.Size.Value == 0 {
			req.Size.SetTo(logicalSize)
		}
	}

	if req.Name.IsSet() && req.Name.Value != "" {
		updateDb.Name = req.Name.Value
	}

	if req.ParentId.IsSet() && req.ParentId.Value != "" {
		updateDb.ParentId = utils.Ptr(req.ParentId.Value)
	}

	if req.ChannelId.IsSet() && req.ChannelId.Value != 0 {
		updateDb.ChannelId = utils.Ptr(req.ChannelId.Value)
	}

	if req.Size.IsSet() && len(req.Parts) > 0 {
		updateDb.Parts = utils.Ptr(datatypes.NewJSONSlice(mapParts(req.Parts)))
		updateDb.Size = utils.Ptr(req.Size.Value)
		isContentUpdate = true
	} else if req.Size.IsSet() && req.Size.Value == 0 {
		updateDb.Size = utils.Ptr(req.Size.Value)
		isContentUpdate = true
	}

	if req.Encrypted.IsSet() {
		updateDb.Encrypted = utils.Ptr(req.Encrypted.Value)
		isContentUpdate = true
	} else if uploadId != "" && len(uploads) > 0 {
		updateDb.Encrypted = utils.Ptr(effectiveEncrypted)
		isContentUpdate = true
	}

	// Update UpdatedAt if content changed OR if explicitly set (e.g., SetModTime)
	if isContentUpdate || req.UpdatedAt.IsSet() {
		if req.UpdatedAt.IsSet() && !req.UpdatedAt.Value.IsZero() {
			updateDb.UpdatedAt = utils.Ptr(req.UpdatedAt.Value)
		} else {
			updateDb.UpdatedAt = utils.Ptr(time.Now().UTC())
		}
	}

	// Use transaction for atomic update
	var file models.File
	err := a.db.Transaction(func(tx *gorm.DB) error {
		// Compute BLAKE3 tree hash if uploadId provided
		if uploadId != "" && len(uploads) > 0 {
			totalLen := 0
			for _, upload := range uploads {
				totalLen += len(upload.BlockHashes)
			}
			allBlockHashes := make([]byte, 0, totalLen)
			for _, upload := range uploads {
				allBlockHashes = append(allBlockHashes, upload.BlockHashes...)
			}

			if len(allBlockHashes) > 0 {
				treeHashBytes := hash.ComputeTreeHash(allBlockHashes)
				treeHash := hash.SumToHex(treeHashBytes)
				updateDb.Hash = &treeHash
			} else if req.Size.IsSet() && req.Size.Value == 0 {
				treeHashBytes := hash.ComputeTreeHash([]byte{})
				treeHash := hash.SumToHex(treeHashBytes)
				updateDb.Hash = &treeHash
			}
		}

		// Build update query - explicitly select UpdatedAt if it's the only change
		query := tx.Model(&models.File{}).Where("id = ?", params.ID)
		if req.UpdatedAt.IsSet() && !isContentUpdate {
			// Force update of updated_at field even when only metadata changes
			query = query.Select("updated_at")
		}
		if err := query.Updates(updateDb).Error; err != nil {
			return err
		}

		// Delete uploads after successful update
		if uploadId != "" {
			if err := tx.Where("upload_id = ?", uploadId).Delete(&models.Upload{}).Error; err != nil {
				return err
			}
		}

		return tx.Where("id = ?", params.ID).First(&file).Error
	})

	if err != nil {
		return nil, &apiError{err: err}
	}

	keys := []string{cache.KeyFile(params.ID), cache.KeyFilePath(params.ID)}
	if len(req.Parts) > 0 {
		keys = append(keys, cache.KeyFileMessages(params.ID))
		a.cache.DeletePattern(ctx, cache.KeyFileLocationPattern(params.ID))
	}
	a.cache.Delete(ctx, keys...)
	if shouldInvalidateDescendantPathCache(file, req) {
		invalidateAllFilePathCache(ctx, a.cache)
	}

	var parentID string
	if file.ParentId != nil {
		parentID = *file.ParentId
	}

	a.events.Record(events.OpUpdate, userId, &models.Source{
		ID:       file.ID,
		Type:     file.Type,
		Name:     file.Name,
		ParentID: parentID,
	})
	return mapper.ToFileOut(file), nil
}

func (a *apiService) streamClientFromPool(ctx context.Context, session *models.Session, token string) (*tg.Client, func(), error) {
	if a.clientPool == nil {
		return nil, nil, errors.New("client pool is not configured")
	}

	var (
		client *telegram.Client
		key    string
		err    error
	)
	if token == "" {
		client, key, err = a.clientPool.GetUserTelegramClient(ctx, session)
	} else {
		client, key, err = a.clientPool.GetBotTelegramClient(ctx, session.UserId, token)
	}
	if err != nil {
		return nil, nil, err
	}

	// Cap invoker pool size by stream concurrency so we don't create more
	// connections than this stream can use.
	poolSize := int64(a.cnf.TG.PoolSize)
	if poolSize < 1 {
		poolSize = 1
	}
	if c := int64(a.cnf.TG.Stream.Concurrency); c > 0 && c < poolSize {
		poolSize = c
	}

	invokerPool := newStreamInvokerPool(client, poolSize, a.newMiddlewares(ctx, 5)...)
	tgClient := invokerPool.Default(ctx)

	cleanup := func() {
		_ = invokerPool.Close()
		a.clientPool.Release(key)
	}
	return tgClient, cleanup, nil
}

func (a *apiService) acquireStreamBotToken(token string) func() {
	if token == "" || a.botHealth == nil {
		return nil
	}
	if !a.botHealth.TryAcquireStream(token) {
		return nil
	}
	return func() {
		a.botHealth.ReleaseStream(token)
	}
}

func (a *apiService) reserveStreamBot(
	ctx context.Context,
	userID int64,
	tokens []string,
	tried map[string]struct{},
) (*reservedStreamBot, error) {
	if len(tokens) == 0 {
		return &reservedStreamBot{}, nil
	}
	if tried == nil {
		tried = make(map[string]struct{}, len(tokens))
	}

	capacityBlocked := false
	for len(tried) < len(tokens) {
		available := make([]string, 0, len(tokens)-len(tried))
		for _, token := range tokens {
			if _, seen := tried[token]; !seen {
				available = append(available, token)
			}
		}
		if len(available) == 0 {
			break
		}

		token, _, err := a.botSelector.Next(ctx, tgc.BotOpStream, userID, available)
		if err != nil {
			return nil, err
		}

		tried[token] = struct{}{}
		release := a.acquireStreamBotToken(token)
		if token != "" && a.botHealth != nil && release == nil {
			capacityBlocked = true
			continue
		}
		return &reservedStreamBot{token: token, release: release}, nil
	}

	if capacityBlocked {
		return nil, tgc.ErrBotStreamCapacityExceeded
	}
	return nil, fmt.Errorf("no stream bot available")
}

func (a *apiService) streamClientFromPoolWithBotRetry(
	ctx context.Context,
	session *models.Session,
	tokens []string,
) (*tg.Client, func(), string, error) {
	if len(tokens) == 0 {
		client, cleanup, err := a.streamClientFromPool(ctx, session, "")
		return client, cleanup, "", err
	}

	tried := make(map[string]struct{}, len(tokens))
	selectedToken := ""
	var lastErr error
	var preferredFallbackErr error
	capacityBlocked := false

	for len(tried) < len(tokens) {
		reserved, err := a.reserveStreamBot(ctx, session.UserId, tokens, tried)
		if err != nil {
			if errors.Is(err, tgc.ErrBotStreamCapacityExceeded) {
				capacityBlocked = true
				break
			}
			return nil, nil, "", err
		}

		selectedToken = reserved.token
		client, cleanup, err := a.streamClientFromPool(ctx, session, reserved.token)
		if err == nil {
			return client, func() {
				cleanup()
				reserved.cleanup()
			}, reserved.token, nil
		}
		if reserved.token != "" && a.botHealth != nil && !errors.Is(err, tgc.ErrBotClientTemporarilyUnavailable) {
			a.botHealth.RecordFailure(reserved.token, err)
		}
		reserved.cleanup()
		lastErr = err
		if !errors.Is(err, tgc.ErrBotClientTemporarilyUnavailable) {
			preferredFallbackErr = err
		}
	}

	if preferredFallbackErr != nil {
		return nil, nil, selectedToken, preferredFallbackErr
	}
	if lastErr != nil {
		return nil, nil, selectedToken, lastErr
	}
	if capacityBlocked {
		return nil, nil, selectedToken, tgc.ErrBotStreamCapacityExceeded
	}
	return nil, nil, selectedToken, tgc.ErrBotClientTemporarilyUnavailable
}

func (a *apiService) streamDirectBotClientWithRetry(
	ctx context.Context,
	session *models.Session,
	tokens []string,
) (*telegram.Client, func(), string, error) {
	tried := make(map[string]struct{}, len(tokens))
	selectedToken := ""
	var lastErr error
	capacityBlocked := false

	for len(tried) < len(tokens) {
		reserved, err := a.reserveStreamBot(ctx, session.UserId, tokens, tried)
		if err != nil {
			if errors.Is(err, tgc.ErrBotStreamCapacityExceeded) {
				capacityBlocked = true
				break
			}
			return nil, nil, "", err
		}

		selectedToken = reserved.token
		client, err := newBotClientForStream(ctx, a.db, a.cache, &a.cnf.TG, reserved.token, a.newMiddlewares(ctx, 5)...)
		if err == nil {
			return client, reserved.cleanup, reserved.token, nil
		}
		if a.botHealth != nil {
			a.botHealth.RecordFailure(reserved.token, err)
		}
		reserved.cleanup()
		lastErr = err
	}

	if lastErr != nil {
		return nil, nil, selectedToken, lastErr
	}
	if capacityBlocked {
		return nil, nil, selectedToken, tgc.ErrBotStreamCapacityExceeded
	}
	return nil, nil, selectedToken, tgc.ErrBotClientTemporarilyUnavailable
}

func shouldFallbackToDirectStreamClient(token string, err error) bool {
	if errors.Is(err, tgc.ErrBotStreamCapacityExceeded) {
		return false
	}
	return true
}

type botPoolSuccessRecorder interface {
	RecordBotSuccess(key string)
}

func recordPooledBotSuccess(pool telegramClientPool, userID int64, token string) {
	if token == "" || pool == nil {
		return
	}
	recorder, ok := pool.(botPoolSuccessRecorder)
	if !ok {
		return
	}
	recorder.RecordBotSuccess(fmt.Sprintf("user:%d:bot:%s", userID, token))
}

func streamBotID(userID int64, token string) string {
	botID := strconv.FormatInt(userID, 10)
	if token == "" {
		return botID
	}

	parts := strings.SplitN(token, ":", 2)
	return parts[0]
}

func streamChunkFailureHandler(token string, botHealth *tgc.BotHealth, recorded *atomic.Bool) func(error) {
	if token == "" || botHealth == nil {
		return nil
	}

	return func(err error) {
		recorded.Store(true)
		botHealth.RecordFailure(token, err)
	}
}

func (e *extendedService) FilesStream(w http.ResponseWriter, r *http.Request, fileId string, userId int64) {
	ctx := r.Context()
	logger := logging.Component("FILE").With(zap.String("file_id", fileId))
	var (
		session *models.Session
		err     error
		user    *types.JWTClaims
	)
	if userId == 0 {

		authHash := r.URL.Query().Get("hash")
		if authHash == "" {
			cookie, err := r.Cookie(authCookieName)
			if err != nil {
				http.Error(w, "missing token or authash", http.StatusUnauthorized)
				return
			}
			user, err = auth.VerifyUser(ctx, e.api.db, e.api.cache, e.api.cnf.JWT.Secret, cookie.Value)
			if err != nil {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			userId, _ := strconv.ParseInt(user.Subject, 10, 64)
			session = &models.Session{UserId: userId, Session: user.TgSession}
		} else {
			session, err = auth.GetSessionByHash(ctx, e.api.db, e.api.cache, authHash)
			if err != nil {
				http.Error(w, "invalid hash", http.StatusBadRequest)
				return
			}
			userId = session.UserId
		}
	} else {
		session = &models.Session{UserId: userId}
	}
	logger = logger.With(zap.Int64("user_id", userId))

	file, err := cache.FetchWithStale(ctx, e.api.cache, cache.Key("files", fileId), fileMetadataCacheTTL, fileMetadataStaleTTL, func(fetchCtx context.Context) (*models.File, error) {
		var result models.File
		if err := e.api.db.WithContext(fetchCtx).Model(&result).Where("id = ?", fileId).First(&result).Error; err != nil {
			return nil, err
		}
		return &result, nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")

	var start, end int64

	rangeHeader := r.Header.Get("Range")
	contentType := defaultContentType

	if file.MimeType != "" {
		contentType = file.MimeType
	}

	if file.Size == nil || *file.Size == 0 {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": file.Name}))
		w.WriteHeader(http.StatusOK)
		return
	}

	status := http.StatusOK
	if rangeHeader == "" {
		start = 0
		end = *file.Size - 1
	} else {
		ranges, err := http_range.Parse(rangeHeader, *file.Size)
		if err == http_range.ErrNoOverlap {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", *file.Size))
			http.Error(w, http_range.ErrNoOverlap.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(ranges) > 1 {
			http.Error(w, "multiple ranges are not supported", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, *file.Size))
		status = http.StatusPartialContent

	}

	contentLength := end - start + 1

	w.Header().Set("Content-Type", contentType)

	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", md5.FromString(fileId+strconv.FormatInt(*file.Size, 10))))
	w.Header().Set("Last-Modified", file.UpdatedAt.UTC().Format(http.TimeFormat))

	disposition := "inline"

	download := r.URL.Query().Get("download") == "1"

	if download {
		disposition = "attachment"
	}

	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": file.Name}))

	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}

	tokens, err := e.api.channelManager.BotTokens(ctx, session.UserId)
	if err != nil {
		logger.Error("stream.bots_fetch_failed", zap.Error(err))
		http.Error(w, "failed to get bots", http.StatusInternalServerError)
		return
	}

	// Limit the number of bots used for streaming if configured
	if limit := e.api.cnf.TG.Stream.BotsLimit; limit > 0 && len(tokens) > limit {
		tokens = tokens[:limit]
	}

	var (
		client           *telegram.Client
		token            string
		releaseStreamBot func()
	)

	// Build chunk-fail callback for bot health tracking.
	// chunkFailRecorded prevents the same failure from being counted twice
	// (once by onChunkFail during reads, and again by the outer error handler).
	var chunkFailRecorded atomic.Bool

	if e.api.clientPool != nil {
		streamClient, cleanup, poolToken, poolErr := e.api.streamClientFromPoolWithBotRetry(ctx, session, tokens)
		token = poolToken
		if poolErr == nil {
			botID := streamBotID(session.UserId, token)
			onChunkFail := streamChunkFailureHandler(token, e.api.botHealth, &chunkFailRecorded)
			defer cleanup()
			streamErr := e.streamWithTGReader(ctx, w, logger, streamClient, file, start, end, contentLength, status, botID, onChunkFail)
			if token != "" && e.api.botHealth != nil {
				if streamErr == nil {
					recordPooledBotSuccess(e.api.clientPool, session.UserId, token)
					e.api.botHealth.RecordSuccess(token)
				} else if errors.Is(streamErr, ErrorStreamAbandoned) {
					// Client aborted stream; do not count as bot success/failure.
				} else if !chunkFailRecorded.Load() {
					e.api.botHealth.RecordFailure(token, streamErr)
				}
			}
			if errors.Is(streamErr, ErrorStreamAbandoned) {
				logger.Debug("stream.abandoned", zap.Error(streamErr))
				return
			}
			if streamErr != nil {
				logger.Error("stream.failed", zap.Error(streamErr))
			}
			return
		}
		if !shouldFallbackToDirectStreamClient(token, poolErr) {
			logger.Warn("stream.client_pool_unavailable", zap.Error(poolErr))
			http.Error(w, poolErr.Error(), http.StatusServiceUnavailable)
			return
		}
		logger.Error("stream.client_pool_failed", zap.Error(poolErr))
	}

	if len(tokens) == 0 {
		client, err = tgc.AuthClient(ctx, &e.api.cnf.TG, session.Session, e.api.newMiddlewares(ctx, 5)...)
		if err != nil {
			logger.Error("stream.auth_client_failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		client, releaseStreamBot, token, err = e.api.streamDirectBotClientWithRetry(ctx, session, tokens)
		if err != nil {
			if errors.Is(err, tgc.ErrBotStreamCapacityExceeded) {
				logger.Warn("stream.bot_capacity_exhausted", zap.Error(err))
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			logger.Error("stream.bot_client_failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer releaseStreamBot()
	}

	botID := streamBotID(session.UserId, token)
	onChunkFail := streamChunkFailureHandler(token, e.api.botHealth, &chunkFailRecorded)

	if err := tgc.RunWithAuth(ctx, client, token, func(ctx context.Context) error {
		streamErr := e.streamWithTGReader(ctx, w, logger, client.API(), file, start, end, contentLength, status, botID, onChunkFail)
		if token != "" && e.api.botHealth != nil {
			if streamErr == nil {
				e.api.botHealth.RecordSuccess(token)
			} else if errors.Is(streamErr, ErrorStreamAbandoned) {
				// Client aborted stream; do not count as bot success/failure.
			} else if !chunkFailRecorded.Load() {
				e.api.botHealth.RecordFailure(token, streamErr)
			}
		}
		return streamErr
	}); err != nil {
		if errors.Is(err, ErrorStreamAbandoned) {
			logger.Debug("stream.abandoned", zap.Error(err))
			return
		}
		logger.Error("stream.failed", zap.Error(err))
	}
}

func (e *extendedService) streamWithTGReader(
	ctx context.Context,
	w http.ResponseWriter,
	logger *zap.Logger,
	client *tg.Client,
	file *models.File,
	start, end, contentLength int64,
	status int,
	botID string,
	onChunkFail func(error),
) error {
	parts, err := fetchPartsForStream(ctx, client, e.api.cache, file, e.api.cnf.TG.SessionInstance, botID)
	if err != nil {
		logger.Error("stream.parts_fetch_failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}

	lr, err := newReaderForStream(ctx,
		client,
		e.api.cache,
		file,
		parts,
		start,
		end,
		&e.api.cnf.TG,
		botID,
		onChunkFail,
	)

	if err != nil {
		logger.Error("stream.reader_create_failed", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	if lr == nil {
		logger.Error("stream.reader_nil")
		http.Error(w, "failed to initialise reader", http.StatusInternalServerError)
		return errors.New("failed to initialise reader")
	}

	w.WriteHeader(status)

	buf := make([]byte, 256*1024) // 256KB buffer reduces syscall overhead vs default 32KB
	written, err := io.CopyBuffer(w, io.LimitReader(lr, contentLength), buf)
	if err != nil {
		lr.Close()
		if isStreamClientDisconnect(err) || errors.Is(ctx.Err(), context.Canceled) {
			return errors.Join(ErrorStreamAbandoned, err)
		}
		return err
	}
	if written < contentLength {
		lr.Close()
		return io.ErrUnexpectedEOF
	}
	return nil
}

func isStreamClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset by peer")
}

func (e *extendedService) SharesStream(w http.ResponseWriter, r *http.Request, shareId, fileId string) {
	share, err := e.api.validFileShare(r, shareId)
	if err != nil && errors.Is(err, ErrEmptyAuth) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	e.FilesStream(w, r, fileId, share.UserId)
}

func (a *apiService) FilesStream(ctx context.Context, params api.FilesStreamParams) (api.FilesStreamRes, error) {
	return nil, nil
}

func (a *apiService) SharesStream(ctx context.Context, params api.SharesStreamParams) (api.SharesStreamRes, error) {
	return nil, nil
}

func mapParts(_parts []api.Part) []api.Part {
	return utils.Map(_parts, func(part api.Part) api.Part {
		p := api.Part{ID: part.ID}
		if part.Salt.Value != "" {
			p.Salt = part.Salt
		}
		return p
	})

}
