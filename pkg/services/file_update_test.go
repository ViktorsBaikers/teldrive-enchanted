package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"github.com/ViktorsBaikers/teldrive/internal/events"
	"github.com/ViktorsBaikers/teldrive/internal/hash"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type noopBroadcaster struct{}

func (noopBroadcaster) Subscribe(int64) chan models.Event    { return make(chan models.Event) }
func (noopBroadcaster) Unsubscribe(int64, chan models.Event) {}
func (noopBroadcaster) Record(events.EventType, int64, *models.Source) {
}
func (noopBroadcaster) Shutdown() {}

func TestFilesUpdateUploadIDInfersEncryptionWhenFieldOmitted(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-uploadid-infers-encryption?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedTrue := true
	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-1",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedTrue,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	upload := &models.Upload{
		UploadId:  "upload-1",
		UserId:    1,
		Name:      "new.bin",
		PartNo:    1,
		PartId:    101,
		Encrypted: false,
		Size:      size,
		CreatedAt: now,
	}
	if err := db.Create(upload).Error; err != nil {
		t.Fatalf("create upload: %v", err)
	}

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-1"),
		Name:     api.NewOptString("new.bin"),
	}

	out, err := svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-1"})
	if err != nil {
		t.Fatalf("FilesUpdate returned error: %v", err)
	}
	if !out.Encrypted.IsSet() {
		t.Fatal("expected encrypted field in response")
	}
	if out.Encrypted.Value {
		t.Fatal("expected updated file to become unencrypted")
	}

	var updated models.File
	if err := db.First(&updated, "id = ?", "file-1").Error; err != nil {
		t.Fatalf("reload file: %v", err)
	}
	if updated.Encrypted == nil || *updated.Encrypted {
		t.Fatalf("expected stored encrypted=false, got %+v", updated.Encrypted)
	}
}

func TestFilesUpdateDoesNotApplyNonzeroSizeWithoutReplacementParts(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-size-without-parts?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-size-only",
		Name:      "old.bin",
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

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		Name: api.NewOptString("renamed.bin"),
		Size: api.NewOptInt64(25),
	}

	out, err := svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-size-only"})
	if err != nil {
		t.Fatalf("FilesUpdate returned error: %v", err)
	}
	if out.Name != "renamed.bin" {
		t.Fatalf("expected renamed file, got %q", out.Name)
	}
	if !out.Size.IsSet() || out.Size.Value != 10 {
		t.Fatalf("expected size to remain 10, got %+v", out.Size)
	}

	var updated models.File
	if err := db.First(&updated, "id = ?", "file-size-only").Error; err != nil {
		t.Fatalf("reload file: %v", err)
	}
	if updated.Size == nil || *updated.Size != 10 {
		t.Fatalf("expected stored size to remain 10, got %+v", updated.Size)
	}
}

func TestFilesUpdateUploadIDPersistsPartsForEncryptedZeroByteReplacement(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-zero-byte-encrypted?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedFalse := false
	size := int64(10)
	now := time.Now().UTC()
	oldParts := datatypes.NewJSONSlice([]api.Part{{ID: 101, Salt: api.NewOptString("old-salt")}})
	file := &models.File{
		ID:        "file-2",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedFalse,
		Parts:     &oldParts,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	upload := &models.Upload{
		UploadId:  "upload-zero",
		UserId:    1,
		Name:      "empty.bin",
		PartNo:    1,
		PartId:    202,
		Encrypted: true,
		Salt:      crypt.StoredSalt("new-salt"),
		Size:      crypt.EncryptedSize(0),
		CreatedAt: now,
	}
	if err := db.Create(upload).Error; err != nil {
		t.Fatalf("create upload: %v", err)
	}

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-zero"),
		Name:     api.NewOptString("empty.bin"),
	}

	out, err := svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-2"})
	if err != nil {
		t.Fatalf("FilesUpdate returned error: %v", err)
	}
	if !out.Encrypted.IsSet() || !out.Encrypted.Value {
		t.Fatalf("expected updated file to become encrypted, got %+v", out.Encrypted)
	}
	if !out.Size.IsSet() || out.Size.Value != 0 {
		t.Fatalf("expected updated file size 0, got %+v", out.Size)
	}

	var updated models.File
	if err := db.First(&updated, "id = ?", "file-2").Error; err != nil {
		t.Fatalf("reload file: %v", err)
	}
	if updated.Size == nil || *updated.Size != 0 {
		t.Fatalf("expected stored size 0, got %+v", updated.Size)
	}
	if updated.Encrypted == nil || !*updated.Encrypted {
		t.Fatalf("expected stored encrypted=true, got %+v", updated.Encrypted)
	}
	emptyHash := hash.SumToHex(hash.ComputeTreeHash([]byte{}))
	if updated.Hash == nil || *updated.Hash != emptyHash {
		t.Fatalf("expected stored empty hash %q, got %+v", emptyHash, updated.Hash)
	}
	if updated.Parts == nil || len(*updated.Parts) != 1 {
		t.Fatalf("expected one stored part, got %+v", updated.Parts)
	}
	if (*updated.Parts)[0].ID != 202 {
		t.Fatalf("expected stored part id 202, got %+v", (*updated.Parts)[0])
	}
	expectedSalt := crypt.StoredSalt("new-salt")
	if !(*updated.Parts)[0].Salt.IsSet() || (*updated.Parts)[0].Salt.Value != expectedSalt {
		t.Fatalf("expected stored part salt %q, got %+v", expectedSalt, (*updated.Parts)[0].Salt)
	}
}

func TestFilesUpdateUploadIDResolvesAmbiguousEncryptedSizeWhenOmitted(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-uploadid-resolves-ambiguous-encrypted-size?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedFalse := false
	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-ambiguous",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedFalse,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	uploads := []*models.Upload{
		{
			UploadId:  "upload-ambiguous",
			UserId:    1,
			Name:      "part-1.bin",
			PartNo:    1,
			PartId:    101,
			Encrypted: true,
			Salt:      "raw-salt-a",
			Size:      crypt.EncryptedSize(5),
			CreatedAt: now,
		},
		{
			UploadId:  "upload-ambiguous",
			UserId:    1,
			Name:      "part-2.bin",
			PartNo:    2,
			PartId:    102,
			Encrypted: true,
			Salt:      "raw-salt-b",
			Size:      crypt.EncryptedSize(7),
			CreatedAt: now,
		},
	}
	for _, upload := range uploads {
		if err := db.Create(upload).Error; err != nil {
			t.Fatalf("create upload: %v", err)
		}
	}

	originalResolver := resolveAmbiguousUploadBackedFileSize
	resolveAmbiguousUploadBackedFileSize = func(_ *apiService, _ context.Context, userID int64, uploads []models.Upload) (int64, error) {
		if userID != 0 {
			t.Fatalf("expected background context user id 0, got %d", userID)
		}
		if len(uploads) != 2 {
			t.Fatalf("expected 2 uploads, got %d", len(uploads))
		}
		return 12, nil
	}
	defer func() { resolveAmbiguousUploadBackedFileSize = originalResolver }()

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-ambiguous"),
		Name:     api.NewOptString("new.bin"),
	}

	out, err := svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-ambiguous"})
	if err != nil {
		t.Fatalf("FilesUpdate returned error: %v", err)
	}
	if !out.Size.IsSet() || out.Size.Value != 12 {
		t.Fatalf("expected updated file size 12, got %+v", out.Size)
	}
	if !out.Encrypted.IsSet() || !out.Encrypted.Value {
		t.Fatalf("expected updated file to become encrypted, got %+v", out.Encrypted)
	}
}

func TestFilesUpdateUploadIDAmbiguousEncryptedSizeResolverError(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-uploadid-ambiguous-encrypted-size-resolver-error?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedFalse := false
	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-ambiguous-error",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedFalse,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	upload := &models.Upload{
		UploadId:  "upload-ambiguous-error",
		UserId:    1,
		Name:      "part-1.bin",
		PartNo:    1,
		PartId:    101,
		Encrypted: true,
		Salt:      "raw-salt-a",
		Size:      crypt.EncryptedSize(5),
		CreatedAt: now,
	}
	if err := db.Create(upload).Error; err != nil {
		t.Fatalf("create upload: %v", err)
	}

	originalResolver := resolveAmbiguousUploadBackedFileSize
	resolveAmbiguousUploadBackedFileSize = func(_ *apiService, _ context.Context, _ int64, _ []models.Upload) (int64, error) {
		return 0, errors.New("resolver failed")
	}
	defer func() { resolveAmbiguousUploadBackedFileSize = originalResolver }()

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-ambiguous-error"),
		Name:     api.NewOptString("new.bin"),
	}

	_, err = svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-ambiguous-error"})
	if err == nil {
		t.Fatal("expected error")
	}
	apiErr := new(apiError)
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected apiError, got %T", err)
	}
	if apiErr.code != 0 {
		t.Fatalf("expected internal apiError with code 0, got %d", apiErr.code)
	}
}

func TestFilesUpdateUploadIDAmbiguousEncryptedSizeRespectsDeclaredLogicalSize(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-uploadid-ambiguous-encrypted-size-declared-size?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedFalse := false
	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-ambiguous-size",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedFalse,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	uploads := []*models.Upload{
		{
			UploadId:  "upload-ambiguous-size",
			UserId:    1,
			Name:      "part-1.bin",
			PartNo:    1,
			PartId:    101,
			Encrypted: true,
			Salt:      "raw-salt-a",
			Size:      100,
			CreatedAt: now,
		},
		{
			UploadId:  "upload-ambiguous-size",
			UserId:    1,
			Name:      "part-2.bin",
			PartNo:    2,
			PartId:    102,
			Encrypted: true,
			Salt:      crypt.StoredSalt("current-salt"),
			Size:      crypt.EncryptedSize(7),
			CreatedAt: now,
		},
	}
	for _, upload := range uploads {
		if err := db.Create(upload).Error; err != nil {
			t.Fatalf("create upload: %v", err)
		}
	}

	originalResolver := resolveAmbiguousUploadBackedFileSize
	resolveAmbiguousUploadBackedFileSize = func(_ *apiService, _ context.Context, _ int64, _ []models.Upload) (int64, error) {
		return 107, nil
	}
	defer func() { resolveAmbiguousUploadBackedFileSize = originalResolver }()

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-ambiguous-size"),
		Name:     api.NewOptString("new.bin"),
		Size:     api.NewOptInt64(200),
	}

	_, err = svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-ambiguous-size"})
	if err == nil {
		t.Fatal("expected error")
	}
	apiErr := new(apiError)
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected apiError, got %T", err)
	}
	if apiErr.code != 400 {
		t.Fatalf("expected 400, got %d", apiErr.code)
	}
	if !errors.Is(apiErr.err, ErrUploadedPartsSizeMismatch) {
		t.Fatalf("expected ErrUploadedPartsSizeMismatch, got %v", apiErr.err)
	}
}

func TestFilesUpdateUploadIDAmbiguousEncryptedSizeAcceptsDeclaredLogicalSize(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:files-update-uploadid-ambiguous-encrypted-size-declared-size-success?mode=memory&cache=shared"), &gorm.Config{})
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
		CREATE TABLE uploads (
			upload_id TEXT,
			user_id INTEGER,
			name TEXT,
			part_no INTEGER,
			part_id INTEGER,
			encrypted NUMERIC,
			salt TEXT,
			block_hashes BLOB,
			channel_id INTEGER,
			size INTEGER,
			created_at DATETIME
		)
	`).Error; err != nil {
		t.Fatalf("create uploads table: %v", err)
	}

	encryptedFalse := false
	size := int64(10)
	now := time.Now().UTC()
	file := &models.File{
		ID:        "file-ambiguous-size-success",
		Name:      "old.bin",
		Type:      "file",
		MimeType:  "application/octet-stream",
		UserId:    1,
		Status:    "active",
		Size:      &size,
		Encrypted: &encryptedFalse,
		UpdatedAt: &now,
	}
	if err := db.Create(file).Error; err != nil {
		t.Fatalf("create file: %v", err)
	}

	uploads := []*models.Upload{
		{
			UploadId:  "upload-ambiguous-size-success",
			UserId:    1,
			Name:      "part-1.bin",
			PartNo:    1,
			PartId:    101,
			Encrypted: true,
			Salt:      "raw-salt-a",
			Size:      100,
			CreatedAt: now,
		},
		{
			UploadId:  "upload-ambiguous-size-success",
			UserId:    1,
			Name:      "part-2.bin",
			PartNo:    2,
			PartId:    102,
			Encrypted: true,
			Salt:      crypt.StoredSalt("current-salt"),
			Size:      crypt.EncryptedSize(7),
			CreatedAt: now,
		},
	}
	for _, upload := range uploads {
		if err := db.Create(upload).Error; err != nil {
			t.Fatalf("create upload: %v", err)
		}
	}

	originalResolver := resolveAmbiguousUploadBackedFileSize
	resolveAmbiguousUploadBackedFileSize = func(_ *apiService, _ context.Context, _ int64, _ []models.Upload) (int64, error) {
		return 107, nil
	}
	defer func() { resolveAmbiguousUploadBackedFileSize = originalResolver }()

	svc := &apiService{
		db:     db,
		cnf:    &config.ServerCmdConfig{},
		cache:  cache.NewMemoryCache(1 << 20),
		events: noopBroadcaster{},
	}
	svc.cnf.TG.Uploads.Retention = time.Hour

	req := &api.FileUpdate{
		UploadId: api.NewOptString("upload-ambiguous-size-success"),
		Name:     api.NewOptString("new.bin"),
		Size:     api.NewOptInt64(107),
	}

	out, err := svc.FilesUpdate(context.Background(), req, api.FilesUpdateParams{ID: "file-ambiguous-size-success"})
	if err != nil {
		t.Fatalf("FilesUpdate returned error: %v", err)
	}
	if !out.Size.IsSet() || out.Size.Value != 107 {
		t.Fatalf("expected updated file size 107, got %+v", out.Size)
	}
}
