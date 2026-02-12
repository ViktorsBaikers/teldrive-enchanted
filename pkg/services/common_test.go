package services

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"gorm.io/datatypes"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
)

func TestBuildPartsAndPrimeLocations(t *testing.T) {
	ctx := context.Background()
	c := cache.NewMemoryCache(1 << 20)
	encrypted := false
	parts := datatypes.NewJSONSlice([]api.Part{
		{ID: 101, Salt: api.NewOptString("salt-a")},
		{ID: 102, Salt: api.NewOptString("salt-b")},
	})
	file := &models.File{
		ID:        "file-1",
		Encrypted: &encrypted,
		Parts:     &parts,
	}

	messages := []tg.MessageClass{
		&tg.Message{
			Media: &tg.MessageMediaDocument{
				Document: &tg.Document{
					ID:            11,
					AccessHash:    21,
					FileReference: []byte{1, 1},
					DCID:          4,
					Size:          10,
				},
			},
		},
		&tg.Message{
			Media: &tg.MessageMediaDocument{
				Document: &tg.Document{
					ID:            12,
					AccessHash:    22,
					FileReference: []byte{2, 2},
					DCID:          4,
					Size:          20,
				},
			},
		},
	}

	got := buildPartsAndPrimeLocations(ctx, c, file, messages, "instance-a", "bot-1")
	if len(got) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(got))
	}

	var loc tg.InputDocumentFileLocation
	if err := c.Get(ctx, cache.KeyFileLocation("instance-a", "bot-1", file.ID, int64(101)), &loc); err != nil {
		t.Fatalf("expected primed cache for first part location: %v", err)
	}
	if loc.ID != 11 {
		t.Fatalf("expected first location id 11, got %d", loc.ID)
	}

	if err := c.Get(ctx, cache.KeyFileLocation("instance-a", "bot-1", file.ID, int64(102)), &loc); err != nil {
		t.Fatalf("expected primed cache for second part location: %v", err)
	}
	if loc.ID != 12 {
		t.Fatalf("expected second location id 12, got %d", loc.ID)
	}
}

func TestBuildPartsAndPrimeLocations_EncryptedFileSetsDecryptedSize(t *testing.T) {
	ctx := context.Background()
	c := cache.NewMemoryCache(1 << 20)
	encrypted := true
	parts := datatypes.NewJSONSlice([]api.Part{
		{ID: 201, Salt: api.NewOptString("salt-z")},
	})
	file := &models.File{
		ID:        "file-enc",
		Encrypted: &encrypted,
		Parts:     &parts,
	}

	docSize := int64(2000)
	messages := []tg.MessageClass{
		&tg.Message{
			Media: &tg.MessageMediaDocument{
				Document: &tg.Document{
					ID:            51,
					AccessHash:    61,
					FileReference: []byte{9, 9},
					DCID:          4,
					Size:          docSize,
				},
			},
		},
	}

	got := buildPartsAndPrimeLocations(ctx, c, file, messages, "instance-b", "bot-2")
	if len(got) != 1 {
		t.Fatalf("expected one part, got %d", len(got))
	}

	expected, err := crypt.DecryptedSize(docSize)
	if err != nil {
		t.Fatalf("unexpected decrypted size error: %v", err)
	}
	if got[0].DecryptedSize != expected {
		t.Fatalf("expected decrypted size %d, got %d", expected, got[0].DecryptedSize)
	}
}
