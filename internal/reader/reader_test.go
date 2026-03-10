package reader

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
)

func TestCalculatePartByteRangesUsesActualPartSizes(t *testing.T) {
	parts := []types.Part{
		{ID: 1, Size: 5},
		{ID: 2, Size: 3},
		{ID: 3, Size: 4},
	}

	ranges, err := calculatePartByteRanges(parts, false, 3, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []Range{
		{Start: 3, End: 4, PartNo: 0},
		{Start: 0, End: 2, PartNo: 1},
		{Start: 0, End: 0, PartNo: 2},
	}
	if len(ranges) != len(want) {
		t.Fatalf("expected %d ranges, got %d", len(want), len(ranges))
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("unexpected range[%d]: got %+v want %+v", i, ranges[i], want[i])
		}
	}
}

func TestCalculatePartByteRangesUsesDecryptedSizesWhenEncrypted(t *testing.T) {
	parts := []types.Part{
		{ID: 1, Size: 100, DecryptedSize: 4},
		{ID: 2, Size: 100, DecryptedSize: 2},
	}

	ranges, err := calculatePartByteRanges(parts, true, 4, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []Range{{Start: 0, End: 1, PartNo: 1}}
	if len(ranges) != len(want) {
		t.Fatalf("expected %d ranges, got %d", len(want), len(ranges))
	}
	if ranges[0] != want[0] {
		t.Fatalf("unexpected range: got %+v want %+v", ranges[0], want[0])
	}
}

func TestCalculatePartByteRangesRejectsOutOfRangeRequest(t *testing.T) {
	parts := []types.Part{{ID: 1, Size: 4}}

	_, err := calculatePartByteRanges(parts, false, 4, 4)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRequestedRangeOutside) {
		t.Fatalf("expected ErrRequestedRangeOutside, got %v", err)
	}
}

func TestGetPartReaderEncryptedDoesNotOpenPlainReaderEagerly(t *testing.T) {
	originalPlain := openPlainPartReader
	originalEncrypted := openEncryptedPartReader
	t.Cleanup(func() {
		openPlainPartReader = originalPlain
		openEncryptedPartReader = originalEncrypted
	})

	plainCalls := 0
	encryptedCalls := 0
	openPlainPartReader = func(ctx context.Context, start, end int64, config *config.TGConfig, chunkSrc ChunkSource, onChunkFail func(error)) (io.ReadCloser, error) {
		plainCalls++
		return io.NopCloser(strings.NewReader("plain")), nil
	}
	openEncryptedPartReader = func(ctx context.Context, encryptionKey string, salt string, start, end int64, config *config.TGConfig, chunkSrc ChunkSource, onChunkFail func(error), partSize int64) (io.ReadCloser, error) {
		encryptedCalls++
		return io.NopCloser(strings.NewReader("encrypted")), nil
	}

	channelID := int64(1)
	encrypted := true
	r := &Reader{
		ctx:    context.Background(),
		file:   &models.File{ChannelId: &channelID, Encrypted: &encrypted},
		parts:  []types.Part{{ID: 10, Size: 128, DecryptedSize: 64, Salt: "salt"}},
		ranges: []Range{{Start: 0, End: 10, PartNo: 0}},
		config: &config.TGConfig{},
	}

	rc, err := r.getPartReader()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	if encryptedCalls != 1 {
		t.Fatalf("expected encrypted reader path once, got %d", encryptedCalls)
	}
	if plainCalls != 0 {
		t.Fatalf("expected plain reader path to stay unused, got %d calls", plainCalls)
	}
}
