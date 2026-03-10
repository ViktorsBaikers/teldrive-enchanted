package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/internal/crypt"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
	"github.com/gotd/td/tg"
)

type Range struct {
	Start, End int64
	PartNo     int64
}

var (
	ErrInvalidPartLayout     = errors.New("invalid file part layout")
	ErrRequestedRangeOutside = errors.New("requested range exceeds available parts")
	openPlainPartReader      = func(ctx context.Context, start, end int64, config *config.TGConfig, chunkSrc ChunkSource, onChunkFail func(error)) (io.ReadCloser, error) {
		return newTGMultiReader(ctx, start, end, config, chunkSrc, onChunkFail)
	}
	openEncryptedPartReader = func(ctx context.Context, encryptionKey string, salt string, start, end int64, config *config.TGConfig, chunkSrc ChunkSource, onChunkFail func(error), partSize int64) (io.ReadCloser, error) {
		cipher, err := crypt.NewCipher(encryptionKey, salt)
		if err != nil {
			return nil, err
		}
		return cipher.DecryptDataSeek(ctx,
			func(ctx context.Context, underlyingOffset, underlyingLimit int64) (io.ReadCloser, error) {
				end := int64(-1)
				if underlyingLimit >= 0 {
					end = min(partSize-1, underlyingOffset+underlyingLimit-1)
				}
				return openPlainPartReader(ctx, underlyingOffset, end, config, chunkSrc, onChunkFail)
			},
			start,
			end-start+1,
		)
	}
)

type Reader struct {
	ctx         context.Context
	file        *models.File
	parts       []types.Part
	ranges      []Range
	pos         int
	reader      io.ReadCloser
	remaining   int64
	config      *config.TGConfig
	client      *tg.Client
	concurrency int
	cache       cache.Cacher
	closeOnce   sync.Once
	closeErr    error
	botID       string
	onChunkFail func(error)
}

func calculatePartByteRanges(parts []types.Part, encrypted bool, start, end int64) ([]Range, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: no parts available", ErrInvalidPartLayout)
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("%w: invalid range %d-%d", ErrInvalidPartLayout, start, end)
	}

	ranges := make([]Range, 0, len(parts))
	var totalSize int64

	for idx, part := range parts {
		partSize := part.Size
		if encrypted {
			partSize = part.DecryptedSize
		}
		if partSize <= 0 {
			return nil, fmt.Errorf("%w: part %d has invalid size %d", ErrInvalidPartLayout, idx, partSize)
		}

		partStartOffset := totalSize
		partEndOffset := totalSize + partSize - 1
		if start <= partEndOffset && end >= partStartOffset {
			ranges = append(ranges, Range{
				Start:  max(start-partStartOffset, 0),
				End:    min(partSize-1, end-partStartOffset),
				PartNo: int64(idx),
			})
		}

		totalSize += partSize
	}

	if len(ranges) == 0 || end >= totalSize {
		return nil, fmt.Errorf("%w: requested %d-%d, available size %d", ErrRequestedRangeOutside, start, end, totalSize)
	}

	return ranges, nil
}

func NewReader(ctx context.Context,
	client *tg.Client,
	cache cache.Cacher,
	file *models.File,
	parts []types.Part,
	start,
	end int64,
	config *config.TGConfig,
	botID string,
	onChunkFail func(error),
) (io.ReadCloser, error) {
	ranges, err := calculatePartByteRanges(parts, *file.Encrypted, start, end)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		ctx:         ctx,
		parts:       parts,
		file:        file,
		remaining:   end - start + 1,
		ranges:      ranges,
		config:      config,
		client:      client,
		cache:       cache,
		botID:       botID,
		onChunkFail: onChunkFail,
	}

	if err := r.initializeReader(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	n, err := r.reader.Read(p)
	r.remaining -= int64(n)

	if err == io.EOF && r.remaining > 0 {
		if err := r.moveToNextPart(); err != nil {
			return n, err
		}
		err = nil
	}

	return n, err
}

func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		if r.reader != nil {
			r.closeErr = r.reader.Close()
			r.reader = nil
		}
	})
	return r.closeErr
}

func (r *Reader) initializeReader() error {
	reader, err := r.getPartReader()
	if err != nil {
		return err
	}
	r.reader = reader
	return nil
}

func (r *Reader) moveToNextPart() error {
	r.reader.Close()
	r.pos++
	if r.pos < len(r.ranges) {
		return r.initializeReader()
	}
	return io.EOF
}

func (r *Reader) getPartReader() (io.ReadCloser, error) {
	currentRange := r.ranges[r.pos]
	partIndex := int(currentRange.PartNo)
	if partIndex < 0 || partIndex >= len(r.parts) {
		return nil, fmt.Errorf("%w: part index %d out of %d", ErrInvalidPartLayout, currentRange.PartNo, len(r.parts))
	}
	part := r.parts[partIndex]
	partId := part.ID

	chunkSrc := &chunkSource{
		channelId:   *r.file.ChannelId,
		partId:      partId,
		client:      r.client,
		concurrency: r.concurrency,
		cache:       r.cache,
		key:         cache.KeyFileLocation(r.config.SessionInstance, r.botID, r.file.ID, partId),
		tgConfig:    r.config,
		streamCtx:   r.ctx,
	}

	if *r.file.Encrypted {
		return openEncryptedPartReader(
			r.ctx,
			r.config.Uploads.EncryptionKey,
			part.Salt,
			currentRange.Start,
			currentRange.End,
			r.config,
			chunkSrc,
			r.onChunkFail,
			part.Size,
		)
	}

	return openPlainPartReader(r.ctx, currentRange.Start, currentRange.End, r.config, chunkSrc, r.onChunkFail)
}
