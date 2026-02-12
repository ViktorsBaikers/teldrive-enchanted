package reader

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/td/tg"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/internal/logging"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"go.uber.org/zap"
)

var (
	ErrStreamAbandoned = errors.New("stream abandoned")
	ErrChunkTimeout    = errors.New("chunk fetch timed out")
)

const (
	locationCacheTTL = 30 * time.Minute
)

var (
	getLocation = tgc.GetLocationCached
	getChunk    = tgc.GetChunk
)

type ChunkSource interface {
	Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error)
	ChunkSize(start, end int64) int64
}

type chunkSource struct {
	channelId   int64
	partId      int64
	concurrency int
	client      *tg.Client
	key         string
	cache       cache.Cacher
}

func (c *chunkSource) ChunkSize(start, end int64) int64 {
	return tgc.CalculateChunkSize(start, end)
}

func (c *chunkSource) Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	location, err := c.loadLocation(ctx)
	if err != nil {
		return nil, err
	}

	return getChunk(ctx, c.client, &location, offset, limit)
}

func (c *chunkSource) loadLocation(ctx context.Context) (tg.InputDocumentFileLocation, error) {
	var location tg.InputDocumentFileLocation
	if err := c.cache.Get(ctx, c.key, &location); err == nil {
		return location, nil
	}

	loc, err := getLocation(ctx, c.client, c.cache, c.channelId, c.partId)
	if err != nil {
		return tg.InputDocumentFileLocation{}, err
	}
	_ = c.cache.Set(ctx, c.key, loc, locationCacheTTL)
	return *loc, nil
}

type tgMultiReader struct {
	ctx         context.Context
	cancel      context.CancelFunc
	offset      int64
	limit       int64
	chunkSize   int64
	bufferChan  chan *buffer
	cur         *buffer
	concurrency int
	leftCut     int64
	rightCut    int64
	totalParts  int
	currentPart int
	chunkSrc    ChunkSource
	timeout     time.Duration
	logger      *zap.Logger
	closeOnce   sync.Once
	onChunkFail func(error)
}

func newTGMultiReader(
	ctx context.Context,
	start int64,
	end int64,
	config *config.TGConfig,
	chunkSrc ChunkSource,
	onChunkFail func(error),
) (*tgMultiReader, error) {
	chunkSize := chunkSrc.ChunkSize(start, end)
	offset := start - (start % chunkSize)

	ctx, cancel := context.WithCancel(ctx)

	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		limit:       end - start + 1,
		bufferChan:  make(chan *buffer, config.Stream.Buffers),
		concurrency: config.Stream.Concurrency,
		leftCut:     start - offset,
		rightCut:    (end % chunkSize) + 1,
		totalParts:  int((end - offset + chunkSize) / chunkSize),
		offset:      offset,
		chunkSize:   chunkSize,
		chunkSrc:    chunkSrc,
		timeout:     config.Stream.ChunkTimeout,
		logger:      logging.FromContext(ctx),
		onChunkFail: onChunkFail,
	}

	go r.fillBuffer()
	return r, nil
}

func (r *tgMultiReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
	})
	return nil
}

func (r *tgMultiReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, io.EOF
	}

	if r.cur == nil || r.cur.isEmpty() {
		select {
		case cur, ok := <-r.bufferChan:
			if !ok {
				return 0, ErrStreamAbandoned
			}
			r.cur = cur
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}

	n := copy(p, r.cur.buffer())
	r.cur.increment(n)
	r.limit -= int64(n)

	if r.limit <= 0 {
		return n, io.EOF
	}

	return n, nil
}

func (r *tgMultiReader) fillBuffer() {
	defer close(r.bufferChan)

	for r.currentPart < r.totalParts {
		if err := r.fillBatch(); err != nil {
			r.cancel()
			return
		}
	}
}

func (r *tgMultiReader) fillBatch() error {
	batchSize := min(r.concurrency, r.totalParts-r.currentPart)

	buffers := make([]*buffer, batchSize)

	for i := 0; i < batchSize; i++ {
		part := r.currentPart + i

		chunkCtx, cancel := context.WithTimeout(r.ctx, r.timeout)
		chunk, err := r.chunkSrc.Chunk(chunkCtx, r.offset+int64(i)*r.chunkSize, r.chunkSize)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("chunk %d: %w", part, ErrChunkTimeout)
			}
			if !errors.Is(err, context.Canceled) {
				if r.onChunkFail != nil {
					r.onChunkFail(err)
				}
				r.logger.Error("stream.chunk_failed", zap.Error(err),
					zap.Int("part", part), zap.Int("total_parts", r.totalParts))
			}
			return err
		}

		if r.totalParts == 1 {
			chunk = chunk[r.leftCut:r.rightCut]
		} else if part == 0 {
			chunk = chunk[r.leftCut:]
		} else if part+1 == r.totalParts {
			chunk = chunk[:r.rightCut]
		}

		buffers[i] = &buffer{buf: chunk}
	}

	for _, buf := range buffers {
		select {
		case r.bufferChan <- buf:
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	r.currentPart += batchSize
	r.offset += r.chunkSize * int64(batchSize)

	return nil
}
