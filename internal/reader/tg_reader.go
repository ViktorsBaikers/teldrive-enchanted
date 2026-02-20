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
	locationCacheTTL = 60 * time.Minute
)

var (
	getLocation   = tgc.GetLocationCached
	getChunk      = tgc.GetChunk
	getChunkNoCDN = tgc.GetChunkNoCDN
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
	tgConfig    *config.TGConfig
	streamCtx   context.Context // long-lived context for CDN connection lifecycle
	cdnFetcher  *tgc.CDNFetcher
	cdnChecked  bool
}

func (c *chunkSource) ChunkSize(start, end int64) int64 {
	return tgc.CalculateChunkSize(start, end)
}

func (c *chunkSource) Chunk(ctx context.Context, offset int64, limit int64) ([]byte, error) {
	// If CDN fetcher is active, use it directly
	if c.cdnFetcher != nil {
		return c.cdnFetcher.Chunk(ctx, offset, limit)
	}

	location, err := c.loadLocation(ctx)
	if err != nil {
		return nil, err
	}

	data, err := getChunk(ctx, c.client, &location, offset, limit)
	if err != nil {
		var cdnRedirect *tgc.CDNRedirect
		if !c.cdnChecked && errors.As(err, &cdnRedirect) {
			c.cdnChecked = true
			fetcher, cdnErr := tgc.NewCDNFetcher(c.streamCtx, c.client, cdnRedirect.Info, c.tgConfig)
			if cdnErr != nil {
				// CDN connection failed — fall back to non-CDN fetch
				logging.FromContext(ctx).Warn("cdn.fallback",
					zap.Error(cdnErr), zap.Int("cdn_dc", cdnRedirect.Info.DCID))
				return getChunkNoCDN(ctx, c.client, &location, offset, limit)
			}
			c.cdnFetcher = fetcher
			return fetcher.Chunk(ctx, offset, limit)
		}
		return nil, err
	}
	return data, nil
}

// closeCDN cleans up the CDN fetcher connection if active.
func (c *chunkSource) closeCDN() {
	if c.cdnFetcher != nil {
		c.cdnFetcher.Close()
		c.cdnFetcher = nil
	}
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
		if cs, ok := r.chunkSrc.(*chunkSource); ok {
			cs.closeCDN()
		}
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

type chunkResult struct {
	buf *buffer
	err error
}

func (r *tgMultiReader) fillBatch() error {
	batchSize := min(r.concurrency, r.totalParts-r.currentPart)

	// Each goroutine writes to its own buffered slot so it never blocks.
	// The loop below drains slots in order, streaming each chunk to the
	// consumer as soon as it is ready instead of waiting for the whole batch.
	slots := make([]chan chunkResult, batchSize)
	for i := 0; i < batchSize; i++ {
		slots[i] = make(chan chunkResult, 1)
		part := r.currentPart + i
		offset := r.offset + int64(i)*r.chunkSize
		slot := slots[i]
		go func() {
			chunkCtx, cancel := context.WithTimeout(r.ctx, r.timeout)
			chunk, err := r.chunkSrc.Chunk(chunkCtx, offset, r.chunkSize)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					err = fmt.Errorf("chunk %d: %w", part, ErrChunkTimeout)
				}
				slot <- chunkResult{err: err}
				return
			}

			var requiredMinLen int64
			switch {
			case r.totalParts == 1:
				requiredMinLen = r.rightCut
			case part == 0:
				// Need at least one byte beyond leftCut; otherwise the chunk doesn't
				// cover the start offset and slicing may panic.
				requiredMinLen = r.leftCut + 1
			case part+1 == r.totalParts:
				requiredMinLen = r.rightCut
			default:
				requiredMinLen = r.chunkSize
			}
			if int64(len(chunk)) < requiredMinLen {
				slot <- chunkResult{err: fmt.Errorf(
					"chunk %d: short read (offset=%d got=%d need>=%d): %w",
					part, offset, len(chunk), requiredMinLen, io.ErrUnexpectedEOF,
				)}
				return
			}

			if r.totalParts == 1 {
				chunk = chunk[r.leftCut:r.rightCut]
			} else if part == 0 {
				chunk = chunk[r.leftCut:]
			} else if part+1 == r.totalParts {
				chunk = chunk[:r.rightCut]
			}

			slot <- chunkResult{buf: &buffer{buf: chunk}}
		}()
	}

	for _, slot := range slots {
		select {
		case res := <-slot:
			if res.err != nil {
				if !errors.Is(res.err, context.Canceled) {
					if r.onChunkFail != nil {
						r.onChunkFail(res.err)
					}
					r.logger.Error("stream.chunk_failed", zap.Error(res.err),
						zap.Int("part", r.currentPart), zap.Int("total_parts", r.totalParts))
				}
				return res.err
			}
			select {
			case r.bufferChan <- res.buf:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	r.currentPart += batchSize
	r.offset += r.chunkSize * int64(batchSize)

	return nil
}
