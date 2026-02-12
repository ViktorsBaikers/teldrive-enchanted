package reader

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
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

var retryableChunkMarkers = []string{
	"timeout",
	"timedout",
	"rpc error code -503",
	"worker_busy_too_long_retry",
	"rpc_call_fail",
	"rpc_mcget_fail",
	"connection reset",
	"connection dead",
	"connection refused",
	"temporary unavailable",
}

const (
	maxChunkRetries          = 2
	chunkRetryInitialBackoff = 150 * time.Millisecond
	chunkRetryMaxBackoff     = 2 * time.Second
	chunkRetryMultiplier     = 2.0
	chunkRetryJitterFactor   = 0.3
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
			chunk, err := r.fetchChunkWithRetry(r.ctx, offset, part)
			if err != nil {
				slot <- chunkResult{err: err}
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

func (r *tgMultiReader) fetchChunkWithRetry(ctx context.Context, offset int64, part int) ([]byte, error) {
	bo := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(chunkRetryInitialBackoff),
		backoff.WithRandomizationFactor(chunkRetryJitterFactor),
		backoff.WithMultiplier(chunkRetryMultiplier),
		backoff.WithMaxInterval(chunkRetryMaxBackoff),
	)

	attempt := 0
	for {
		chunkCtx, cancel := context.WithTimeout(ctx, r.timeout)
		chunk, err := r.chunkSrc.Chunk(chunkCtx, offset, r.chunkSize)
		cancel()

		if err == nil {
			return chunk, nil
		}

		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("chunk %d: %w", part, ErrChunkTimeout)
		}

		if attempt >= maxChunkRetries || !isRetryableChunkError(err) {
			return nil, err
		}

		waitFor := bo.NextBackOff()
		if waitFor == backoff.Stop {
			return nil, err
		}

		attempt++
		r.logger.Warn("stream.chunk_retry",
			zap.Int("part", part),
			zap.Int("attempt", attempt),
			zap.Duration("wait", waitFor),
			zap.Error(err))

		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func isRetryableChunkError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrChunkTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	msg := strings.ToLower(err.Error())
	for _, marker := range retryableChunkMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}

	return false
}
