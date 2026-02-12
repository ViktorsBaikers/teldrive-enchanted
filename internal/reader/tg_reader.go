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
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/logging"
	"github.com/tgdrive/teldrive/internal/tgc"
	"github.com/valyala/bytebufferpool"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

var (
	ErrStreamAbandoned = errors.New("stream abandoned")
	ErrChunkTimeout    = errors.New("chunk fetch timed out")
)

const (
	locationCacheTTL         = 30 * time.Minute
	locationNegativeCacheTTL = 10 * time.Second
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
	getLocation = tgc.GetLocation
	getChunk    = tgc.GetChunk

	locationFetchGroup singleflight.Group
)

type locationLookupFailure struct {
	Message            string
	DeadlineExceeded   bool
	Canceled           bool
	ChunkTimeoutMarker bool
}

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

	resultCh := locationFetchGroup.DoChan(c.key, func() (interface{}, error) {
		fetchCtx, cancel := detachedDeadlineContext(ctx)
		defer cancel()

		var cached tg.InputDocumentFileLocation
		if err := c.cache.Get(fetchCtx, c.key, &cached); err == nil {
			return cached, nil
		}

		if err := c.getCachedLocationFailure(fetchCtx); err != nil {
			return tg.InputDocumentFileLocation{}, err
		}

		loc, fetchErr := getLocation(fetchCtx, c.client, c.channelId, c.partId)
		if fetchErr != nil {
			if !errors.Is(fetchErr, context.Canceled) {
				_ = c.setCachedLocationFailure(fetchCtx, fetchErr)
			}
			return tg.InputDocumentFileLocation{}, fetchErr
		}

		_ = c.cache.Set(fetchCtx, c.key, loc, locationCacheTTL)
		return *loc, nil
	})

	select {
	case <-ctx.Done():
		return tg.InputDocumentFileLocation{}, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return tg.InputDocumentFileLocation{}, result.Err
		}
		typed, ok := result.Val.(tg.InputDocumentFileLocation)
		if !ok {
			return tg.InputDocumentFileLocation{}, fmt.Errorf("location fetch type mismatch for key %s", c.key)
		}
		return typed, nil
	}
}

func detachedDeadlineContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.Background(), func() {}
	}

	base := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(base, deadline)
	}
	return base, func() {}
}

func (c *chunkSource) locationFailureKey() string {
	return cache.Key(c.key, "neg")
}

func (c *chunkSource) getCachedLocationFailure(ctx context.Context) error {
	var failure locationLookupFailure
	if err := c.cache.Get(ctx, c.locationFailureKey(), &failure); err != nil {
		return nil
	}
	if failure.Message == "" {
		return errors.New("location unavailable")
	}
	switch {
	case failure.DeadlineExceeded:
		return fmt.Errorf("%s: %w", failure.Message, context.DeadlineExceeded)
	case failure.Canceled:
		return fmt.Errorf("%s: %w", failure.Message, context.Canceled)
	case failure.ChunkTimeoutMarker:
		return fmt.Errorf("%s: %w", failure.Message, ErrChunkTimeout)
	default:
		return errors.New(failure.Message)
	}
}

func (c *chunkSource) setCachedLocationFailure(ctx context.Context, err error) error {
	return c.cache.Set(ctx, c.locationFailureKey(), &locationLookupFailure{
		Message:            err.Error(),
		DeadlineExceeded:   errors.Is(err, context.DeadlineExceeded),
		Canceled:           errors.Is(err, context.Canceled),
		ChunkTimeoutMarker: errors.Is(err, ErrChunkTimeout),
	}, locationNegativeCacheTTL)
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
		// Return current buffer to pool if exists
		if r.cur != nil && r.cur.buf != nil {
			bytebufferpool.Put(r.cur.buf)
			r.cur.buf = nil
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
		// Return buffer to pool when fully consumed
		if r.cur != nil && r.cur.buf != nil {
			bytebufferpool.Put(r.cur.buf)
			r.cur.buf = nil
		}
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
	g, ctx := errgroup.WithContext(r.ctx)
	g.SetLimit(r.concurrency)

	batchSize := min(r.concurrency, r.totalParts-r.currentPart)
	buffers := make([]*buffer, batchSize)

	for i := 0; i < batchSize; i++ {
		part := r.currentPart + i
		offset := r.offset + int64(i)*r.chunkSize
		bufferIdx := i
		g.Go(func() error {
			chunk, err := r.fetchChunkWithRetry(ctx, offset, part)
			if err != nil {
				return err
			}

			buf := bytebufferpool.Get()
			_, _ = buf.Write(chunk)

			if r.totalParts == 1 {
				buf.B = buf.B[r.leftCut:r.rightCut]
			} else if part == 0 {
				buf.B = buf.B[r.leftCut:]
			} else if part+1 == r.totalParts {
				buf.B = buf.B[:r.rightCut]
			}

			buffers[bufferIdx] = &buffer{buf: buf}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		for _, buf := range buffers {
			if buf != nil && buf.buf != nil {
				bytebufferpool.Put(buf.buf)
			}
		}
		if !errors.Is(err, context.Canceled) {
			if r.onChunkFail != nil {
				r.onChunkFail(err)
			}
			r.logger.Error("stream.chunk_failed", zap.Error(err), zap.Int("part", r.currentPart), zap.Int("total_parts", r.totalParts))
		}

		return err
	}

	for _, buf := range buffers {
		if buf == nil {
			break
		}
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
