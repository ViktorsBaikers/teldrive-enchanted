package reader

import (
	"context"
	stderrors "errors"
	"sort"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

type recordingChunkSource struct {
	mu        sync.Mutex
	chunkSize int64
	payload   map[int64][]byte
	errs      map[int64][]error
	calls     []int64
}

func (s *recordingChunkSource) Chunk(_ context.Context, offset int64, _ int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, offset)
	if seq, ok := s.errs[offset]; ok && len(seq) > 0 {
		err := seq[0]
		s.errs[offset] = seq[1:]
		if err != nil {
			return nil, err
		}
	}

	if p, ok := s.payload[offset]; ok {
		cp := make([]byte, len(p))
		copy(cp, p)
		return cp, nil
	}

	return []byte{0}, nil
}

func (s *recordingChunkSource) ChunkSize(_, _ int64) int64 {
	return s.chunkSize
}

func (s *recordingChunkSource) callCount(offset int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for _, call := range s.calls {
		if call == offset {
			total++
		}
	}
	return total
}

func TestFillBatch_UsesCorrectPartOffsets(t *testing.T) {
	chunkSize := int64(4)
	src := &recordingChunkSource{
		chunkSize: chunkSize,
		payload: map[int64][]byte{
			0: []byte("aaaa"),
			4: []byte("bbbb"),
			8: []byte("cccc"),
		},
		errs: map[int64][]error{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      0,
		chunkSize:   chunkSize,
		bufferChan:  make(chan *buffer, 3),
		concurrency: 3,
		leftCut:     0,
		rightCut:    chunkSize,
		totalParts:  3,
		currentPart: 0,
		chunkSrc:    src,
		timeout:     time.Second,
		logger:      zap.NewNop(),
	}

	if err := r.fillBatch(); err != nil {
		t.Fatalf("fillBatch failed: %v", err)
	}

	got := append([]int64(nil), src.calls...)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []int64{0, 4, 8}
	if len(got) != len(want) {
		t.Fatalf("unexpected calls len: got=%d want=%d calls=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected offset[%d]: got=%d want=%d calls=%v", i, got[i], want[i], got)
		}
	}

	if r.currentPart != 3 {
		t.Fatalf("unexpected currentPart: got=%d want=3", r.currentPart)
	}
	if r.offset != 12 {
		t.Fatalf("unexpected offset: got=%d want=12", r.offset)
	}
}

func TestFillBatch_CallsOnChunkFailForNonCanceledErrors(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callbackCalls int
	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      0,
		chunkSize:   4,
		bufferChan:  make(chan *buffer, 1),
		concurrency: 1,
		leftCut:     0,
		rightCut:    4,
		totalParts:  1,
		currentPart: 0,
		chunkSrc:    src,
		timeout:     100 * time.Millisecond,
		logger:      zap.NewNop(),
		onChunkFail: func(error) { callbackCalls++ },
	}

	if err := r.fillBatch(); err == nil {
		t.Fatalf("expected fillBatch to fail")
	}
	if callbackCalls != 1 {
		t.Fatalf("expected onChunkFail to be called once, got %d", callbackCalls)
	}
}

func TestFillBatch_DoesNotCallOnChunkFailForCanceledContext(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				context.Canceled,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callbackCalls int
	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      0,
		chunkSize:   4,
		bufferChan:  make(chan *buffer, 1),
		concurrency: 1,
		leftCut:     0,
		rightCut:    4,
		totalParts:  1,
		currentPart: 0,
		chunkSrc:    src,
		timeout:     100 * time.Millisecond,
		logger:      zap.NewNop(),
		onChunkFail: func(error) { callbackCalls++ },
	}

	if err := r.fillBatch(); !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("expected onChunkFail not to be called, got %d", callbackCalls)
	}
}

func TestFillBatch_AdvancesByRemainingParts(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			8: []byte("cccc"),
		},
		errs: map[int64][]error{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      8,
		chunkSize:   4,
		bufferChan:  make(chan *buffer, 1),
		concurrency: 3,
		leftCut:     0,
		rightCut:    4,
		totalParts:  3,
		currentPart: 2,
		chunkSrc:    src,
		timeout:     time.Second,
		logger:      zap.NewNop(),
	}

	if err := r.fillBatch(); err != nil {
		t.Fatalf("fillBatch failed: %v", err)
	}

	if got := src.callCount(8); got != 1 {
		t.Fatalf("expected exactly one fetch for remaining part, got %d", got)
	}
	if r.currentPart != 3 {
		t.Fatalf("expected currentPart=3, got %d", r.currentPart)
	}
	if r.offset != 12 {
		t.Fatalf("expected offset=12, got %d", r.offset)
	}
}

func TestFetchChunkWithRetry_RetriesTransientErrors(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				nil,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:       ctx,
		cancel:    cancel,
		chunkSize: 4,
		chunkSrc:  src,
		timeout:   time.Second,
		logger:    zap.NewNop(),
	}

	chunk, err := r.fetchChunkWithRetry(ctx, 0, 0)
	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}
	if string(chunk) != "abcd" {
		t.Fatalf("unexpected chunk payload: %q", string(chunk))
	}
	if got := src.callCount(0); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
}

func TestFetchChunkWithRetry_DoesNotRetryNonTransientErrors(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				stderrors.New("retry middleware skip: permission denied"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:       ctx,
		cancel:    cancel,
		chunkSize: 4,
		chunkSrc:  src,
		timeout:   time.Second,
		logger:    zap.NewNop(),
	}

	_, err := r.fetchChunkWithRetry(ctx, 0, 0)
	if err == nil {
		t.Fatalf("expected non-transient error")
	}
	if got := src.callCount(0); got != 1 {
		t.Fatalf("expected a single attempt, got %d", got)
	}
}

func TestFetchChunkWithRetry_StopsAfterRetryBudget(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:       ctx,
		cancel:    cancel,
		chunkSize: 4,
		chunkSrc:  src,
		timeout:   time.Second,
		logger:    zap.NewNop(),
	}

	_, err := r.fetchChunkWithRetry(ctx, 0, 0)
	if err == nil {
		t.Fatalf("expected retry budget exhaustion error")
	}

	if got := src.callCount(0); got != maxChunkRetries+1 {
		t.Fatalf("expected %d attempts, got %d", maxChunkRetries+1, got)
	}
}

func TestFetchChunkWithRetry_MapsDeadlineExceededToChunkTimeout(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				context.DeadlineExceeded,
				context.DeadlineExceeded,
				context.DeadlineExceeded,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &tgMultiReader{
		ctx:       ctx,
		cancel:    cancel,
		chunkSize: 4,
		chunkSrc:  src,
		timeout:   time.Second,
		logger:    zap.NewNop(),
	}

	_, err := r.fetchChunkWithRetry(ctx, 0, 0)
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !stderrors.Is(err, ErrChunkTimeout) {
		t.Fatalf("expected ErrChunkTimeout, got: %v", err)
	}
}

func TestFetchChunkWithRetry_StopsWhenContextCanceledDuringBackoff(t *testing.T) {
	src := &recordingChunkSource{
		chunkSize: 4,
		payload: map[int64][]byte{
			0: []byte("abcd"),
		},
		errs: map[int64][]error{
			0: {
				stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &tgMultiReader{
		ctx:       ctx,
		cancel:    cancel,
		chunkSize: 4,
		chunkSrc:  src,
		timeout:   time.Second,
		logger:    zap.NewNop(),
	}

	_, err := r.fetchChunkWithRetry(ctx, 0, 0)
	if !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if got := src.callCount(0); got != 1 {
		t.Fatalf("expected a single call before cancellation, got %d", got)
	}
}
