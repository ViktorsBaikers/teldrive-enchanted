package reader

import (
	"context"
	stderrors "errors"
	"io"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestFillBatch_ReturnsUnexpectedEOFOnEmptyFirstChunk(t *testing.T) {
	const chunkSize = int64(64 * 1024)

	src := &recordingChunkSource{
		chunkSize: chunkSize,
		payload: map[int64][]byte{
			0: {},
		},
		errs: map[int64][]error{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callbackCalls int
	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      0,
		chunkSize:   chunkSize,
		bufferChan:  make(chan *buffer, 1),
		concurrency: 1,
		leftCut:     32 * 1024,
		rightCut:    chunkSize,
		totalParts:  2,
		currentPart: 0,
		chunkSrc:    src,
		timeout:     time.Second,
		logger:      zap.NewNop(),
		onChunkFail: func(error) { callbackCalls++ },
	}

	err := r.fillBatch()
	if err == nil {
		t.Fatal("expected fillBatch to fail")
	}
	if !stderrors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("expected onChunkFail to be called once, got %d", callbackCalls)
	}
}

func TestFillBatch_ReturnsUnexpectedEOFOnShortLastChunk(t *testing.T) {
	const chunkSize = int64(8)

	src := &recordingChunkSource{
		chunkSize: chunkSize,
		payload: map[int64][]byte{
			8: []byte("xx"), // too short for rightCut below
		},
		errs: map[int64][]error{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callbackCalls int
	r := &tgMultiReader{
		ctx:         ctx,
		cancel:      cancel,
		offset:      8,
		chunkSize:   chunkSize,
		bufferChan:  make(chan *buffer, 1),
		concurrency: 1,
		leftCut:     0,
		rightCut:    6,
		totalParts:  2,
		currentPart: 1,
		chunkSrc:    src,
		timeout:     time.Second,
		logger:      zap.NewNop(),
		onChunkFail: func(error) { callbackCalls++ },
	}

	err := r.fillBatch()
	if err == nil {
		t.Fatal("expected fillBatch to fail")
	}
	if !stderrors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
	if callbackCalls != 1 {
		t.Fatalf("expected onChunkFail to be called once, got %d", callbackCalls)
	}
}
