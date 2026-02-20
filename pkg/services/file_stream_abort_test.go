package services

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/config"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/ViktorsBaikers/teldrive/pkg/types"
)

type failingResponseWriter struct {
	header http.Header
	err    error
}

func (w *failingResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failingResponseWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func (w *failingResponseWriter) WriteHeader(int) {}

func TestStreamWithTGReader_ClientDisconnectReturnsStreamAbandoned(t *testing.T) {
	svc := newStreamTestService()

	origFetch := fetchPartsForStream
	origNewReader := newReaderForStream
	defer func() {
		fetchPartsForStream = origFetch
		newReaderForStream = origNewReader
	}()

	fetchPartsForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, string, string) ([]types.Part, error) {
		return []types.Part{{ID: 1, Size: 3}}, nil
	}
	newReaderForStream = func(context.Context, *tg.Client, cache.Cacher, *models.File, []types.Part, int64, int64, *config.TGConfig, string, func(error)) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("abc"))), nil
	}

	rw := &failingResponseWriter{err: syscall.ECONNRESET}
	err := svc.streamWithTGReader(
		context.Background(),
		rw,
		zap.NewNop(),
		nil,
		&models.File{ID: "file-abort"},
		0, 2, 3,
		"botAbort",
		nil,
	)

	if err == nil {
		t.Fatal("expected error from streamWithTGReader")
	}
	if !stderrors.Is(err, ErrorStreamAbandoned) {
		t.Fatalf("expected ErrorStreamAbandoned, got %v", err)
	}
	if !stderrors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("expected syscall.ECONNRESET in error chain, got %v", err)
	}
}

func TestIsStreamClientDisconnect(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "context_canceled", err: context.Canceled, want: true},
		{name: "pipe", err: syscall.EPIPE, want: true},
		{name: "connreset", err: syscall.ECONNRESET, want: true},
		{name: "string_broken_pipe", err: stderrors.New("write: broken pipe"), want: true},
		{name: "string_connreset", err: stderrors.New("write: connection reset by peer"), want: true},
		{name: "wrapped_syscall", err: fmt.Errorf("callback: %w", syscall.ECONNRESET), want: true},
		{name: "other", err: stderrors.New("boom"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStreamClientDisconnect(tt.err); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
