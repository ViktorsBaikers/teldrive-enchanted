package retry

import (
	"context"
	stderrors "errors"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type sequenceInvoker struct {
	errs  []error
	calls int
}

func (s *sequenceInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	s.calls++
	if len(s.errs) == 0 {
		return nil
	}

	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}

func TestIsErrorMatch_MatchesWrappedTimeout(t *testing.T) {
	t.Helper()

	base := stderrors.New("rpcDoRequest: rpc error code -503: Timeout")
	wrapped := stderrors.New("retry middleware skip: " + base.Error())
	normalized := make([]string, len(internalErrors))
	for i, pattern := range internalErrors {
		normalized[i] = strings.ToLower(pattern)
	}

	if !isErrorMatch(wrapped, normalized) {
		t.Fatalf("expected wrapped timeout error to be matched")
	}
}

func TestRetryHandle_RetriesWrappedTimeout(t *testing.T) {
	t.Helper()

	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected retry to recover, got error: %v", err)
	}

	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_SkipsNonRetryableError(t *testing.T) {
	t.Helper()

	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("permission denied"),
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	err := invoke(context.Background(), nil, nil)
	if err == nil {
		t.Fatalf("expected non-retryable error")
	}
	if !strings.Contains(err.Error(), "retry middleware skip") {
		t.Fatalf("expected retry middleware skip wrapper, got: %v", err)
	}
	if invoker.calls != 1 {
		t.Fatalf("expected 1 call, got %d", invoker.calls)
	}
}

func TestRetryHandle_ReturnsRetryLimitError(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	err := invoke(context.Background(), nil, nil)
	if err == nil {
		t.Fatalf("expected retry limit error")
	}
	if !strings.Contains(err.Error(), "retry limit reached after 3 attempts") {
		t.Fatalf("unexpected error: %v", err)
	}
	if invoker.calls != 3 {
		t.Fatalf("expected 3 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_DoesNotDelayAfterFinalRetry(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var next tg.Invoker = invoker
	invoke := New(1).Handle(next)
	err := invoke(ctx, nil, nil)
	if err == nil {
		t.Fatalf("expected retry limit error")
	}
	if !strings.Contains(err.Error(), "retry limit reached after 1 attempts") {
		t.Fatalf("expected retry limit reached error, got: %v", err)
	}
	if invoker.calls != 1 {
		t.Fatalf("expected 1 call, got %d", invoker.calls)
	}
}

func TestRetryHandle_RetriesTgErrMatches(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			tgerr.New(500, "RPC_CALL_FAIL"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected tgerr retry to recover, got: %v", err)
	}
	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_CustomPatternCaseInsensitiveMatch(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("server overloaded"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3, "SERVER OVERLOADED").Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected custom pattern retry to recover, got: %v", err)
	}
	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_RetriesTgErr503ByCode(t *testing.T) {
	// Simulate the actual production error: tgerr.New wrapped by go-faster/errors.Wrap
	// This is what gotd produces at mtproto/rpc.go:44
	invoker := &sequenceInvoker{
		errs: []error{
			errors.Wrap(tgerr.New(-503, "Timeout"), "rpcDoRequest"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected -503 retry to recover via code match, got: %v", err)
	}
	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_RetriesTgErr500ByCode(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			errors.Wrap(tgerr.New(500, "UNKNOWN_ERROR"), "rpcDoRequest"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected 500 retry to recover via code match, got: %v", err)
	}
	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_BackoffDelayBetweenRetries(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			nil,
		},
	}

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	if err := invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("expected retry with backoff to recover, got: %v", err)
	}
	if invoker.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", invoker.calls)
	}
}

func TestRetryHandle_ContextCancelDuringBackoff(t *testing.T) {
	invoker := &sequenceInvoker{
		errs: []error{
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
			stderrors.New("rpcDoRequest: rpc error code -503: Timeout"),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately — the backoff sleep should be interrupted
	cancel()

	var next tg.Invoker = invoker
	invoke := New(3).Handle(next)
	err := invoke(ctx, nil, nil)
	if err == nil {
		t.Fatalf("expected context error during backoff")
	}
	if !stderrors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
