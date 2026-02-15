package retry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

var internalErrors = []string{
	"Timedout",
	"Timeout",
	"No workers running",
	"RPC_CALL_FAIL",
	"RPC_MCGET_FAIL",
	"WORKER_BUSY_TOO_LONG_RETRY",
	"memory limit exit",
	"connection dead",
	"engine was closed",
	"STORAGE_CHOOSE_VOLUME_FAILED",
}

// retryableCodes are RPC error codes that should always be retried,
// regardless of how the error type string is parsed.
// -503 = server timeout (DC overloaded), 500 = internal server error.
var retryableCodes = []int{-503, 500}

type retry struct {
	max           int
	errors        []string
	matchPatterns []string
}

func isErrorMatch(err error, normalizedPatterns []string) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	for _, pattern := range normalizedPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}

func isRetryable(err error, types []string, patterns []string) bool {
	if tgerr.Is(err, types...) {
		return true
	}
	if tgerr.IsCode(err, retryableCodes...) {
		return true
	}
	return isErrorMatch(err, patterns)
}

func (r retry) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		retries := 0

		for retries < r.max {
			if err := next.Invoke(ctx, input, output); err != nil {
				if isRetryable(err, r.errors, r.matchPatterns) {
					retries++
					if retries >= r.max {
						if err := ctx.Err(); err != nil {
							return err
						}
						break
					}
					select {
					case <-time.After(time.Duration(retries) * 500 * time.Millisecond):
					case <-ctx.Done():
						return ctx.Err()
					}
					continue
				}
				return errors.Wrap(err, "retry middleware skip")
			}

			return nil
		}

		return fmt.Errorf("retry limit reached after %d attempts", r.max)
	}
}

func New(max int, retryErrors ...string) telegram.Middleware {
	patterns := append([]string{}, retryErrors...)
	patterns = append(patterns, internalErrors...)

	matchPatterns := make([]string, len(patterns))
	for i, p := range patterns {
		matchPatterns[i] = strings.ToLower(p)
	}

	return retry{
		max:           max,
		errors:        patterns,
		matchPatterns: matchPatterns,
	}
}
