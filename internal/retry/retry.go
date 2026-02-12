package retry

import (
	"context"
	"fmt"
	"strings"

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

func (r retry) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		retries := 0

		for retries < r.max {
			if err := next.Invoke(ctx, input, output); err != nil {
				if tgerr.Is(err, r.errors...) || isErrorMatch(err, r.matchPatterns) {
					retries++
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
