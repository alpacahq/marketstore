package replication

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
)

// RetryableError is a custom error to retry the logic when returned
type RetryableError string

func (re RetryableError) Error() string {
	return string(re)
}
func (re RetryableError) Is(err error) bool {
	return err == RetryableError("")
}

type retryer struct {
	retryFunc    func(ctx context.Context) error
	interval     time.Duration
	backoffCoeff int
}

func NewRetryer(retryFunc func(ctx context.Context) error, interval time.Duration, backoffCoeff int) *retryer {
	return &retryer{
		retryFunc:    retryFunc,
		interval:     interval,
		backoffCoeff: backoffCoeff,
	}
}

// Run tries the retryer until it succeeds, it returns unretriable error, or the context is canceled.
func (r *retryer) Run(ctx context.Context) error {
	cnt := -1
	for {
		cnt++
		select {
		case <-ctx.Done():
			return errors.New("context canceled")
		default:
			err := r.retryFunc(ctx)
			// success
			if err == nil {
				return nil
			}

			if errors.Is(err, RetryableError("")) {
				// retryable error. continue
				interval := retryInterval(r.interval, r.backoffCoeff, cnt)
				log.Warn("caught a retryable error. It will be retried after an interval:" +
					strconv.FormatInt(interval.Milliseconds(), 10) + "[ms], err=" + err.Error())
				time.Sleep(interval)
				continue
			} else {
				// not retryable error, give up.
				log.Warn("caught a non-retryable error:" + err.Error())
				return err
			}
		}
	}
}

func retryInterval(interval time.Duration, backoffCoeff, retryCount int) time.Duration {
	coeff := math.Pow(float64(backoffCoeff), float64(retryCount))
	intervalMilliSec := float64(interval.Milliseconds())
	return time.Duration(intervalMilliSec*coeff) * time.Millisecond
}
