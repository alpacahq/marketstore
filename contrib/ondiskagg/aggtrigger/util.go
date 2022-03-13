package aggtrigger

import (
	"github.com/alpacahq/marketstore/v4/utils"
)

type timeframes []utils.Timeframe

func (tfs *timeframes) UpperBound() (tf *utils.Timeframe) {
	if tfs == nil {
		return nil
	}

	for _, t := range *tfs {
		t := t
		if tf == nil {
			tf = &t
			continue
		}

		if t.Duration > tf.Duration {
			tf = &t
		}
	}

	return tf
}

func (tfs *timeframes) LowerBound() (tf *utils.Timeframe) {
	if tfs == nil {
		return nil
	}

	for _, t := range *tfs {
		t := t
		if tf == nil {
			tf = &t
			continue
		}

		if t.Duration < tf.Duration {
			tf = &t
		}
	}

	return tf
}
