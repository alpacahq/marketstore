package aggtrigger

import (
	"github.com/alpacahq/marketstore/utils"
)

type timeframes []utils.Timeframe

func (tfs *timeframes) UpperBound() (tf *utils.Timeframe) {
	if tfs == nil {
		return nil
	}

	for _, t := range *tfs {
		if tf == nil {
			tf = &t
			continue
		}

		if t.Duration > tf.Duration {
			tf = &t
		}
	}

	return
}

func (tfs *timeframes) LowerBound() (tf *utils.Timeframe) {
	if tfs == nil {
		return nil
	}

	for _, t := range *tfs {
		if tf == nil {
			tf = &t
			continue
		}

		if t.Duration < tf.Duration {
			tf = &t
		}
	}

	return
}
