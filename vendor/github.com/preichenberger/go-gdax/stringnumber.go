package gdax

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type StringNumber string

func (s *StringNumber) UnmarshalJSON(data []byte) error {
	var v interface{}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	switch v := v.(type) {
	case float64:
		*s = StringNumber(fmt.Sprintf("%f", v))
	case int:
		*s = StringNumber(strconv.Itoa(v))
	case string:
		*s = StringNumber(v)
	default:
		return errors.New("Not an int or string")
	}

	return nil
}
