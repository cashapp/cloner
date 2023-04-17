package clone

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

func genericCompareKeys(a []interface{}, b []interface{}) int {
	for i := range a {
		compare := genericCompareOrPanic(a[i], b[i])
		if compare != 0 {
			return compare
		}
	}
	return 0
}

func genericCompare(a interface{}, b interface{}) (int, error) {
	// Different database drivers interpret SQL types differently (it seems)
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)

	// If they do NOT have same type, we coerce the types to their widest type and then compare
	// We only support the combinations we've encountered in the wild here
	switch a := a.(type) {
	case int:
		return compareInt64(a, b)
	case int64:
		return compareInt64(a, b)
	case int32:
		return compareInt64(a, b)
	case uint:
		return compareUint64(a, b)
	case uint32:
		return compareUint64(a, b)
	case uint64:
		return compareUint64(a, b)
	case float32:
		return compareFloat64(a, b)
	case float64:
		return compareFloat64(a, b)
	case nil:
		if b == nil {
			return 0, nil
		} else {
			return -1, nil
		}
	case time.Time:
		return compareTime(a, b)
	case string:
		coerced, err := coerceString(b)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if a == coerced {
			return 0, nil
		} else if a < coerced {
			return -1, nil
		} else {
			return 1, nil
		}
	case []byte:
		coerced, err := coerceRaw(b)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return bytes.Compare(a, coerced), nil
	default:
		return 0, errors.Errorf("type combination %v to %v not supported yet: source=%v target=%v",
			aType, bType, a, b)
	}
}

func genericCompareOrPanic(a interface{}, b interface{}) int {
	result, err := genericCompare(a, b)
	if err != nil {
		panic(err)
	}
	return result
}

// compareUint64 coerces both numbers to its widest uint form (uint64) and compares them
func compareUint64(a interface{}, b interface{}) (int, error) {
	coercedA, err := coerceUint64(a)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	switch b := b.(type) {
	case int:
		if b < 0 {
			return compareInt64(a, b)
		}
	case int32:
		if b < 0 {
			return compareInt64(a, b)
		}
	case int64:
		if b < 0 {
			return compareInt64(a, b)
		}
	}
	coercedB, err := coerceUint64(b)
	if err != nil {
		return 0, err
	}
	if coercedA == coercedB {
		return 0, nil
	} else if coercedA < coercedB {
		return -1, nil
	} else {
		return 1, nil
	}
}

// compareFloat64 coerces both numbers to its widest float form (float64) and compares them
func compareFloat64(a interface{}, b interface{}) (int, error) {
	coercedA, err := coerceFloat64(a)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	coercedB, err := coerceFloat64(b)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if coercedA == coercedB {
		return 0, nil
	} else if coercedA < coercedB {
		return -1, nil
	} else {
		return 1, nil
	}
}

// compareTime coerces both numbers to its widest float form (float64) and compares them
func compareTime(a interface{}, b interface{}) (int, error) {
	coercedA, err := coerceTime(a)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	coercedB, err := coerceTime(b)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if coercedA.Equal(coercedB) {
		return 0, nil
	} else if coercedA.Before(coercedB) {
		return -1, nil
	} else {
		return 1, nil
	}
}

func coerceTime(value interface{}) (time.Time, error) {
	switch value := value.(type) {
	case time.Time:
		return value, nil
	default:
		return time.Time{}, errors.Errorf("can't (yet?) coerce %v to time.Time: %v", reflect.TypeOf(value), value)
	}
}

// compareInt64 coerces both numbers to its widest int form (int64) and compares them
func compareInt64(a interface{}, b interface{}) (int, error) {
	coercedA, err := coerceInt64(a)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if coercedA >= 0 {
		switch b := b.(type) {
		case uint:
			return compareUint64(a, b)
		case uint32:
			return compareUint64(a, b)
		case uint64:
			return compareUint64(a, b)
		}
	}
	coercedB, err := coerceInt64(b)
	if err != nil {
		return 0, err
	}
	if coercedA == coercedB {
		return 0, nil
	} else if coercedA < coercedB {
		return -1, nil
	} else {
		return 1, nil
	}
}

func coerceInt64(value interface{}) (int64, error) {
	switch value := value.(type) {
	case uint:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > math.MaxUint32 {
			return -1, errors.Errorf("value too large to convert to int64: %+v", value)
		}
		return int64(value), nil
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case []byte:
		// This means it was sent as a unicode encoded string
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, errors.Errorf("can't (yet?) coerce %v to int64: %v", reflect.TypeOf(value), value)
	}
}

func coerceUint64(value interface{}) (uint64, error) {
	switch value := value.(type) {
	case int:
		if value < 0 {
			return 0, errors.Errorf("can't coerce negative number to uint64: %+v", value)
		}
		return uint64(value), nil
	case int32:
		if value < 0 {
			return 0, errors.Errorf("can't coerce negative number to uint64: %+v", value)
		}
		return uint64(value), nil
	case int64:
		if value < 0 {
			return 0, errors.Errorf("can't coerce negative number to uint64: %+v", value)
		}
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case uint64:
		return value, nil
	default:
		return 0, errors.Errorf("can't (yet?) coerce %v to uint64: %v", reflect.TypeOf(value), value)
	}
}

func coerceFloat64(value interface{}) (float64, error) {
	switch value := value.(type) {
	case float32:
		return float64(value), nil
	default:
		return 0, errors.Errorf("can't (yet?) coerce %v to float64: %v", reflect.TypeOf(value), value)
	}
}

func coerceString(value interface{}) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case nil:
		return "", nil
	case int64:
		return fmt.Sprintf("%d", value), nil
	case time.Time:
		return value.Format(mysqlTimeFormat), nil
	default:
		return "", errors.Errorf("can't (yet?) coerce %v to string: %v", reflect.TypeOf(value), value)
	}
}

func coerceRaw(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	case int64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(value))
		return b, nil
	default:
		return nil, errors.Errorf("can't (yet?) coerce %v to []byte: %v", reflect.TypeOf(value), value)
	}
}

//nolint:deadcode
func coerceRawArray(vals []interface{}) ([][]byte, error) {
	var err error
	raw := make([][]byte, len(vals))
	for i, val := range vals {
		raw[i], err = coerceRaw(val)
		if err != nil {
			return raw, err
		}
	}
	return raw, err
}
