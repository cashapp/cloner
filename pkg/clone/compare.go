package clone

import (
	"bytes"
	"fmt"
	"reflect"
)

func genericCompareKeys(a []interface{}, b []interface{}) int {
	for i := range a {
		compare := genericCompare(a[i], b[i])
		if compare != 0 {
			return compare
		}
	}
	return 0
}

func genericCompare(a interface{}, b interface{}) int {
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
	case string:
		coerced, err := coerceString(b)
		if err != nil {
			panic(err)
		}
		if a == coerced {
			return 0
		} else if a < coerced {
			return -1
		} else {
			return 1
		}
	case []byte:
		coerced, err := coerceRaw(b)
		if err != nil {
			panic(err)
		}
		return bytes.Compare(a, coerced)
	default:
		panic(fmt.Sprintf("type combination %v -> %v not supported yet: source=%v target=%v",
			aType, bType, a, b))
	}
}

// compareUint64 coerces both numbers to its widest uint form (uint64) and compares them
func compareUint64(a interface{}, b interface{}) int {
	coercedA, err := coerceUint64(a)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	if coercedA == coercedB {
		return 0
	} else if coercedA < coercedB {
		return -1
	} else {
		return 1
	}
}

// compareFloat64 coerces both numbers to its widest float form (float64) and compares them
func compareFloat64(a interface{}, b interface{}) int {
	coercedA, err := coerceFloat64(a)
	if err != nil {
		panic(err)
	}
	coercedB, err := coerceFloat64(b)
	if err != nil {
		panic(err)
	}
	if coercedA == coercedB {
		return 0
	} else if coercedA < coercedB {
		return -1
	} else {
		return 1
	}
}

// compareInt64 coerces both numbers to its widest int form (int64) and compares them
func compareInt64(a interface{}, b interface{}) int {
	coercedA, err := coerceInt64(a)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	if coercedA == coercedB {
		return 0
	} else if coercedA < coercedB {
		return -1
	} else {
		return 1
	}
}
