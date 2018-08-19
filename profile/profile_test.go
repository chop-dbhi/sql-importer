package profile

import (
	"testing"
	"time"
)

func TestProfilerRecord(t *testing.T) {
	tests := map[string]struct {
		Raw  string
		Type ValueType
		Val  interface{}
	}{
		"string": {
			"bar",
			StringType,
			"bar",
		},
		"int": {
			"10",
			IntType,
			int64(10),
		},
		"float": {
			"1.20",
			FloatType,
			float64(1.20),
		},
		"bool": {
			"true",
			BoolType,
			true,
		},
		"date-1": {
			"2014-02-01",
			DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-2": {
			"02/01/2014",
			DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-3": {
			"02/01/14",
			DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-4": {
			"2/1/14",
			DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"datetime": {
			"2014-02-01 10:00:00",
			DateTimeType,
			time.Date(2014, time.February, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	p := NewProfiler(nil)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			p.Record("test", test.Raw)
		})
	}
}
