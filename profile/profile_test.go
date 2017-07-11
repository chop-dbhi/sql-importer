package profile

import (
	"reflect"
	"testing"
	"time"

	"github.com/chop-dbhi/revue"
)

func TestProfilerRecord(t *testing.T) {
	tests := map[string]struct {
		Raw  string
		Type revue.ValueType
		Val  interface{}
	}{
		"string": {
			"bar",
			revue.StringType,
			"bar",
		},
		"int": {
			"10",
			revue.IntType,
			int64(10),
		},
		"float": {
			"1.20",
			revue.FloatType,
			float64(1.20),
		},
		"bool": {
			"true",
			revue.BoolType,
			true,
		},
		"date-1": {
			"2014-02-01",
			revue.DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-2": {
			"02/01/2014",
			revue.DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-3": {
			"02/01/14",
			revue.DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"date-4": {
			"2/1/14",
			revue.DateType,
			time.Date(2014, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		"datetime": {
			"2014-02-01 10:00:00",
			revue.DateTimeType,
			time.Date(2014, time.February, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	p := NewProfiler(nil)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			typ, val := p.Record("test", test.Raw)

			if typ != test.Type {
				t.Errorf("expected %s, got %s", test.Type, typ)
			}

			if !reflect.DeepEqual(val, test.Val) {
				t.Errorf("expected %v, got %v", test.Val, val)
			}
		})
	}
}
