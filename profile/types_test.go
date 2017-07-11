package profile

import "testing"

func assertType(t *testing.T, e, a ValueType) {
	if e != a {
		t.Errorf("expected %s, got %s", e, a)
	}
}

func TestGeneralizeType(t *testing.T) {
	assertType(t, GeneralizeType(IntType, FloatType), FloatType)
	assertType(t, GeneralizeType(IntType, BoolType), IntType)
	assertType(t, GeneralizeType(StringType, BoolType), StringType)
	assertType(t, GeneralizeType(DateTimeType, DateType), DateTimeType)
}
