package profile

import (
	"encoding/json"
	"strings"
)

const (
	UnknownType ValueType = iota
	NullType
	StringType
	BinaryType
	IntType
	FloatType
	BoolType
	DateType
	DateTimeType
	ObjectType
)

// ValueType is a type of value.
type ValueType uint8

func (v ValueType) String() string {
	switch v {
	case NullType:
		return "null"
	case StringType:
		return "string"
	case BinaryType:
		return "binary"
	case IntType:
		return "integer"
	case FloatType:
		return "float"
	case BoolType:
		return "boolean"
	case DateType:
		return "date"
	case DateTimeType:
		return "datetime"
	case ObjectType:
		return "object"
	}

	return ""
}

func (v ValueType) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v *ValueType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	var t ValueType

	switch strings.ToLower(s) {
	case "string":
		t = StringType
	case "null":
		t = NullType
	case "binary":
		t = BinaryType
	case "integer":
		t = IntType
	case "float":
		t = FloatType
	case "boolean":
		t = BoolType
	case "date":
		t = DateType
	case "datetime":
		t = DateTimeType
	case "object":
		t = ObjectType
	}

	*v = t

	return nil
}

var typeGeneralizationMap = map[[2]ValueType]ValueType{
	{BoolType, IntType}:      IntType,
	{IntType, FloatType}:     FloatType,
	{BoolType, FloatType}:    FloatType,
	{DateTimeType, DateType}: DateTimeType,
}

// GeneralizeType takes two types and returns the more general
// type of the two with string being the most general if both
// are not null types.
func GeneralizeType(t1, t2 ValueType) ValueType {
	// Same type.
	if t1 == t2 {
		return t1
	}

	if t1 == NullType {
		return t2
	}

	if t2 == NullType {
		return t1
	}

	key := [2]ValueType{t1, t2}

	t, ok := typeGeneralizationMap[key]
	if ok {
		return t
	}

	// Swap order.
	key[0], key[1] = key[1], key[0]

	t, ok = typeGeneralizationMap[key]
	if ok {
		return t
	}

	// Everything can be generalized to a string.
	return StringType
}
