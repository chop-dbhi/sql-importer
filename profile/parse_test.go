package profile

import "testing"

func BenchmarkParseDateValid(b *testing.B) {
	s := "1998-10-01"
	for i := 0; i < b.N; i++ {
		ParseDate(s)
	}
}

func BenchmarkParseDateInvalid(b *testing.B) {
	s := "not a date"
	for i := 0; i < b.N; i++ {
		ParseDate(s)
	}
}

func BenchmarkParseDateTimeValid(b *testing.B) {
	s := "1998-10-01 01:32:10"
	for i := 0; i < b.N; i++ {
		ParseDateTime(s)
	}
}

func BenchmarkParseDateTimeInvalid(b *testing.B) {
	s := "not a date time"
	for i := 0; i < b.N; i++ {
		ParseDateTime(s)
	}
}

func BenchmarkParseFloatValid(b *testing.B) {
	s := "32.10219"
	for i := 0; i < b.N; i++ {
		ParseFloat(s)
	}
}

func BenchmarkParseFloatInvalid(b *testing.B) {
	s := "not a number"
	for i := 0; i < b.N; i++ {
		ParseFloat(s)
	}
}

func BenchmarkParseIntValid(b *testing.B) {
	s := "3210219"
	for i := 0; i < b.N; i++ {
		ParseInt(s)
	}
}

func BenchmarkParseIntInvalid(b *testing.B) {
	s := "not a number"
	for i := 0; i < b.N; i++ {
		ParseInt(s)
	}
}

func BenchmarkParseBoolValid(b *testing.B) {
	s := "TRUE"
	for i := 0; i < b.N; i++ {
		ParseBool(s)
	}
}

func BenchmarkParseBoolInvalid(b *testing.B) {
	s := "not a bool"
	for i := 0; i < b.N; i++ {
		ParseBool(s)
	}
}
