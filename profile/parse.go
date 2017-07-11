package profile

import (
	"strconv"
	"strings"
	"time"
)

var (
	dateFormats = []string{
		"2006-01-02",
		"01-02-2006",
		"01-02-06",
		"01/02/2006",
		"01/02/06",
		"1/2/06",
	}

	dateTimeFormats = []string{
		"2006-01-02 15:04",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05Z07:00",
	}
)

func ParseBool(s string) (bool, bool) {
	s = strings.TrimSpace(s)

	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, false
	}

	return b, true
}

func ParseDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)

	for _, layout := range dateFormats {
		if v, err := time.Parse(layout, s); err == nil {
			return v, true
		}
	}

	return time.Time{}, false
}

func ParseDateTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)

	for _, layout := range dateTimeFormats {
		if v, err := time.Parse(layout, s); err == nil {
			return v, true
		}
	}

	return time.Time{}, false
}

func ParseFloat(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func ParseInt(s string) (int64, bool) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}
