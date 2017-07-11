package json

import (
	"bytes"
	"testing"
)

func TestProfileJSON(t *testing.T) {
	b := bytes.NewBufferString(`[
		{"name": "John", "color": "Blue", "dob": "1985-03-10"},
		{"name": "Jane", "color": "Red"}
	]`)

	p, err := Profile(nil, b, "json")
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(p.Fields))
	}
}

func TestProfileLDJSON(t *testing.T) {
	b := bytes.NewBufferString(`
		{"name": "John", "color": "Blue", "dob": "1985-03-10"}
		{"name": "Jane", "color": "Red"}
		`)

	p, err := Profile(nil, b, "ldjson")
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(p.Fields))
	}
}
