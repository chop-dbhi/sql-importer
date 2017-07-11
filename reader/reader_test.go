package reader

import (
	"bytes"
	"testing"
)

func TestUniversalReader(t *testing.T) {
	s := "\xef\xbb\xbfhello world!\r"

	r := bytes.NewBufferString(s)
	ur := &UniversalReader{r}

	buf := make([]byte, 20)
	n, err := ur.Read(buf)

	if err != nil {
		t.Fatalf("problem reading: %s", err)
	}

	if cap(buf) != 20 {
		t.Fatalf("expected 20 cap, got %d", cap(buf))
	}

	if len(s)-3 != n {
		t.Errorf("expected %d bytes, got %d", len(s)-3, n)
	}

	exp := "hello world!\n"

	if string(buf[:n]) != exp {
		t.Errorf("expected '%v', got '%v'", exp, string(buf[:n]))
	}
}
