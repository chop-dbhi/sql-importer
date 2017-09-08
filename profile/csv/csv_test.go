package csv

import (
	"bytes"
	"testing"

	"github.com/chop-dbhi/sql-importer/profile"
)

func TestProfiler(t *testing.T) {
	b := bytes.NewBufferString(`name,color,dob
John,Blue,03/11/2013
Jane,Red,2008-2-24
Joe,,2010-02-11
`)

	pr := NewProfiler(b)
	p, err := pr.Profile()
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(p.Fields))
	}

	if p.Fields["dob"].Type != profile.DateType {
		t.Errorf("expected date type, got %s", p.Fields["dob"].Type)
	}
}
