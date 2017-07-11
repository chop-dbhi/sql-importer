package csv

import (
	"bytes"
	"strings"
	"testing"

	"github.com/chop-dbhi/revue"
)

func TestProfile(t *testing.T) {
	b := bytes.NewBufferString(`name,color,dob
John,Blue,03/11/2013
Jane,Red,2008-2-24
Joe,,2010-02-11
`)

	var w bytes.Buffer

	p, err := Profile(nil, b, &w)
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(p.Fields))
	}

	f := p.Fields["name"]
	if len(f.ValueCounts) != 3 {
		t.Errorf("expected 3 name values, got %d", len(f.ValueCounts))
	}

	if p.Fields["dob"].Type != revue.DateType {
		t.Errorf("expected date type, got %s", p.Fields["dob"].Type)
	}

	res := `name,color,dob
John,Blue,2013-03-11
Jane,Red,2008-2-24
Joe,,2010-02-11
`

	if strings.TrimSpace(w.String()) != strings.TrimSpace(res) {
		t.Errorf("strings don't match")
	}
}
