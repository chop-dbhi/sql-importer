package csv

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

func compareRows(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

func tableToCSV(t [][]string) []byte {
	buf := bytes.NewBuffer(nil)
	sep := []byte{','}
	nl := []byte{'\n'}

	for _, r := range t {
		for i, c := range r {
			if i != 0 {
				buf.Write(sep)
			}
			if c != "" {
				buf.WriteString(fmt.Sprintf(`"%s"`, c))
			}
		}

		buf.Write(nl)
	}

	return buf.Bytes()
}

func tableToToks(t [][]string) []string {
	var toks []string

	for _, r := range t {
		toks = append(toks, r...)
	}

	return toks
}

func TestCSVReader(t *testing.T) {
	table := [][]string{
		{"name", "gender", "state"},
		{"Joe", "M", "GA"},
		{"Sue", "F", "NJ"},
		{"Bob", "M", "NY"},
		{"Bill", "M", ""}, // trailing comma
	}

	buf := bytes.NewBuffer(tableToCSV(table))
	toks := tableToToks(table)

	cr := DefaultCSVReader(buf)

	var i, c, l int

	for i = 0; cr.Scan(); i++ {
		// Increment line and reset column every three tokens.
		if i%3 == 0 {
			l++
			c = 1
		} else {
			c++
		}

		if i == len(toks) {
			t.Errorf("scan exceeded %d tokens", i+1)
			break
		}

		tok := cr.Text()

		if tok != toks[i] {
			t.Errorf("line %d, column %d: expected %s, got %s", cr.LineNumber(), cr.ColumnNumber(), toks[i], tok)
		}

		if cr.LineNumber() != l {
			t.Errorf("expected line %d, got %d for %s", l, cr.LineNumber(), tok)
		}

		if cr.ColumnNumber() != c {
			t.Errorf("expected column %d, got %d for %s", c, cr.ColumnNumber(), tok)
		}
	}

	if err := cr.Err(); err != io.EOF {
		t.Errorf("unexpected error: %s", err)
	}

	if i != len(toks) {
		t.Errorf("expected %d, got %d", len(toks), i)
	}
}

func TestCSVScanLine(t *testing.T) {
	table := [][]string{
		{"name", "gender", "state"},
		{"Joe", "M", "GA"},
		{"Sue", "F", "NJ"},
		{"Bob", "M", "NY"},
		{"Bill", "M", ""},
	}

	buf := bytes.NewBuffer(tableToCSV(table))

	cr := DefaultCSVReader(buf)

	var (
		i   int
		err error
		row = make([]string, 3)
	)

	for {
		err = cr.ScanLine(row)

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Errorf("%d: unexpected error: %s", i, err)
		}

		if cr.LineNumber() != i+1 {
			t.Errorf("%d: got wrong line number %d", i, cr.LineNumber())
		}

		if !compareRows(table[i], row) {
			t.Errorf("%d: wrong row, got %v", row)
		}

		i++
	}

	if i != 5 {
		t.Errorf("scanned wrong number of lines %d", i)
	}
}

func TestCSVInput(t *testing.T) {
	rows := []string{
		`"name","gender",state`,
		`Joe,"M",GA`,
		`"Sue","""F""",NJ`,
		`Bob,M,NY`,
	}

	buf := bytes.NewBuffer([]byte(strings.Join(rows, "\n")))
	cr := DefaultCSVReader(buf)

	var (
		err error
		row = make([]string, 3)
	)

	for {
		err = cr.ScanLine(row)

		if err == io.EOF {
			break
		}

		if err != nil {
			t.Errorf("%d: unexpected error: %s", cr.LineNumber(), err)
		}
	}
}

func TestCSVScanLineBadInput(t *testing.T) {
	rows := []string{
		`"name", "gender",state`,
		`Joe,"M", "GA"`,
		`"Sue", "F", "NJ"`,
		`"Bob",M,NY"`,
	}

	buf := bytes.NewBuffer([]byte(strings.Join(rows, "\n")))
	cr := DefaultCSVReader(buf)

	var (
		i   int
		err error
		row = make([]string, 3)
	)

	for {
		err = cr.ScanLine(row)

		if err == io.EOF {
			break
		}

		if cr.Line() != rows[i] {
			t.Errorf("%d: bad line `%s`", i, cr.Line())
		}

		if err == nil {
			t.Errorf("%d: expected error", i)
		} else if cr.LineNumber() != i+1 {
			t.Errorf("%d: got wrong line number %d", i, cr.LineNumber())
		}

		i++
	}

	if i != 4 {
		t.Errorf("scanned wrong number of lines %d", i)
	}
}

func TestCSVReaderBadInput(t *testing.T) {
	rows := []string{
		`"name","gender", state`,
		`Joe,"M", "GA"`,
		`"Sue", "F", "NJ"`,
		`"Bob",M,NY"`,
	}

	expectedToks := []struct {
		Token  string
		Error  bool
		Line   int
		Column int
	}{
		{"name", false, 1, 1},
		{"gender", false, 1, 2},
		{" state", false, 1, 3},
		{"Joe", false, 2, 1},
		{"M", false, 2, 2},
		{` "GA"`, true, 2, 3},
		{"Sue", false, 3, 1},
		{` "F", "NJ"`, true, 3, 2},
		{"Bob", false, 4, 1},
		{"M", false, 4, 2},
		{`NY"`, true, 4, 3},
	}

	buf := bytes.NewBuffer([]byte(strings.Join(rows, "\n")))
	cr := DefaultCSVReader(buf)

	var (
		err error
		tok string
	)

	for i := 0; cr.Scan(); i++ {
		tok = cr.Text()
		exp := expectedToks[i]

		if cr.LineNumber() != exp.Line {
			t.Errorf("%d: expected line %d, got %d", i, exp.Line, cr.LineNumber())
		}

		if cr.ColumnNumber() != exp.Column {
			t.Errorf("%d: expected column %d, got %d", i, exp.Column, cr.ColumnNumber())
		}

		if exp.Token != tok {
			t.Errorf("%d: expected token `%s`, got `%s`", i, exp.Token, tok)
		}

		err = cr.Err()

		if err == nil && exp.Error {
			t.Errorf("%d: expected error", i)
		} else if err != nil && !exp.Error {
			t.Errorf("%d: unexpected error: %s", i, err)
		}
	}
}

func TestCSVExtraColumns(t *testing.T) {
	buf := bytes.NewBufferString("one,two,three,four")
	cr := DefaultCSVReader(buf)

	// 3 columns expected.
	toks := make([]string, 3)
	err := cr.ScanLine(toks)

	if err == nil {
		t.Errorf("Expected error")
	} else if err != csvErrExtraColumns {
		t.Errorf("Expected extra columns error, got %s instead", err)
	}
}

var line = `"3","\PCORI\VITAL\TOBACCO\SMOKING\","Smoked Tobacco","N","FAE",,,,"concept_cd","CONCEPT_DIMENSION","concept_path","T","like","\PCORI\VITAL\TOBACCO\SMOKING\","CDMv2","This field is new to v3.0. Indicator for any form of tobacco that is smoked.Per Meaningful Use guidance, smoking status includes any form of tobacco that is smoked, but not all tobacco use. ""Light smoker"" is interpreted to mean less than 10 cigarettes per day, or an equivalent (but less concretely defined) quantity of cigar or pipe smoke. ""Heavy smoker"" is interpreted to mean greater than 10 cigarettes per day or an equivalent (but less concretely defined) quantity of cigar or pipe smoke. ","@","2015-08-20 312:14:14.0","2015-08-20 12:14:14.0","2015-08-20 12:14:14.0","PCORNET_CDM",,,"\PCORI\VITAL\TOBACCO\","SMOKING"` + "\n"

func BenchmarkCSVReaderScan(b *testing.B) {
	cr := DefaultCSVReader(&bytes.Buffer{})

	data := []byte(line)

	for i := 0; i < b.N; i++ {
		_, data, _, _ = cr.scanField(data)

		if len(data) == 0 {
			data = []byte(line)
		}
	}
}
