package csv

import (
	"fmt"
	"io"
	"strings"

	"github.com/chop-dbhi/sql-importer/profile"
)

type Profiler struct {
	Config    *profile.Config
	Delimiter byte
	Header    bool

	in io.Reader
}

func (x *Profiler) Profile() (*profile.Profile, error) {
	p := profile.NewProfiler(x.Config)
	cr := NewCSVReader(x.in, x.Delimiter)

	// First record, may be the header.
	record, err := cr.Read()
	if err != nil {
		return nil, err
	}

	header := make([]string, len(record))
	if x.Header {
		for i, n := range record {
			header[i] = strings.ToLower(n)
		}
	} else {
		for i, _ := range record {
			header[i] = fmt.Sprintf("c%d", i)
		}
	}

	// Profile first record.
	if !x.Header {
		for i, field := range header {
			val := record[i]

			// Treat empty strings as a null value.
			if val == "" {
				p.RecordType(field, nil, profile.NullType)
			} else {
				p.Record(field, val)
			}
		}

		p.Incr()
	}

	// Continue with remaining records.
	for {
		err := cr.ScanLine(record)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		for i, field := range header {
			val := record[i]

			// Treat empty strings as a null value.
			if val == "" {
				p.RecordType(field, nil, profile.NullType)
			} else {
				p.Record(field, val)
			}
		}

		p.Incr()
	}

	pf := p.Profile()

	// Set the index of the field.
	for idx, name := range header {
		pf.Fields[name].Index = idx
	}

	return pf, nil
}

func NewProfiler(r io.Reader) *Profiler {
	return &Profiler{
		Delimiter: ',',
		Header:    true,
		in:        r,
	}
}
