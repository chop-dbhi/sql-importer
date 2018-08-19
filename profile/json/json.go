package json

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/chop-dbhi/sql-importer/profile"
)

type analyzer struct {
	p profile.Profiler
}

func (a *analyzer) parseField(path, field string, value interface{}) {
	fp := fmt.Sprintf("%s%s", path, field)

	switch x := value.(type) {
	case nil:
		a.p.RecordType(fp, nil, profile.NullType)

	// Nested object.
	case map[string]interface{}:
		a.parseMap(fp+"/", x)

	// Array.
	case []interface{}:
		for _, v := range x {
			a.parseField(path, field, v)
		}

	case bool:
		a.p.RecordType(fp, x, profile.BoolType)

	case string:
		var t profile.ValueType

		if _, ok := profile.ParseDate(x); ok {
			t = profile.DateType
		} else if _, ok := profile.ParseDateTime(x); ok {
			t = profile.DateTimeType
		} else {
			t = profile.StringType
		}

		a.p.RecordType(fp, x, t)

	case json.Number:
		if v, err := x.Int64(); err == nil {
			a.p.RecordType(fp, v, profile.IntType)
		} else if v, err := x.Float64(); err == nil {
			a.p.RecordType(fp, v, profile.FloatType)
		} else {
			panic("could not parse JSON number")
		}

	default:
		panic(fmt.Sprintf("unsupported type: %#T", value))
	}
}

// types are identified relative to the path.
func (a *analyzer) parseMap(path string, m map[string]interface{}) {
	for k, v := range m {
		a.parseField(path, k, v)
	}
}

func (a *analyzer) parseLDJSON(r io.Reader) error {
	s := bufio.NewScanner(r)

	// Initialize buffer and JSON decoder.
	var b bytes.Buffer
	dec := json.NewDecoder(&b)
	dec.UseNumber()

	for s.Scan() {
		line := bytes.TrimSpace(s.Bytes())
		if len(line) == 0 {
			continue
		}

		b.Reset()
		b.Write(line)

		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			return err
		}

		a.parseMap("", m)
	}

	return s.Err()
}

func (a *analyzer) parseJSON(r io.Reader) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return err
	}

	if tok != json.Delim('[') {
		return fmt.Errorf("expected array, got: %v", tok)
	}

	// More elements in the array.
	for dec.More() {
		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			return err
		}

		a.parseMap("", m)
	}

	return nil
}

func Profile(config *profile.Config, in io.Reader, format string) (*profile.Profile, error) {
	p := profile.NewProfiler(config)

	a := analyzer{
		p: p,
	}

	var err error

	switch format {
	case "ldjson":
		err = a.parseLDJSON(in)
	case "json":
		err = a.parseJSON(in)
	}

	if err != nil {
		return nil, err
	}

	return p.Profile(), nil
}
