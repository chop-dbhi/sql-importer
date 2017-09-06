package sqlimporter

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/chop-dbhi/sql-importer/profile/csv"
	"github.com/chop-dbhi/sql-importer/reader"
)

type Request struct {
	// Input path.
	Path string

	// Target database.
	Database string
	Schema   string
	Table    string

	// Behavior
	AppendTable bool
	CStore      bool

	// File specifics.
	CSV         bool
	Compression string

	// CSV
	Delimiter string
	Header    bool
}

func Import(r *Request) error {
	fileType, fileComp := reader.DetectType(r.Path)

	if r.CSV || fileType == "csv" {
		r.CSV = true
	} else {
		return fmt.Errorf("file type not supported: %s", fileType)
	}

	if r.Compression == "" {
		r.Compression = fileComp
	}

	if r.Table == "" {
		_, base := path.Split(r.Path)
		r.Table = strings.Split(base, ".")[0]
	}

	// Connect to database.
	db, err := sql.Open("postgres", r.Database)
	if err != nil {
		return fmt.Errorf("cannot open db connection: %s", err)
	}
	defer db.Close()

	// Open the input stream.
	input, err := reader.Open(r.Path, r.Compression)
	if err != nil {
		return fmt.Errorf("cannot open input: %s", err)
	}
	defer input.Close()

	cp := csv.NewProfiler(input)
	cp.Delimiter = r.Delimiter[0]
	cp.Header = r.Header

	prof, err := cp.Profile()
	if err != nil {
		return fmt.Errorf("profile error: %s", err)
	}

	log.Print("Done profiling")

	input.Close()
	input, err = reader.Open(r.Path, r.Compression)
	if err != nil {
		return fmt.Errorf("cannot open input: %s", err)
	}
	defer input.Close()

	schema := NewSchema(prof)
	if r.CStore {
		schema.Cstore = true
	}

	// Load intot he database.
	log.Printf(`Begin load into "%s"."%s"`, r.Schema, r.Table)

	var n int64
	dbc := New(db)
	if r.AppendTable {
		n, err = dbc.Append(r.Schema, r.Table, schema, input)
	} else {
		n, err = dbc.Replace(r.Schema, r.Table, schema, input)
	}
	if err != nil {
		return fmt.Errorf("error loading: %s", err)
	}

	log.Printf("Loaded %d records", n)

	return nil
}
