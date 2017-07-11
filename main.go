package main

import (
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"path"
	"strings"
	"time"

	"github.com/chop-dbhi/sql-importer/profile/csv"
	"github.com/chop-dbhi/sql-importer/reader"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var (
		dbUrl           string
		schemaName      string
		tableName       string
		compressionType string

		csvType      bool
		csvDelimiter string
		csvNoheader  bool

		appendTable bool
		useCstore   bool
	)

	flag.StringVar(&dbUrl, "db", "", "Database URL.")
	flag.StringVar(&schemaName, "schema", "public", "Schema name.")
	flag.StringVar(&tableName, "table", "", "Table name.")
	flag.BoolVar(&csvType, "csv", true, "CSV file. Required if using stdin.")
	flag.StringVar(&csvDelimiter, "csv.delim", ",", "CSV delimiter.")
	flag.BoolVar(&csvNoheader, "csv.noheader", false, "No CSV header present.")
	flag.StringVar(&compressionType, "compression", "", "Compression used.")
	flag.BoolVar(&useCstore, "cstore", false, "Use cstore table.")
	flag.BoolVar(&appendTable, "append", false, "Append to table.")

	flag.Parse()
	args := flag.Args()

	var (
		fileType string
	)

	// Input.
	if len(args) == 0 {
		log.Fatal("file name required")
	}

	inputName := args[0]
	fileType, fileComp := reader.DetectType(inputName)

	if csvType || fileType == "csv" {
		csvType = true
	} else {
		log.Fatal("file type not supported: %s", fileType)
	}

	if compressionType == "" {
		compressionType = fileComp
	}

	if tableName == "" {
		_, base := path.Split(inputName)
		tableName = strings.Split(base, ".")[0]
	}

	// Connect to database.
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("cannot open db connection: %s", err)
	}
	defer db.Close()

	// Open the input stream.
	input, err := reader.Open(inputName, compressionType)
	if err != nil {
		log.Fatal("cannot open input: %s", err)
	}
	defer input.Close()

	cp := csv.NewProfiler(input)
	cp.Delimiter = csvDelimiter[0]
	cp.Header = !csvNoheader

	prof, err := cp.Profile()
	if err != nil {
		log.Fatalf("profile error: %s", err)
	}

	log.Print("Done profiling")

	input.Close()
	input, err = reader.Open(inputName, compressionType)
	if err != nil {
		log.Fatal("cannot open input: %s", err)
	}
	defer input.Close()

	schema := NewSchema(prof)
	if useCstore {
		schema.Cstore = true
	}

	// Load intot he database.
	log.Printf(`Begin load into "%s"."%s"`, schemaName, tableName)

	var n int64
	dbc := New(db)
	if appendTable {
		n, err = dbc.Append(schemaName, tableName, schema, input)
	} else {
		n, err = dbc.Replace(schemaName, tableName, schema, input)
	}
	if err != nil {
		log.Fatalf("error loading: %s", err)
	}

	log.Printf("Loaded %d records", n)
}
