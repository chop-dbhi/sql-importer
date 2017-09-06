package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/chop-dbhi/sql-importer"
)

func main() {
	var (
		dbUrl           string
		schemaName      string
		tableName       string
		compressionType string

		csvType      bool
		csvDelimiter string
		csvNoHeader  bool

		useCstore   bool
		appendTable bool
	)

	flag.StringVar(&dbUrl, "db", "", "Database URL.")
	flag.StringVar(&schemaName, "schema", "public", "Schema name.")
	flag.StringVar(&tableName, "table", "", "Table name.")
	flag.BoolVar(&csvType, "csv", true, "CSV file. Required if using stdin.")
	flag.StringVar(&csvDelimiter, "csv.delim", ",", "CSV delimiter.")
	flag.BoolVar(&csvNoHeader, "csv.noheader", false, "No CSV header present.")
	flag.StringVar(&compressionType, "compression", "", "Compression used.")
	flag.BoolVar(&useCstore, "cstore", false, "Use cstore table.")
	flag.BoolVar(&appendTable, "append", false, "Append to table.")

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		log.Fatal("file name or directory required")
	}

	inputName := args[0]

	stat, _ := os.Stat(inputName)

	if stat.IsDir() {
		loadDir(
			inputName,
			dbUrl,
			compressionType,
			csvDelimiter,
			appendTable,
			useCstore,
		)
	} else {
		loadFile(
			inputName,
			dbUrl,
			schemaName,
			tableName,
			compressionType,
			csvDelimiter,
			csvType,
			appendTable,
			useCstore,
			csvNoHeader,
		)
	}
}

func loadFile(path, dbUrl, schemaName, tableName, compressionType, csvDelimiter string, csvType, appendTable, useCstore, csvNoHeader bool) {
	r := sqlimporter.Request{
		Path: path,

		Database: dbUrl,
		Schema:   schemaName,
		Table:    tableName,

		AppendTable: appendTable,
		CStore:      useCstore,

		CSV:         csvType,
		Compression: compressionType,

		Delimiter: csvDelimiter,
		Header:    !csvNoHeader,
	}

	if err := sqlimporter.Import(&r); err != nil {
		log.Fatal(err)
	}
}

func loadDir(rootDir, dbUrl, compressionType, csvDelimiter string, appendTable, useCstore bool) {
	wg := &sync.WaitGroup{}

	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		rpath, _ := filepath.Rel(rootDir, path)
		dir, base := filepath.Split(rpath)

		tableName := strings.Split(base, ".")[0]
		schemaName := strings.Replace(strings.Trim(dir, "/"), "/", "_", -1)

		if schemaName == "" {
			schemaName = "public"
		}

		r := sqlimporter.Request{
			Path: path,

			Database: dbUrl,
			Schema:   schemaName,
			Table:    tableName,

			AppendTable: appendTable,
			CStore:      useCstore,

			CSV:         true,
			Compression: compressionType,

			Delimiter: csvDelimiter,
			Header:    true,
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			defer func() {
				if err := recover(); err != nil {
					log.Printf("error loading file: %s", rpath)
					log.Printf("%s", err)
				}
			}()

			log.Printf(`loading file %s into table "%s"."%s"`, rpath, schemaName, tableName)

			if err := sqlimporter.Import(&r); err != nil {
				log.Printf("error importing file: %s", err)
			}
		}()

		return nil
	})

	wg.Wait()

}
