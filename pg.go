package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/chop-dbhi/sql-importer/profile"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

var (
	badChars *regexp.Regexp
	sepChars *regexp.Regexp

	sqlTmpl = template.New("sql")

	queryTmpls = map[string]string{
		"createSchema":      `create schema if not exists "{{.Schema}}"`,
		"createTable":       `create table if not exists "{{.Schema}}"."{{.Table}}" ( {{.Columns}} )`,
		"createCstoreTable": `create foreign table if not exists "{{.Schema}}"."{{.Table}}" ( {{.Columns}} ) server cstore_server options (compression 'pglz')`,
		"dropTable":         `drop table if exists "{{.Schema}}"."{{.Table}}"`,
		"renameTable":       `alter table "{{.Schema}}"."{{.TempTable}}" rename to "{{.Table}}"`,
		"analyzeTable":      `analyze "{{.Schema}}"."{{.Table}}"`,
	}
)

func init() {
	// Initialize SQL statement templates.
	for name, tmpl := range queryTmpls {
		template.Must(sqlTmpl.New(name).Parse(tmpl))
	}

	badChars = regexp.MustCompile(`[^a-z0-9_\-\.\+]+`)
	sepChars = regexp.MustCompile(`[_\-\.\+]+`)
}

// Map of revue types to SQL types.
var sqlTypeMap = map[profile.ValueType]string{
	profile.UnknownType:  "integer",
	profile.BoolType:     "boolean",
	profile.StringType:   "text",
	profile.IntType:      "integer",
	profile.FloatType:    "real",
	profile.DateType:     "date",
	profile.DateTimeType: "timestamp",
	profile.NullType:     "text",
}

type Schema struct {
	Cstore bool
	Fields map[string]*Field `json:"fields"`
}

func NewSchema(p *profile.Profile) *Schema {
	fields := make(map[string]*Field, len(p.Fields))

	for n, f := range p.Fields {
		fields[n] = &Field{
			Name:     n,
			Type:     sqlTypeMap[f.Type],
			Unique:   f.Unique,
			Nullable: f.Nullable,
		}
	}

	return &Schema{
		Fields: fields,
	}
}

// Field is a data definition on a schema.
type Field struct {
	// Name is the unique name of the field with respect to the schema.
	Name string `json:"name"`

	// Type is the data type of the values that can be assigned to
	// this field.
	Type string `json:"type"`

	// If true, this value supports multiple values.
	Multiple bool `json:"multiple"`

	// If true, values across a set of records are expected to be unique.
	Unique bool `json:"unique"`

	// If true, values can be "null", that is, not specified.
	Nullable bool `json:"nullable"`
}

type tableData struct {
	Schema    string
	TempTable string
	Table     string
	Columns   string
}

// TODO: fuzz test this.
func cleanFieldName(n string) string {
	n = strings.ToLower(n)
	n = badChars.ReplaceAllString(n, "_")
	return sepChars.ReplaceAllString(n, "_")
}

type Client struct {
	db *sql.DB
}

// execTx calls a function within a transaction.
func (c *Client) execTx(fn func(tx *sql.Tx) error) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (c *Client) Replace(schemaName, tableName string, tableSchema *Schema, data io.Reader) (int64, error) {
	tempTableName := uuid.NewV4().String()

	if err := c.createSchema(schemaName); err != nil {
		return 0, err
	}

	if err := c.createTable(schemaName, tempTableName, tableSchema); err != nil {
		return 0, err
	}

	n, err := c.copyData(schemaName, tempTableName, data)
	if err != nil {
		return 0, err
	}

	if err := c.renameTable(schemaName, tempTableName, tableName); err != nil {
		return n, err
	}

	return n, c.analyzeTable(schemaName, tableName)
}

func (c *Client) Append(schemaName, tableName string, tableSchema *Schema, data io.Reader) (int64, error) {
	if err := c.createSchema(schemaName); err != nil {
		return 0, err
	}

	if err := c.createTable(schemaName, tableName, tableSchema); err != nil {
		return 0, err
	}

	n, err := c.copyData(schemaName, tableName, data)
	if err != nil {
		return 0, err
	}

	return n, c.analyzeTable(schemaName, tableName)
}

func (c *Client) createSchema(schemaName string) error {
	// Create the set of statements to
	data := &tableData{
		Schema: schemaName,
	}

	var b bytes.Buffer
	if err := sqlTmpl.ExecuteTemplate(&b, "createSchema", data); err != nil {
		return err
	}

	return c.execTx(func(tx *sql.Tx) error {
		sql := b.String()
		_, err := tx.Exec(sql)
		if err != nil {
			return fmt.Errorf("error creating schema: %s\n%s", err, sql)
		}

		return nil
	})
}

func (c *Client) createTable(schemaName, tableName string, tableSchema *Schema) error {
	var columns []string

	for _, f := range tableSchema.Fields {
		var col string

		// Create index.
		if f.Unique {
			col = "%s %s unique"
		} else if !f.Nullable {
			col = "%s %s not null"
		} else {
			col = "%s %s"
		}

		name := cleanFieldName(f.Name)
		columns = append(columns, fmt.Sprintf(col, pq.QuoteIdentifier(name), f.Type))
	}

	sort.Strings(columns)

	// Create the set of statements to
	data := &tableData{
		Schema:  schemaName,
		Table:   tableName,
		Columns: strings.Join(columns, ","),
	}

	return c.execTx(func(tx *sql.Tx) error {
		tmplName := "createTable"
		if tableSchema.Cstore {
			tmplName = "createCstoreTable"
		}

		var b bytes.Buffer
		if err := sqlTmpl.ExecuteTemplate(&b, tmplName, data); err != nil {
			return err
		}

		sql := b.String()
		if _, err := tx.Exec(sql); err != nil {
			return fmt.Errorf("error creating table: %s\n%s", err, sql)
		}

		return nil
	})
}

func (c *Client) renameTable(schemaName, tempTableName, tableName string) error {
	// Create the set of statements to
	data := &tableData{
		Schema:    schemaName,
		TempTable: tempTableName,
		Table:     tableName,
	}

	tmpls := []string{
		"dropTable",
		"renameTable",
	}

	var b bytes.Buffer

	return c.execTx(func(tx *sql.Tx) error {
		for _, name := range tmpls {
			b.Reset()
			if err := sqlTmpl.ExecuteTemplate(&b, name, data); err != nil {
				return err
			}

			if _, err := tx.Exec(b.String()); err != nil {
				return fmt.Errorf("error renaming table: %s", err)
			}
		}

		return nil
	})
}

func (c *Client) analyzeTable(schemaName, tableName string) error {
	return c.execTx(func(tx *sql.Tx) error {
		// Create the set of statements to
		data := &tableData{
			Schema: schemaName,
			Table:  tableName,
		}

		var b bytes.Buffer
		if err := sqlTmpl.ExecuteTemplate(&b, "analyzeTable", data); err != nil {
			return err
		}

		sql := b.String()
		if _, err := tx.Exec(sql); err != nil {
			return fmt.Errorf("error analyzinng table: %s\n%s", err, sql)
		}

		return nil
	})
}

func (c *Client) copyData(schemaName, tableName string, in io.Reader) (int64, error) {
	cr := csv.NewReader(in)

	columns, err := cr.Read()
	if err != nil {
		return 0, err
	}

	for i, c := range columns {
		columns[i] = cleanFieldName(c)
	}

	var n int64

	err = c.execTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyInSchema(schemaName, tableName, columns...))
		if err != nil {
			return fmt.Errorf("error preparing copy: %s", err)
		}

		cargs := make([]interface{}, len(columns))

		// Buffer records for COPY statement.
		for {
			row, err := cr.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				return fmt.Errorf("error reading record: %s", err)
			}

			for i, v := range row {
				if v == "" {
					cargs[i] = nil
				} else {
					cargs[i] = v
				}
			}

			_, err = stmt.Exec(cargs...)
			if err != nil {
				return fmt.Errorf("error sending row: %s", err)
			}

			n++
		}

		// Empty exec to flush the buffer.
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("error executing copy: %s", err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return n, nil
}

func New(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
}
