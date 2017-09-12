package sqlimporter

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"text/template"

	"github.com/chop-dbhi/sql-importer/profile"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

const (
	rowIdColumn = "_row_id"

	// Maximum number of entries in a "target list" (e.g. column list).
	pgMaxTargetListSize = 1664
)

var (
	badChars *regexp.Regexp
	sepChars *regexp.Regexp

	sqlTmpl = template.New("sql")

	queryTmpls = map[string]string{
		"createSchema":      `create schema if not exists "{{.Schema}}"`,
		"createTable":       `create table if not exists "{{.Schema}}"."{{.Table}}" ( {{.Columns}} )`,
		"createView":        `create or replace view "{{.Schema}}"."{{.View}}" as select {{.Columns}} from "{{.Schema}}"."{{.Table}}" {{.Joins}}`,
		"createCstoreTable": `create foreign table if not exists "{{.Schema}}"."{{.Table}}" ( {{.Columns}} ) server cstore_server options (compression 'pglz')`,
		"dropTable":         `drop table if exists "{{.Schema}}"."{{.Table}}"`,
		"dropView":          `drop view if exists "{{.Schema}}"."{{.View}}"`,
		"renameTable":       `alter table "{{.Schema}}"."{{.TempTable}}" rename to "{{.Table}}"`,
		"analyzeTable":      `analyze "{{.Schema}}"."{{.Table}}"`,
	}

	// Map of revue types to SQL types.
	sqlTypeMap = map[profile.ValueType]string{
		profile.UnknownType:  "integer",
		profile.BoolType:     "boolean",
		profile.StringType:   "text",
		profile.IntType:      "integer",
		profile.FloatType:    "real",
		profile.DateType:     "date",
		profile.DateTimeType: "timestamp",
		profile.NullType:     "text",
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

func splitN(l, n int) (int, int) {
	if n > l {
		return 1, 0
	}

	// Parts.
	p := l / n

	// Remainder.
	r := l % n

	return p, r
}

func splitColumns(columns []string, n int) [][]string {
	l := len(columns)
	if n >= l {
		return [][]string{columns}
	}

	// Split columns.
	p, r := splitN(l, n)

	var hi, low int
	var colparts [][]string

	for i := 0; i < p; i++ {
		low = i * n
		hi = low + n
		var cp []string
		cp = append(cp, columns[low:hi]...)
		colparts = append(colparts, cp)
	}

	// Remainder, add another part.
	if r > 0 {
		var cp []string
		cp = append(cp, columns[hi:]...)
		colparts = append(colparts, cp)
	}

	return colparts
}

type Schema struct {
	Cstore bool
	Fields []*Field
}

func NewSchema(p *profile.Profile) *Schema {
	fields := make([]*Field, len(p.Fields))

	for n, f := range p.Fields {
		fields[f.Index] = &Field{
			Name:     n,
			Type:     sqlTypeMap[f.Type],
			Unique:   f.Unique,
			Nullable: f.Nullable || f.Missing,
		}
	}

	return &Schema{
		Fields: fields,
	}
}

// Field is a data definition on a schema.
type Field struct {
	Name     string
	Type     string
	Multiple bool
	Unique   bool
	Nullable bool
}

type tableData struct {
	Schema    string
	TempTable string
	Table     string
	View      string
	Columns   string
	Joins     string
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

	splits, err := c.createTable(schemaName, tempTableName, tableSchema)
	if err != nil {
		return 0, err
	}

	n, err := c.copyData(schemaName, tempTableName, splits, data)
	if err != nil {
		return 0, err
	}

	if err := c.dropView(schemaName, tableName); err != nil {
		return n, err
	}

	if err := c.dropTable(schemaName, tableName); err != nil {
		return n, err
	}

	if err := c.renameTable(schemaName, tempTableName, tableName, len(splits)); err != nil {
		return n, err
	}

	// Create a view if necessary and possible.
	if len(splits) > 1 && len(tableSchema.Fields)+len(splits) <= pgMaxTargetListSize {
		if err := c.createView(schemaName, tableName, tableName, splits); err != nil {
			return n, err
		}
	}

	return n, c.analyzeTable(schemaName, tableName, splits)
}

func (c *Client) Append(schemaName, tableName string, tableSchema *Schema, data io.Reader) (int64, error) {
	if err := c.createSchema(schemaName); err != nil {
		return 0, err
	}

	splits, err := c.createTable(schemaName, tableName, tableSchema)
	if err != nil {
		return 0, err
	}

	n, err := c.copyData(schemaName, tableName, splits, data)
	if err != nil {
		return 0, err
	}

	return n, c.analyzeTable(schemaName, tableName, splits)
}

func (c *Client) dropView(schemaName, viewName string) error {
	// Create the set of statements to
	data := &tableData{
		Schema: schemaName,
		View:   viewName,
	}

	var b bytes.Buffer
	if err := sqlTmpl.ExecuteTemplate(&b, "dropView", data); err != nil {
		return err
	}

	return c.execTx(func(tx *sql.Tx) error {
		sql := b.String()
		_, err := tx.Exec(sql)
		if err != nil {
			return fmt.Errorf("error dropping view: %s\n%s", err, sql)
		}

		return nil
	})
}

func (c *Client) dropTable(schemaName, tableName string) error {
	// Create the set of statements to
	data := &tableData{
		Schema: schemaName,
		Table:  tableName,
	}

	var b bytes.Buffer
	if err := sqlTmpl.ExecuteTemplate(&b, "dropTable", data); err != nil {
		return err
	}

	return c.execTx(func(tx *sql.Tx) error {
		sql := b.String()
		_, err := tx.Exec(sql)
		if err != nil {
			return fmt.Errorf("error dropping table: %s\n%s", err, sql)
		}

		return nil
	})
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

func (c *Client) createView(schemaName, viewName string, tableName string, tableColumns [][]string) error {
	var (
		firstTable    string
		rightTable    string
		leftTable     string
		selectColumns []string
		joins         []string
	)

	for i, cols := range tableColumns {
		rightTable = fmt.Sprintf("%s_%d", tableName, i)

		if firstTable == "" {
			firstTable = rightTable
		}

		// Add columns to select statement.
		for _, col := range cols {
			selectColumns = append(selectColumns, fmt.Sprintf(`"%s"."%s"."%s"`, schemaName, rightTable, col))
		}

		if leftTable != "" {
			joins = append(joins, fmt.Sprintf(`inner join "%s"."%s" on ("%s"."%s"."%s" = "%s"."%s"."%s")`, schemaName, rightTable, schemaName, leftTable, rowIdColumn, schemaName, rightTable, rowIdColumn))
		}

		leftTable = rightTable
	}

	data := &tableData{
		Table:   firstTable,
		View:    viewName,
		Schema:  schemaName,
		Columns: strings.Join(selectColumns, ", "),
		Joins:   strings.Join(joins, " "),
	}

	var b bytes.Buffer
	if err := sqlTmpl.ExecuteTemplate(&b, "createView", data); err != nil {
		return err
	}

	return c.execTx(func(tx *sql.Tx) error {
		sql := b.String()
		_, err := tx.Exec(sql)
		if err != nil {
			return fmt.Errorf("error creating view: %s\n%s", err, sql)
		}

		return nil
	})
}

func (c *Client) createTable(schemaName, tableName string, tableSchema *Schema) ([][]string, error) {
	var (
		columns       []string
		columnSchemas []string
	)

	for _, f := range tableSchema.Fields {
		// Cleaned column name.
		name := cleanFieldName(f.Name)
		columns = append(columns, name)

		var col string

		// Create index.
		// TODO: long text values cannot be indexed.
		// https://dba.stackexchange.com/questions/25138/index-max-row-size-error.
		// Should this check the max value length?
		if f.Unique && f.Type != "text" {
			col = "%s %s unique"
		} else if !f.Nullable {
			col = "%s %s not null"
		} else {
			col = "%s %s"
		}

		columnSchemas = append(columnSchemas, fmt.Sprintf(col, pq.QuoteIdentifier(name), f.Type))
	}

	partSizes := []int{
		924,
		249, // max for certain types
	}

	for _, size := range partSizes {
		columnSplits := splitColumns(columns, size)
		columnSchemaSplits := splitColumns(columnSchemas, size)

		err := c.createTableSplits(schemaName, tableName, columnSchemaSplits, tableSchema.Cstore)

		// Success.
		if err == nil {
			return columnSplits, nil
		}

		if !strings.Contains(err.Error(), "pq: tables can have at most 1600 columns") {
			return nil, err
		}
	}

	return nil, errors.New("failed to partition columns")
}

func (c *Client) createTableSplits(schemaName, tableName string, splitColumns [][]string, cstore bool) error {
	// All columns fit in the table.
	if len(splitColumns) == 1 {
		return c.execTx(func(tx *sql.Tx) error {
			return c.createSingleTable(tx, schemaName, tableName, splitColumns[0], cstore)
		})
	}

	return c.execTx(func(tx *sql.Tx) error {
		var partTables []string

		// Multiple tables, so we need to add the rowIdColumn.
		// A suffix is added to each table name. Then a view is created
		// to join the tables to together.
		for i, cols := range splitColumns {
			partTableName := fmt.Sprintf("%s_%d", tableName, i)

			ncols := []string{
				rowIdColumn + " integer not null unique",
			}
			ncols = append(ncols, cols...)

			// TODO: clean up partially created tables?
			if err := c.createSingleTable(tx, schemaName, partTableName, ncols, cstore); err != nil {
				return err
			}

			partTables = append(partTables, partTableName)
		}

		return nil
	})
}

func (c *Client) createSingleTable(tx *sql.Tx, schemaName, tableName string, columns []string, cstore bool) error {
	// Create the set of statements to
	data := &tableData{
		Schema:  schemaName,
		Table:   tableName,
		Columns: strings.Join(columns, ","),
	}

	tmplName := "createTable"
	if cstore {
		tmplName = "createCstoreTable"
	}

	var b bytes.Buffer
	if err := sqlTmpl.ExecuteTemplate(&b, tmplName, data); err != nil {
		return err
	}

	sql := b.String()
	_, err := tx.Exec(sql)
	if err != nil {
		return fmt.Errorf("error creating table: %s\n%s", err, sql)
	}
	return err
}

func (c *Client) renameSingleTable(tx *sql.Tx, schemaName, tempTableName, tableName string) error {
	var b bytes.Buffer

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
}

func (c *Client) renameTable(schemaName, tempTableName, tableName string, tableParts int) error {
	if tableParts == 1 {
		return c.execTx(func(tx *sql.Tx) error {
			return c.renameSingleTable(tx, schemaName, tempTableName, tableName)
		})
	}

	return c.execTx(func(tx *sql.Tx) error {
		for i := 0; i < tableParts; i++ {
			if err := c.renameSingleTable(tx, schemaName, fmt.Sprintf("%s_%d", tempTableName, i), fmt.Sprintf("%s_%d", tableName, i)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Client) analyzeTable(schemaName, tableName string, tableColumns [][]string) error {
	if len(tableColumns) == 1 {
		return c.execTx(func(tx *sql.Tx) error {
			return c.analyzeSingleTable(tx, schemaName, tableName)
		})
	}

	return c.execTx(func(tx *sql.Tx) error {
		for i := range tableColumns {
			if err := c.analyzeSingleTable(tx, schemaName, fmt.Sprintf("%s_%d", tableName, i)); err != nil {
				return err
			}
		}

		return nil
	})
}

func (c *Client) analyzeSingleTable(tx *sql.Tx, schemaName, tableName string) error {
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
}

func (c *Client) copyData(schemaName, tableName string, tableColumns [][]string, in io.Reader) (int64, error) {
	cr := csv.NewReader(in)

	// Read and skip columns.
	_, err := cr.Read()
	if err != nil {
		return 0, err
	}

	singleTable := len(tableColumns) == 1
	singleTableSize := len(tableColumns[0])

	txs := make([]*sql.Tx, len(tableColumns))
	stmts := make([]*sql.Stmt, len(tableColumns))

	defer func() {
		for i := range txs {
			stmts[i].Close()
			txs[i].Rollback()
		}
	}()

	for i, cols := range tableColumns {
		tx, err := c.db.Begin()
		if err != nil {
			return 0, err
		}

		txs[i] = tx

		targetTable := tableName
		if !singleTable {
			cols = append([]string{rowIdColumn}, cols...)
			targetTable = fmt.Sprintf("%s_%d", tableName, i)
		}

		stmt, err := tx.Prepare(pq.CopyInSchema(schemaName, targetTable, cols...))
		if err != nil {
			return 0, fmt.Errorf("error preparing copy: %s", err)
		}

		stmts[i] = stmt
	}

	// Allocate buffer. Max width + 1 for row id.
	// The actual bounds will need to be maintained.
	cargs := make([]interface{}, len(tableColumns[0])+1)

	var (
		n     int64
		rowid int64
	)

	// Buffer records for COPY statement.
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, fmt.Errorf("error reading record: %s", err)
		}

		rowid++

		if singleTable {
			for i, v := range row {
				if v == "" {
					cargs[i] = nil
				} else {
					cargs[i] = v
				}
			}

			_, err = stmts[0].Exec(cargs[:singleTableSize]...)
			if err != nil {
				return 0, fmt.Errorf("error sending row: %s", err)
			}
		} else {
			var low, hi int

			for i, cols := range tableColumns {
				hi = low + len(cols)

				cargs[0] = rowid

				for j, v := range row[low:hi] {
					if v == "" {
						cargs[j+1] = nil
					} else {
						cargs[j+1] = v
					}
				}

				low = hi

				_, err = stmts[i].Exec(cargs[:len(cols)+1]...)
				if err != nil {
					return 0, fmt.Errorf("error sending row: %s: %v, %v", err, cols, cargs[:len(cols)+1])
				}
			}
		}

		n++
	}

	// Empty exec to flush the buffer.
	for _, stmt := range stmts {
		_, err = stmt.Exec()
		if err != nil {
			return 0, fmt.Errorf("error executing copy: %s", err)
		}
	}

	if err != nil {
		return 0, err
	}

	// Commit transactions.
	for _, tx := range txs {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func New(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
}
