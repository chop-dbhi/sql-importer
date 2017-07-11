package csv

import (
	"bufio"
	"errors"
	"io"
)

var (
	csvErrUnquotedField     = errors.New("unquoted field")
	csvErrUnescapedQuote    = errors.New("bare quote")
	csvErrUnterminatedField = errors.New("unterminated field")
	csvErrExtraColumns      = errors.New("extra columns")
)

func clearRow(row []string) {
	for i, _ := range row {
		row[i] = ""
	}
}

// CSVReader provides an interface for reading CSV data
// (compatible with rfc4180 and extended with the option of having a separator other than ",").
// Successive calls to the Scan method will step through the 'fields', skipping the separator/newline between the fields.
// The EndOfRecord method tells when a field is terminated by a line break.
type CSVReader struct {
	sc *bufio.Scanner

	// If true, the scanner will continue scanning if field-level errors are
	// encountered. The error should be checked after each call to Scan to
	// handle the error.
	ContinueOnError bool

	sep    byte // values separator
	eor    bool // true when the most recent field has been terminated by a newline (not a separator).
	lineno int  // current line number (not record number)
	column int  // current column index 1-based

	eof bool
	// Error. Only set if
	err error

	// Full line, last valid column value, remaining data in the line.
	line  string
	token []byte
	data  []byte

	trail bool
}

// DefaultReader creates a "standard" CSV reader.
func DefaultCSVReader(rd io.Reader) *CSVReader {
	return NewCSVReader(rd, ',')
}

// NewReader returns a new CSV scanner.
func NewCSVReader(r io.Reader, sep byte) *CSVReader {
	s := &CSVReader{
		ContinueOnError: true,

		// Defaults to splitting by line.
		sc:  bufio.NewScanner(r),
		sep: sep,
		eor: true,
	}

	return s
}

// Line returns the current line as a string.
func (s *CSVReader) Line() string {
	return s.line
}

// Text returns the text of the current field.
func (s *CSVReader) Text() string {
	return string(s.token)
}

// LineNumber returns current line number.
func (s *CSVReader) LineNumber() int {
	return s.lineno
}

// ColumnNumber returns the column index of the current field.
func (s *CSVReader) ColumnNumber() int {
	return s.column
}

// EndOfRecord returns true when the most recent field has been terminated by a newline (not a separator).
func (s *CSVReader) EndOfRecord() bool {
	return s.eor
}

// Err returns an error if one occurred during scanning.
func (s *CSVReader) Err() error {
	if err := s.sc.Err(); err != nil {
		return err
	}

	if s.err != nil {
		return s.err
	}

	if s.eof {
		return io.EOF
	}

	return nil
}

// Read scans all fields in one line builds a slice of values.
func (s *CSVReader) Read() ([]string, error) {
	var (
		err error
		r   []string
	)

	for s.Scan() {
		if err = s.Err(); err != nil {
			return nil, err
		}

		r = append(r, s.Text())

		if s.EndOfRecord() {
			break
		}
	}

	return r, s.Err()
}

// ScanLine scans all fields in one line and puts the values in
// the passed slice.
func (s *CSVReader) ScanLine(r []string) error {
	var (
		err error
		max = len(r)
	)

	for i := 0; s.Scan(); i++ {
		// Line too long.
		if i == max {
			return csvErrExtraColumns
		}

		if err = s.Err(); err != nil {
			clearRow(r[i:])
			return err
		}

		r[i] = s.Text()

		if s.EndOfRecord() {
			break
		}
	}

	return s.Err()
}

func (s *CSVReader) Scan() bool {
	// Error.
	if s.err != nil && !s.ContinueOnError {
		return false
	}

	// EOF
	if s.eof && len(s.data) == 0 {
		return false
	}

	// If the end of the record has been reached, scan for the next line.
	if s.eor {
		// Clear.
		s.line = ""
		s.data = nil
		s.token = nil

		// Scan until there is a non-empty line to parse.
		for {
			if !s.sc.Scan() {
				// If there was an error, return. Otherwise mark as EOF.
				if err := s.sc.Err(); err != nil {
					return false
				}

				s.eof = true
				break
			}

			// Set the current line. Add the new line to parsing.
			s.line = s.sc.Text()

			// Skip empty lines.
			if s.line != "" {
				s.data = s.sc.Bytes()
				break
			}
		}
	}

	adv, token, trail, err := s.scanField(s.data)

	// Advance the section of the line for the next field.
	s.data = s.data[adv:]
	s.err = err

	if trail && len(s.data) == 0 {
		s.trail = trail
	}

	// Set the token if no error occurred otherwise mark as the end of record.
	if err == nil {
		s.token = token
	} else {
		if s.ContinueOnError {
			s.token = s.data
			s.eor = true
		} else {
			return false
		}
	}

	if !s.trail && s.eof && len(s.data) == 0 {
		return false
	}

	return true
}

func (s *CSVReader) scanField(data []byte) (int, []byte, bool, error) {
	// Special case.
	if s.trail {
		s.column++
		s.eor = true
		s.trail = false
		return 0, data, false, nil
	}

	if len(data) == 0 {
		return 0, nil, false, nil
	}

	// Previous iteration was the end of a record. Increment line and reset column.
	if s.eor {
		s.column = 0
		s.lineno++
	}

	s.column++
	s.eor = false

	// Quoted field.
	if data[0] == '"' {
		var (
			eq    int
			oq    bool
			c, pc byte
		)

		// Scan until the end quote is found.
		for i := 1; i < len(data); i++ {
			c = data[i]

			// Successive quotes denote an escaped quote. Clear the previous byte
			// to escaped quotes are not overlapped.
			if c == '"' {
				if pc == '"' {
					pc = 0
					oq = false
					eq++
					continue
				}

				// Open quote.
				if oq {
					return 0, nil, false, csvErrUnescapedQuote
				}

				oq = true
			}

			// End of field with a trailing comma.
			if pc == '"' && c == s.sep {
				return i + 1, unescapeQuotes(data[1:i-1], eq), true, nil
			}

			// Shift previous characters.
			pc = c
		}

		// Ran out of bytes.
		s.eor = true

		// Final character in the line is a quote of the last field.
		if c == '"' {
			return len(data), unescapeQuotes(data[1:len(data)-1], eq), false, nil
		}

		// End of line without a terminated quote.
		return 0, nil, false, csvErrUnterminatedField
	}

	// Unquoted fields. Only fail if a double quote is found.
	for i, c := range data {
		if c == s.sep {
			s.eor = false
			return i + 1, data[0:i], true, nil
		}

		// Unquoted field with quote.
		if c == '"' {
			return 0, nil, false, csvErrUnquotedField
		}
	}

	// Ran out of bytes.
	s.eor = true

	return len(data), data, false, nil
}

// Removes escaped quotes from the string.
func unescapeQuotes(b []byte, count int) []byte {
	if count == 0 {
		return b
	}

	for i, j := 0, 0; i < len(b); i, j = i+1, j+1 {
		b[j] = b[i]

		if b[i] == '"' && (i < len(b)-1 && b[i+1] == '"') {
			i++
		}
	}

	return b[:len(b)-count]
}
