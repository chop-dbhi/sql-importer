package reader

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var bom = []byte{0xef, 0xbb, 0xbf}

// UniversalReader wraps an io.Reader to replace carriage returns with newlines.
// This is used with the csv.Reader so it can properly delimit lines.
type UniversalReader struct {
	r io.Reader
}

func (r *UniversalReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)

	// Detect and remove BOM.
	if bytes.HasPrefix(buf, bom) {
		copy(buf, buf[len(bom):])
		n -= len(bom)
	}

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

func (r *UniversalReader) Close() error {
	if rc, ok := r.r.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

func NewUniversalReader(r io.Reader) *UniversalReader {
	return &UniversalReader{r}
}

// Decompress takes a compression type and a reader and returns
// reader that will be decompressed if the type is supported.
func Decompress(t string, r io.Reader) (io.Reader, error) {
	if t == "" {
		return r, nil
	}

	switch t {
	case "gzip", "gz":
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return gr, nil

	case "bz2", "bzip2":
		return bzip2.NewReader(r), nil
	}

	return nil, fmt.Errorf("compression type not supported: %s", t)
}

// DetectType attempts to detect the file format and compression types by looking at the
// file path extensions.
func DetectType(url string) (string, string) {
	_, name := path.Split(url)

	// Split up extensions.
	exts := strings.Split(name, ".")[1:]

	var (
		compression string
		format      string
	)

	for _, ext := range exts {
		switch ext {
		case "gz", "gzip":
			compression = "gzip"

		case "bz2", "bzip2":
			compression = "bzip2"

		case "json":
			format = "json"

		case "csv":
			format = "csv"

		case "ldjson":
			format = "ldjson"
		}
	}

	return format, compression
}

func detectCompression(name string) string {
	switch filepath.Ext(name) {
	case ".gzip", ".gz":
		return "gzip"
	case ".bzip2", ".bz2":
		return "bzip2"
	}

	return ""
}

// Reader encapsulates a stdin stream.
type Reader struct {
	Name        string
	Compression string

	reader io.Reader
	file   *os.File
}

// Read implements the io.Reader interface.
func (r *Reader) Read(buf []byte) (int, error) {
	return r.reader.Read(buf)
}

// Close implements the io.Closer interface.
func (r *Reader) Close() {
	if r.file != nil {
		r.file.Close()
	}
}

// Open a reader by name with optional compression. If no name is specified, STDIN
// is used.
func Open(name, compr string) (*Reader, error) {
	r := new(Reader)

	if compr == "" {
		compr = detectCompression(name)
	}

	// Validate Compressionession method before working with files.
	switch compr {
	case "bzip2", "gzip", "":
	default:
		return nil, fmt.Errorf("unknown compression type %s", compr)
	}

	if name == "" {
		r.reader = os.Stdin
	} else {
		file, err := os.Open(name)

		if err != nil {
			return nil, err
		}

		r.file = file
		r.reader = file
	}

	// Apply the Compressionession decoder.
	switch compr {
	case "gzip":
		reader, err := gzip.NewReader(r.reader)

		if err != nil {
			r.Close()
			return nil, err
		}

		r.reader = reader
	case "bzip2":
		r.reader = bzip2.NewReader(r.reader)
	}

	r.Compression = compr

	r.reader = &UniversalReader{r.reader}

	return r, nil
}
