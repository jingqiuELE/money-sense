package input

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

type CSVInput struct {
	options         *CSVInputOptions
	reader          *csv.Reader
	name            string
	types           []string
	header          []string
	minOutputLength int
}

// CSVInputOptions options are passed to the underlying encoding/csv reader.
type CSVInputOptions struct {
	// Separator is the rune that fields are delimited by.
	Separator rune
	// ReadFrom is where the data will be read from.
	ReadFrom io.Reader
}

func NewCSVInput(opts *CSVInputOptions) (*CSVInput, error) {
	csvInput := &CSVInput{
		options: opts,
		reader:  csv.NewReader(opts.ReadFrom),
	}

	csvInput.reader.FieldsPerRecord = -1
	csvInput.reader.Comma = csvInput.options.Separator
	csvInput.reader.LazyQuotes = true

	headerErr := csvInput.readHeader()
	if headerErr != nil {
		return nil, headerErr
	}

	if asFile, ok := csvInput.options.ReadFrom.(*os.File); ok {
		csvInput.name = filepath.Dir(asFile.Name())
	} else {
		csvInput.name = "pipe"
	}

	return csvInput, nil
}

// ReadRecord reads a single record from the CSV.
// If the record is empty, an empty []string is returned.
// Record expand to match the current row size, adding blank fields as needed.
// Records never return less then the number of fields in the first row.
// Returns nil on EOF
// In the event of a parse error due to an invalid record, it is logged, and
// an empty []string is returned with the number of fields in the first row,
// as if the record were empty.
func (csvInput *CSVInput) ReadRecord() []string {
	var row []string
	var fileErr error

	row, fileErr = csvInput.reader.Read()
	emptysToAppend := csvInput.minOutputLength - len(row)
	if fileErr == io.EOF {
		return nil
	} else if parseErr, ok := fileErr.(*csv.ParseError); ok {
		log.Println(parseErr)
		emptysToAppend = csvInput.minOutputLength
	}

	if emptysToAppend > 0 {
		for counter := 0; counter < emptysToAppend; counter++ {
			row = append(row, "")
		}
	}
	return row
}

func (csvInput *CSVInput) readHeader() error {
	var readErr error

	csvInput.types, readErr = csvInput.reader.Read()
	if readErr != nil {
		log.Fatalln(readErr)
		return readErr
	}

	csvInput.minOutputLength = len(csvInput.types)
	csvInput.header, readErr = csvInput.reader.Read()
	if readErr != nil {
		header := make([]string, csvInput.minOutputLength)
		copy(header, csvInput.header)
		for i := len(csvInput.header); i < csvInput.minOutputLength; i++ {
			header[i] = "c" + strconv.Itoa(i)
		}
		csvInput.header = header
	}
	if len(csvInput.header) != csvInput.minOutputLength {
		log.Fatalln("Column names and types should have the same length!")
	}
	return nil
}

// Name returns the name of the CSV being read.
// By default, either the base filename or 'pipe' if it is a unix pipe
func (csvInput *CSVInput) Name() string {
	return csvInput.name
}

// Header returns the header of the csvInput. Either the first row if a header
// set in the options, or c#, where # is the column number, starting with 0.
func (csvInput *CSVInput) Header() []string {
	return csvInput.header
}

func (csvInput *CSVInput) Types() []string {
	return csvInput.types
}
