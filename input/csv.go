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
	Options   *CSVInputOptions
	reader    *csv.Reader
	name      string
	types     []string
	columns   []string
	columnLen int
}

// CSVInputOptions options are passed to the underlying encoding/csv reader.
type CSVInputOptions struct {
	// Separator is the rune that fields are delimited by.
	Separator rune
	// ReadFrom is where the data will be read from.
	ReadFrom   io.Reader
	TimeFormat string
}

func NewCSVInput(opts *CSVInputOptions) (*CSVInput, error) {
	csvInput := &CSVInput{
		Options: opts,
		reader:  csv.NewReader(opts.ReadFrom),
	}

	csvInput.reader.FieldsPerRecord = -1
	csvInput.reader.Comma = csvInput.Options.Separator
	csvInput.reader.LazyQuotes = true

	headerErr := csvInput.readHeader()
	if headerErr != nil {
		return nil, headerErr
	}

	if asFile, ok := csvInput.Options.ReadFrom.(*os.File); ok {
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
func (csvInput *CSVInput) ReadRow() []string {
	var row []string
	var fileErr error

	row, fileErr = csvInput.reader.Read()
	emptysToAppend := csvInput.columnLen - len(row)
	if fileErr == io.EOF {
		return nil
	} else if parseErr, ok := fileErr.(*csv.ParseError); ok {
		log.Println(parseErr)
		emptysToAppend = csvInput.columnLen
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

	csvInput.columnLen = len(csvInput.types)
	csvInput.columns, readErr = csvInput.reader.Read()
	if readErr != nil {
		columns := make([]string, csvInput.columnLen)
		copy(columns, csvInput.columns)
		for i := len(csvInput.columns); i < csvInput.columnLen; i++ {
			columns[i] = "c" + strconv.Itoa(i)
		}
		csvInput.columns = columns
	}
	if len(csvInput.columns) != csvInput.columnLen {
		log.Fatalln("Column names and types should have the same length!")
	}
	return nil
}

// Name returns the name of the CSV being read.
// By default, either the base filename or 'pipe' if it is a unix pipe
func (csvInput *CSVInput) Name() string {
	return csvInput.name
}

// columns returns the columns of the csvInput. Either the first row if a columns
// set in the options, or c#, where # is the column number, starting with 0.
func (csvInput *CSVInput) Columns() []string {
	return csvInput.columns
}

func (csvInput *CSVInput) Types() []string {
	return csvInput.types
}
