package output

import (
	"encoding/csv"
	"io"
)

type CSVOutput struct {
	Options *CSVOutputOptions
	writer  *csv.Writer
}

type CSVOutputOptions struct {
	Separator  rune
	WriteTo    io.Writer
	TimeFormat string
}

func NewCSVOutput(opts *CSVOutputOptions) *CSVOutput {
	csvOutput := &CSVOutput{
		Options: opts,
		writer:  csv.NewWriter(opts.WriteTo),
	}
	csvOutput.writer.Comma = opts.Separator

	return csvOutput
}

func (csvOutput *CSVOutput) WriteHeader(types []string, columns []string) error {
	err := csvOutput.writer.Write(types)
	if err != nil {
		return err
	}

	err = csvOutput.writer.Write(columns)
	if err != nil {
		return err
	}
	return nil
}

func (csvOutput *CSVOutput) WriteRow(values []string) error {
	err := csvOutput.writer.Write(values)
	csvOutput.writer.Flush()
	return err
}

func (csvOutput *CSVOutput) Flush() error {
	csvOutput.writer.Flush()
	return nil
}
