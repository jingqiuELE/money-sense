package storage

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"../input"
	"../output"
	"../test_util"
)

var (
	simpleCSV = `TEXT,TEXT,TEXT
a,b,mechant
1,2,apple
4,5,pear
6,7,safeway`

	simpleClass = `TEXT,TEXT
	mechant,category
	apple,computer
	safeway,grocery
	`
)

func NewTestCSVInput() (csvInput *input.CSVInput, fp *os.File) {
	fp = test_util.OpenCSVFromString(simpleCSV, "test.csv")

	opts := &input.CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
	}

	newInput, _ := input.NewCSVInput(opts)
	return newInput, fp
}

func TestSQLiteStorageLoadInput(t *testing.T) {
	storage := NewStorage()
	defer storage.Close()

	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())

	err := storage.Load("test", input)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestSQLiteStorageSave(t *testing.T) {
	var (
		err      error
		tempFile *os.File
	)

	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load("test", input)

	tempFile, err = ioutil.TempFile(os.TempDir(), "moneysense_test")

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer os.Remove(tempFile.Name())
	tempFile.Close()

	csvOutputOptions := output.CSVOutputOptions{
		Separator: ',',
		WriteTo:   tempFile,
	}
	csvOutput := output.NewCSVOutput(&csvOutputOptions)
	storage.Save("test", csvOutput)
	storage.Close()
}

func TestStorageQueryNormalSQL(t *testing.T) {
	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load("test", input)

	sqlString := "select count(*) from test"

	rows, rowsErr := storage.Query(sqlString)

	if rowsErr != nil {
		t.Fatalf(rowsErr.Error())
	}

	cols, colsErr := rows.Columns()

	if colsErr != nil {
		t.Fatalf(colsErr.Error())
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 column, got (%v)", len(cols))
	}

	var dest int

	for rows.Next() {
		rows.Scan(&dest)
		if dest != 3 {
			t.Fatalf("Expected 3 rows counted, got (%v)", dest)
		}
	}
}

func TestSQLiteStorageExec(t *testing.T) {
	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load("test", input)

	sqlString := "insert into test values (7,8,9)"

	result, resultErr := storage.Exec(sqlString)

	if resultErr != nil {
		t.Fatalf(resultErr.Error())
	}

	rowsAffected, rowsErr := result.RowsAffected()

	if rowsErr != nil {
		t.Fatalf(rowsErr.Error())
	}

	if rowsAffected != 1 {
		t.Fatalf("Expected 1 row affected, got (%v)", rowsAffected)
	}
}
