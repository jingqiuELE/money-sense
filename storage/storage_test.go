package storage

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"../input"
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

func TestSQLiteStorageCreateTable(t *testing.T) {
	storage := NewStorage()
	defer storage.Close()

	storage.Exec("CREATE TABLE IF NOT EXISTS history (a TEXT,b TEXT,mechant TEXT);")
	storage.Exec("CREATE TABLE IF NOT EXISTS classifier (mechant TEXT,category TEXT);")

	_, err := storage.Exec(`INSERT INTO history VALUES ("sand", "beach", "wave");`)
	if err != nil {
		t.Fatalf("Failed to INSERT into table:%v", err)
	}

	tx, txErr := storage.db.Begin()

	if txErr != nil {
		log.Fatalln(txErr)
	}
	stmt := storage.createLoadStmt("history", 3, tx)
	stmt.Close()
	tx.Commit()
}

func TestSQLiteStorageLoadInput(t *testing.T) {
	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load(input)
}

func TestSQLiteStorageSaveTo(t *testing.T) {
	var (
		cmdOut   []byte
		err      error
		tempFile *os.File
	)

	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load(input)

	tempFile, err = ioutil.TempFile(os.TempDir(), "moneysense_test")

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer os.Remove(tempFile.Name())
	tempFile.Close()
	storage.SaveTo(tempFile.Name())
	storage.Close()

	args := []string{tempFile.Name(), "pragma integrity_check;"}

	cmd := exec.Command("sqlite3", args...)
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if cmdOut, err = cmd.Output(); err != nil {
		fmt.Println(string(cmdOut))
		fmt.Println(args)
		t.Fatalf(err.Error())
	}
	cmdOutStr := string(cmdOut)

	if cmdOutStr != "ok\n" {
		fmt.Println(cmdOutStr)
		t.Fatalf("SaveTo integrity check failed!")
	}
}

func TestStorageQueryNormalSQL(t *testing.T) {
	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load(input)

	tableName := strings.Replace(input.Name(), path.Ext(input.Name()), "", -1)
	sqlString := "select count(*) from " + tableName

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

func LoadTestDataAndExecuteQuery(t *testing.T, testData string, sqlString string) (map[int]map[string]interface{}, []string) {
	storage := NewStorage()
	fp := test_util.OpenCSVFromString(testData, "test.csv")

	opts := &input.CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
	}

	input, _ := input.NewCSVInput(opts)
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load(input)

	rows, rowsErr := storage.Query(sqlString)

	if rowsErr != nil {
		t.Fatalf(rowsErr.Error())
	}

	cols, colsErr := rows.Columns()

	if colsErr != nil {
		t.Fatalf(colsErr.Error())
	}

	rowNumber := 0
	result := make(map[int]map[string]interface{})
	rawResult := make([]interface{}, len(cols))
	dest := make([]interface{}, len(cols))

	for i := range cols {
		dest[i] = &rawResult[i]
	}

	for rows.Next() {
		scanErr := rows.Scan(dest...)

		if scanErr != nil {
			t.Fatalf(scanErr.Error())
		}

		result[rowNumber] = make(map[string]interface{})
		for i, raw := range rawResult {
			result[rowNumber][cols[i]] = raw
		}
		rowNumber++
	}

	return result, cols
}

func TestSQLiteStorageExec(t *testing.T) {
	storage := NewStorage()
	input, fp := NewTestCSVInput()
	defer fp.Close()
	defer os.Remove(fp.Name())
	defer storage.Close()

	storage.Load(input)

	tableName := strings.Replace(input.Name(), path.Ext(input.Name()), "", -1)
	sqlString := "insert into " + tableName + " values (7,8,9)"

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
