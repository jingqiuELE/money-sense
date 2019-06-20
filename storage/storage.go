package storage

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"strings"

	"../input"

	sqlite3 "github.com/mattn/go-sqlite3"
)

type Storage struct {
	db     *sql.DB
	connID int
}

var sqlite3conn = []*sqlite3.SQLiteConn{}

func init() {
	sql.Register("sqlite3_ms",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				sqlite3conn = append(sqlite3conn, conn)
				return nil
			},
		})
}

func (s *Storage) open() error {
	db, err := sql.Open("sqlite3_ms", "file::memory:?cache=shared")
	if err != nil {
		log.Fatalln(err)
	}

	err = db.Ping()

	if err != nil {
		log.Fatalln(err)
	}

	s.connID = len(sqlite3conn) - 1
	s.db = db
	return err
}

func NewStorage() *Storage {
	storage := Storage{}

	err := storage.open()
	if err != nil {
		log.Fatal("Failed to initialize storage")
	}
	return &storage
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) Load(input *input.CSVInput) error {
	tableName := strings.Replace(input.Name(), path.Ext(input.Name()), "", -1)
	err := s.createTable(tableName, input.Header(), input.Types())
	if err != nil {
		log.Fatal("Failed to create table!")
	}

	tx, txErr := s.db.Begin()

	if txErr != nil {
		log.Fatalln(txErr)
	}

	stmt := s.createLoadStmt(tableName, len(input.Header()), tx)

	row := input.ReadRecord()
	for {
		if row == nil {
			break
		}
		s.loadRow(tableName, len(input.Header()), row, tx, stmt, true)
		row = input.ReadRecord()
	}
	stmt.Close()
	tx.Commit()

	return err
}

func (s *Storage) Query(query string) (*sql.Rows, error) {
	rows, err := s.db.Query(query)
	return rows, err
}

func (s *Storage) QueryRow(query string) *sql.Row {
	return s.db.QueryRow(query)
}

func (s *Storage) Exec(query string) (sql.Result, error) {
	return s.db.Exec(query)
}

// SaveTo saves the current in memory database to the path provided as a string.
func (s *Storage) SaveTo(path string) error {
	backupDb, openErr := sql.Open("sqlite3_ms", path)
	if openErr != nil {
		return openErr
	}

	backupPingErr := backupDb.Ping()
	if backupPingErr != nil {
		return backupPingErr
	}
	backupConnID := len(sqlite3conn) - 1

	backup, backupStartErr := sqlite3conn[backupConnID].Backup("main", sqlite3conn[s.connID], "main")
	if backupStartErr != nil {
		return backupStartErr
	}

	_, backupPerformError := backup.Step(-1)
	if backupPerformError != nil {
		return backupPerformError
	}

	backupFinishError := backup.Finish()
	if backupFinishError != nil {
		return backupFinishError
	}

	backupCloseError := backupDb.Close()
	if backupCloseError != nil {
		return backupCloseError
	}
	return nil
}

func (s *Storage) createTable(tableName string, headers []string, types []string) error {
	var sqlStmt strings.Builder

	if len(headers) != len(types) {
		log.Fatal("Unmatched column names!")
	}

	cols := len(headers)
	fmt.Fprintf(&sqlStmt, "CREATE TABLE IF NOT EXISTS %v (", tableName)
	for i := 0; i < cols; i++ {
		sqlStmt.WriteString(headers[i])
		sqlStmt.WriteString(" ")
		sqlStmt.WriteString(types[i])
		if i != cols-1 {
			sqlStmt.WriteString(",")
		}
	}
	sqlStmt.WriteString(");")
	_, err := s.db.Exec(sqlStmt.String())
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	return err
}

func (s *Storage) createLoadStmt(tableName string, colCount int, db *sql.Tx) *sql.Stmt {
	if colCount == 0 {
		log.Fatalln("Nothing to build insert with!")
	}
	var buffer strings.Builder

	buffer.WriteString("INSERT INTO " + (tableName) + " VALUES (")
	// Don't write the comma for the last column
	for i := 1; i <= colCount; i++ {
		buffer.WriteString("nullif(?,'')")
		if i != colCount {
			buffer.WriteString(", ")
		}
	}

	buffer.WriteString(");")

	stmt, err := s.db.Prepare(buffer.String())

	if err != nil {
		log.Fatalln("Could not create load stmt:", err)
	}
	return stmt
}

func (s *Storage) loadRow(tableName string, colCount int, values []string, db *sql.Tx, stmt *sql.Stmt, verbose bool) error {
	if len(values) == 0 || colCount == 0 {
		return nil
	}

	var vals []interface{}

	for i := 0; i < colCount; i++ {
		vals = append(vals, values[i])
	}

	_, err := stmt.Exec(vals...)

	if err != nil && verbose {
		log.Printf("Bad row: %v\n", err)
	}

	return err
}
