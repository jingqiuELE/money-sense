package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"./input"
	"./output"
	"./storage"
)

const TimeFormat = "01/02/2006"

type MoneySense struct {
	store          *storage.Storage
	historyPath    string
	history        string
	classifierPath string
	classifier     string
}

type Record struct {
	Date     time.Time
	Amount   float64
	Category string
}

func NewMoneySense(historyPath string, classifierPath string) (*MoneySense, error) {
	store := storage.NewStorage()

	historyName, err := loadData(historyPath, store)
	if err != nil {
		return nil, err
	}

	classifierName, err := loadData(classifierPath, store)
	if err != nil {
		return nil, err
	}

	return &MoneySense{
		store:          store,
		historyPath:    historyPath,
		history:        historyName,
		classifierPath: classifierPath,
		classifier:     classifierName,
	}, nil
}

func loadData(filePath string, store *storage.Storage) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		log.Fatal("Failed to get the status of file %v\n", filePath)
	}

	var tableName string
	switch mode := fi.Mode(); {
	case mode.IsDir():
		tableName = path.Base(filePath)
	case mode.IsRegular():
		extension := path.Ext(filePath)
		tableName = path.Base(filePath[0 : len(filePath)-len(extension)])
	}

	err = filepath.Walk(filePath, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", p, err)
			return err
		}
		if path.Ext(p) == ".csv" {
			fmt.Println("loadData on", p)
			reader, err := os.Open(p)
			if err != nil {
				log.Fatal("Could not open %q", p)
			}

			opts := &input.CSVInputOptions{
				Separator:  ',',
				ReadFrom:   reader,
				TimeFormat: TimeFormat,
			}

			csvInput, err := input.NewCSVInput(opts)
			if err != nil {
				log.Fatal("Could not create new CSVInput")
			}
			err = store.Load(tableName, csvInput)
			if err != nil {
				log.Fatal("Could not load csv file into Storage")
			}
		}
		return err
	})

	fmt.Println("tableName:", tableName)
	return tableName, err
}

func (ms *MoneySense) Close() error {
	err := ms.store.Close()
	if err != nil {
		log.Fatal("Could not close Storage")
	}

	return err
}

func (ms *MoneySense) Classify() error {
	query := fmt.Sprintf(`SELECT date, mechant, IFNULL(credit, 0) FROM %v`, ms.history)
	rows, err := ms.store.Query(query)
	if err != nil {
		log.Fatalf("Failed to query storage! %q, err=%v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		var date time.Time
		var mechant, category string
		var credit float64
		var changed bool
		err = rows.Scan(&date, &mechant, &credit)
		if err != nil {
			log.Fatal(err)
		}
		query := fmt.Sprintf(`SELECT category FROM "%v" WHERE mechant == "%v"`, ms.classifier, mechant)
		err = ms.store.QueryRow(query).Scan(&category)
		if err == sql.ErrNoRows {
			fmt.Printf("What is the category of %v?\n", mechant)
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			} else {
				category = strings.TrimSuffix(input, "\n")
				insert := fmt.Sprintf(`INSERT INTO "%v"(mechant, category) VALUES("%v", "%v")`, ms.classifier, mechant, category)
				_, err = ms.store.Exec(insert)
				if err != nil {
					log.Fatal("Failed to insert category information")
				}
			}
			changed = true
		} else if err != nil {
			log.Fatalf("Failed to classify: %v, err=%v\n", query, err)
		} else {
			fmt.Printf("Classify %v as %v\n", mechant, category)
		}

		if changed {
			writer, err := os.OpenFile(ms.classifierPath, os.O_WRONLY, 0600)
			if err != nil {
				log.Fatalf("Could not open %q", ms.classifierPath)
			}

			csvOutputOptions := output.CSVOutputOptions{
				Separator:  ',',
				WriteTo:    writer,
				TimeFormat: TimeFormat,
			}
			csvOutput := output.NewCSVOutput(&csvOutputOptions)
			err = ms.store.Save(ms.classifier, csvOutput)
			if err != nil {
				log.Fatal("Failed to save new classified data", err)
			}
		}
	}
	return nil
}

func (ms *MoneySense) Retrieve(category string, start string, end string) []Record {
	var result []Record

	start_dt, err := time.Parse(TimeFormat, start)
	if err != nil {
		log.Fatal("Failed to parse date:", start)
	}

	end_dt, err := time.Parse(TimeFormat, end)
	if err != nil {
		log.Fatal("Failed to parse date:", end)
	}

	QUERY := fmt.Sprintf(`SELECT date, IFNULL(credit, 0), category FROM %v INNER JOIN %v ON %v.mechant = %v.mechant WHERE date >= '%v' AND date <= '%v' ORDER BY date ASC`,
		ms.history, ms.classifier, ms.history, ms.classifier, start_dt, end_dt)
	rows, err := ms.store.Query(QUERY)
	if err != nil {
		log.Fatal("query data base failed!: ", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r Record

		err = rows.Scan(&r.Date, &r.Amount, &r.Category)
		if err != nil {
			log.Fatal(err)
		}
		if category == "*" || r.Category == category {
			result = append(result, r)
		}
		fmt.Println("rows:", r)
	}
	if rows.Err() != nil {
		log.Fatal(rows.Err())
	}
	return result
}
