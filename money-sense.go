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
	"./storage"
)

const TimeFormat = "01/02/2006"

type MoneySense struct {
	store      *storage.Storage
	history    string
	classifier string
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
		store:      store,
		history:    historyName,
		classifier: classifierName,
	}, nil
}

func loadData(csvPath string, store *storage.Storage) (string, error) {
	err := filepath.Walk(csvPath, func(p string, info os.FileInfo, err error) error {
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
				Separator: ',',
				ReadFrom:  reader,
			}

			csvInput, err := input.NewCSVInput(opts)
			if err != nil {
				log.Fatal("Could not create new CSVInput")
			}
			err = store.Load(csvInput)
			if err != nil {
				log.Fatal("Could not load csv file into Storage")
			}
		}
		return err
	})
	return filepath.Base(csvPath), err
}

func (ms *MoneySense) Close() error {
	err := ms.store.Close()
	if err != nil {
		log.Fatal("Could not close Storage")
	}

	return err
}

func (ms *MoneySense) Classify() error {
	query := fmt.Sprintf(`SELECT * FROM %v`, ms.history)
	rows, err := ms.store.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var date time.Time
		var mechant, category string
		var credit, deposit, total float64
		err = rows.Scan(&date, &mechant, &credit, &deposit, &total)
		if err != nil {
			log.Fatal(err)
		}
		query := fmt.Sprintf(`SELECT category FROM %v WHERE mechant == '%v'`, ms.classifier, mechant)
		err = ms.store.QueryRow(query).Scan(&category)
		if err == sql.ErrNoRows {
			fmt.Printf("What is the category of %v?\n", mechant)
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			} else {
				category = strings.TrimSuffix(input, "\n")
				insert := fmt.Sprintf(`INSERT INTO %v(mechant, category) VALUES("%v", "%v")`, ms.classifier, mechant, category)
				_, err = ms.store.Exec(insert)
				if err != nil {
					log.Fatal("Failed to insert category information")
				}
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

	//QUERY := fmt.Sprintf(`SELECT date, history.mechant, credit, category FROM %v INNER JOIN %v ON %v.mechant = %v.mechant WHERE date >= '%v' AND date <= '%v'`,
	//	ms.history, ms.classifier, ms.history, ms.classifier, start_dt, end_dt)
	//QUERY := fmt.Sprintf(`SELECT date, mechant, credit FROM %v WHERE date >= '%v' AND date <= '%v';`, ms.history, start_dt, end_dt)
	_ = start_dt
	_ = end_dt
	QUERY := fmt.Sprintf(`SELECT date, mechant, credit FROM %v`, ms.history)
	fmt.Println("query:", QUERY)
	rows, err := ms.store.Query(QUERY)
	if err != nil {
		log.Fatal("query data base failed!: ", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r Record

		//err = rows.Scan(&r.Date, &r.Mechant, &r.Amount, &r.Category)
		err = rows.Scan(&r.Date, &r.Mechant, &r.Amount)
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
