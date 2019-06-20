package main

import (
	"log"
	"strconv"
	"time"
)

type Record struct {
	Date     time.Time
	Mechant  string
	Amount   float64
	Category string
}

func NewRecord(date string, format string, mechant string, amount string, category string) Record {
	var record Record
	dt, err := time.Parse(format, date)
	if err != nil {
		log.Fatal("Failed to parse date!")
	}

	a, err := strconv.ParseFloat(amount, 64)
	if err != nil {
		log.Fatal("Failed to parse amount!", err)
	}
	record = Record{dt, mechant, a, category}
	return record
}
