package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type TimeUnit uint8

const (
	ByDate = TimeUnit(iota)
	ByWeek
	ByMonth
)

func main() {
	var historyPath = flag.String("d", "./", "path for history csv records.")
	var classifierPath = flag.String("c", "./", "path for classifier.")
	flag.Parse()

	ms, err := NewMoneySense(*historyPath, *classifierPath)
	if err != nil {
		log.Fatal("Could not initiate MoneySense!")
	}

	err = ms.Classify()
	if err != nil {
		log.Fatal("Could not classify records!", err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("$ ")
		cmdString, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		err = runCommand(cmdString, ms)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

func runCommand(commandStr string, ms *MoneySense) error {
	commandStr = strings.TrimSuffix(commandStr, "\n")
	if len(commandStr) == 0 {
		return nil
	}
	arrCommandStr := strings.Fields(commandStr)
	switch arrCommandStr[0] {
	case "exit":
		os.Exit(0)
	case "pc":
		if len(arrCommandStr) < 3 {
			return errors.New("Require 2 arguments specifying date range.")
		}
		printCategoryPercentage(arrCommandStr[1], arrCommandStr[2], ms)
	case "hd":
		if len(arrCommandStr) < 4 {
			return errors.New("Require 3 arguments specifying category and date range.")
		}
		printHistory(arrCommandStr[1], arrCommandStr[2], arrCommandStr[3], ms, ByDate)
	case "hw":
		if len(arrCommandStr) < 4 {
			return errors.New("Require 3 arguments specifying category and date range.")
		}
		printHistory(arrCommandStr[1], arrCommandStr[2], arrCommandStr[3], ms, ByWeek)
	case "hm":
		if len(arrCommandStr) < 4 {
			return errors.New("Require 3 arguments specifying category and date range.")
		}
		printHistory(arrCommandStr[1], arrCommandStr[2], arrCommandStr[3], ms, ByMonth)
	}
	return nil
}

func printCategoryPercentage(start string, end string, ms *MoneySense) error {
	var total float64

	var m = make(map[string]float64)

	records := ms.Retrieve("*", start, end)
	for _, r := range records {
		m[r.Category] += r.Amount
	}
	err := PlotPieByCategory(m)
	if err != nil {
		return err
	}

	pl := sortMapByValue(m)
	fmt.Printf("|%-16s|%-16s|%-16s\n", "Category", "Percentage", "Amount")
	fmt.Println("-----------------------------------------------")
	for _, amount := range m {
		total += amount
	}
	for _, p := range pl {
		fmt.Printf("|%-16v|%%%-15.2f|$%-16.2f\n", p.Key, (p.Value/total)*100, p.Value)
	}
	return nil
}

func printHistory(category string, start string, end string, ms *MoneySense, unit TimeUnit) error {
	var m = make(map[string][]Record)
	records := ms.Retrieve(category, start, end)
	for _, r := range records {
		m[r.Category] = append(m[r.Category], r)
	}

	switch unit {
	case ByDate:
	case ByWeek:
		for category, records := range m {
			m[category] = mergeRecordsByWeek(records)
		}
	case ByMonth:
		for category, records := range m {
			m[category] = mergeRecordsByMonth(records)
		}
	}
	err := plotLinePointsHistory(m)
	return err
}

func mergeRecordsByWeek(records []Record) []Record {
	var year, week, pYear, pWeek int
	var result []Record
	for _, r := range records {
		year, week = r.Date.ISOWeek()
		len := len(result)
		if year == pYear && week == pWeek {
			result[len-1].Amount += r.Amount
		} else {
			result = append(result, r)
			pYear = year
			pWeek = week
		}
	}
	return result
}

func mergeRecordsByMonth(records []Record) []Record {
	var year, pYear int
	var month, pMonth time.Month
	var result []Record

	for _, r := range records {
		year = r.Date.Year()
		month = r.Date.Month()
		len := len(result)
		if year == pYear && month == pMonth {
			result[len-1].Amount += r.Amount
		} else {
			result = append(result, r)
			pYear = year
			pMonth = month
		}
	}
	return result
}
