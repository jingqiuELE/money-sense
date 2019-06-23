package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	var historyPath = flag.String("d", "./", "path for history csv records.")
	var classifierPath = flag.String("c", "./", "path for classifier.")
	flag.Parse()

	ms, err := NewMoneySense(*historyPath, *classifierPath)
	if err != nil {
		log.Fatal("Could not initiate MoneySense!")
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

	pl := sortByPercentage(m)
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
