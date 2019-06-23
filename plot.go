package main

import (
	"fmt"
	"image/color"
	"log"
	"math/rand"

	"github.com/benoitmasson/plotters/piechart"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
)

func PlotPieByCategory(data map[string]float64) error {
	var total float64

	p, err := plot.New()
	if err != nil {
		panic(err)

	}
	p.HideAxes()

	for _, amount := range data {
		total += amount
	}

	var offset float64
	// Setup pie chart
	for category, amount := range data {
		fmt.Println("Plotting", category, amount)
		pie, err := piechart.NewPieChart(plotter.Values{amount})
		if err != nil {
			log.Fatal("Failed to plot:", err)
		}
		pie.Total = total
		pie.Offset.Value = offset
		pie.Labels.Nominal = []string{category}
		pie.Labels.Values.Show = true
		//pie.Labels.Values.Percentage = true
		pie.Color = color.RGBA{uint8(rand.Intn(255)), uint8(rand.Intn(255)), uint8(rand.Intn(255)), 255}
		p.Add(pie)
		p.Legend.Add(category, pie)
		offset += amount
	}
	err = p.Save(600, 600, "./graph/PieByCategory.png")
	if err != nil {
		log.Fatal("Failed to generate png output!", err)
	}
	return err
}
