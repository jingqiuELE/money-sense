package main

import (
	"fmt"
	"image/color"
	"log"
	"math/rand"

	"github.com/benoitmasson/plotters/piechart"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
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

func plotLinePointsHistory(history map[string][]Record) error {
	p, err := plot.New()
	if err != nil {
		log.Panic(err)
	}
	// xticks defines how we convert and display time.Time values.
	xticks := plot.TimeTicks{Format: TimeFormat}
	p.Title.Text = "Spending History"
	p.X.Tick.Marker = xticks
	p.X.Label.Text = "Date"
	p.Y.Label.Text = "Amount"
	p.Add(plotter.NewGrid())
	p.Legend.Top = true

	for category, records := range history {
		var pts plotter.XYs
		for _, r := range records {
			point := plotter.XY{
				X: float64(r.Date.Unix()),
				Y: r.Amount,
			}
			pts = append(pts, point)
		}
		lpLine, lpPoints, err := plotter.NewLinePoints(pts)
		if err != nil {
			log.Panic(err)
		}
		lpLine.Color = color.RGBA{
			R: uint8(rand.Intn(255)),
			G: uint8(rand.Intn(255)),
			B: uint8(rand.Intn(255)),
			A: 255,
		}
		lpPoints.Shape = draw.CrossGlyph{}
		lpPoints.Color = lpLine.Color
		p.Add(lpLine, lpPoints)
		p.Legend.Add(category, lpLine, lpPoints)
	}

	err = p.Save(1000, 1000, "./graph/plotLinePointsHistory.png")
	if err != nil {
		log.Panic(err)
	}
	return nil
}

func plotBarChartHistory(history map[string][]Record) error {
	p, err := plot.New()
	if err != nil {
		log.Fatal(err)
		return err
	}
	// xticks defines how we convert and display time.Time values.
	xticks := plot.TimeTicks{Format: TimeFormat}
	p.Title.Text = "Spending History"
	p.X.Tick.Marker = xticks
	p.X.Label.Text = "Date"
	p.Y.Label.Text = "Amount"
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	w := vg.Points(10)
	var pBars *plotter.BarChart
	var xnames []string
	for category, records := range history {
		var values plotter.Values
		xnames = nil
		for _, r := range records {
			values = append(values, r.Amount)
			xnames = append(xnames, r.Date.Format(TimeFormat))
		}
		bars, err := plotter.NewBarChart(values, w)
		if err != nil {
			log.Fatal(err)
		}
		bars.LineStyle.Width = vg.Length(0)
		bars.Color = color.RGBA{
			R: uint8(rand.Intn(255)),
			G: uint8(rand.Intn(255)),
			B: uint8(rand.Intn(255)),
			A: 255,
		}
		if pBars != nil {
			bars.StackOn(pBars)
		}
		p.Add(bars)
		p.Legend.Add(category, bars)
		p.NominalX(xnames...)
		pBars = bars
	}
	err = p.Save(vg.Length(len(xnames))*vg.Inch, 1000, "./graph/plotBarChartHistory.png")
	if err != nil {
		log.Panic(err)
	}
	return nil
}
