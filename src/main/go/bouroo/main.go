package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	messurementsFile = "../../../../data/measurements.txt"
	workers          = runtime.NumCPU() // Number of goroutines for processing lines
	batchSize        = 1024
)

type StationSummary struct {
	Min, Max, Sum float64
	Count         int
}

func main() {
	started := time.Now()
	err := run()
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("Elapsed time: %+v\n", time.Since(started))
}

func run() (err error) {

	file, err := os.Open(messurementsFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	linesChan := make(chan []string, workers)
	results := make(chan map[string]StationSummary, workers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		go func() {
			for batch := range linesChan {
				processBatch(batch, results, &wg)
			}
		}()
	}

	go func() {
		scanner := bufio.NewScanner(file)
		lines := make([]string, 0, batchSize)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
			if len(lines) >= batchSize {
				wg.Add(1)
				linesChan <- lines
				lines = make([]string, 0, batchSize)
			}
		}
		if len(lines) > 0 {
			wg.Add(1)
			linesChan <- lines
		}
		close(linesChan)
	}()

	// Wait for all workers to finish processing
	go func() {
		wg.Wait()
		close(results)
	}()

	finalResults := make(map[string]StationSummary)
	var aggWg sync.WaitGroup

	// Aggregate results
	aggWg.Add(1)
	go aggregateResults(results, finalResults, &aggWg)

	aggWg.Wait()

	// Print final results
	for station, summary := range finalResults {
		mean := summary.Sum / float64(summary.Count)
		fmt.Printf("%s, %.1f, %.1f, %.1f\n", station, summary.Min, mean, summary.Max)
	}

	return
}

func processBatch(lines []string, results chan<- map[string]StationSummary, wg *sync.WaitGroup) {
	defer wg.Done()
	stationData := make(map[string]StationSummary)

	for _, line := range lines {
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			continue
		}

		station := parts[0]
		tempStr := parts[1]
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			continue
		}

		summary, exists := stationData[station]
		if !exists {
			summary = StationSummary{Min: temp, Max: temp, Sum: temp, Count: 1}
		} else {
			if temp < summary.Min {
				summary.Min = temp
			}
			if temp > summary.Max {
				summary.Max = temp
			}
			summary.Sum += temp
			summary.Count++
		}
		stationData[station] = summary
	}

	results <- stationData
}

func aggregateResults(results <-chan map[string]StationSummary, finalResults map[string]StationSummary, wg *sync.WaitGroup) {
	defer wg.Done()

	for stationData := range results {
		for station, summary := range stationData {
			finalSummary, exists := finalResults[station]
			if !exists {
				finalResults[station] = summary
			} else {
				if summary.Min < finalSummary.Min {
					finalSummary.Min = summary.Min
				}
				if summary.Max > finalSummary.Max {
					finalSummary.Max = summary.Max
				}
				finalSummary.Sum += summary.Sum
				finalSummary.Count += summary.Count
				finalResults[station] = finalSummary
			}
		}
	}
}
