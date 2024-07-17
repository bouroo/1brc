package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "go.uber.org/automaxprocs"
)

var (
	measurementsFile = "../../../../data/measurements.txt"
	workers          = runtime.NumCPU() // Number of goroutines for processing lines
)

type StationSummary struct {
	Min, Max, Sum, Count float64
	mu                   sync.Mutex
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
	file, err := os.Open(measurementsFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	results := map[string]*StationSummary{}
	var mu sync.Mutex

	lineCh := make(chan string, 1000) // Adjust buffer size as needed
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lineCh {
				processLine(line, results, &mu)
			}
		}()
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineCh <- scanner.Text()
	}
	close(lineCh)

	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	// Print final results
	for station, summary := range results {
		mean := summary.Sum / summary.Count
		fmt.Printf("%s, %.1f, %.1f, %.1f\n", station, summary.Min, mean, summary.Max)
	}

	return
}

func processLine(line string, results map[string]*StationSummary, mu *sync.Mutex) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		log.Panic("Invalid line:", line)
		return
	}

	station := parts[0]
	temperature, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		log.Println("Error parsing temperature:", err)
		return
	}

	mu.Lock()
	summary, ok := results[station]
	if !ok {
		summary = &StationSummary{Min: temperature, Max: temperature, Sum: temperature, Count: 1}
		results[station] = summary
	} else {
		summary.mu.Lock()
		summary.Min = math.Min(summary.Min, temperature)
		summary.Max = math.Max(summary.Max, temperature)
		summary.Sum += temperature
		summary.Count++
		summary.mu.Unlock()
	}
	mu.Unlock()
}
