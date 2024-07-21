package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	measurementsFile = "../../../../data/measurements.txt"
	workers          = runtime.NumCPU()
	batchSize        = 10000
	buffSize         = 4 * 1024 * 1024
	appPprof         = flag.Bool("pprof", false, "write cpu profile to `file`")
)

// StationData holds the aggregated data for a weather station
type StationData struct {
	Min   float64
	Max   float64
	Total float64
	Count int
}

func main() {
	flag.Parse()
	if *appPprof {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Use all available CPU cores
	runtime.GOMAXPROCS(workers)

	started := time.Now()
	linesCh := make(chan [][]byte, workers*4)
	data := sync.Map{}
	wg := sync.WaitGroup{}

	// Process file concurrently
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go ProcessLines(linesCh, &data, &wg)
	}

	wg.Add(1)
	go ReadFile(measurementsFile, linesCh, &wg)

	// Wait for all workers to finish
	wg.Wait()

	FormatOutput(&data)

	fmt.Printf("Elapsed time: %+v\n", time.Since(started))

	if *appPprof {
		f, err := os.Create("mem.pprof")
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

// ParseLine parses a line from the input file and returns the station name and temperature
func ParseLine(line []byte) (string, float64, error) {
	parts := bytes.Split(line, []byte(";"))
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid line format")
	}
	temperature, err := strconv.ParseFloat(string(parts[1]), 64)
	if err != nil {
		return "", 0, err
	}
	return string(parts[0]), temperature, nil
}

// ProcessLines processes lines concurrently and aggregates the data per weather station
func ProcessLines(lines <-chan [][]byte, data *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range lines {
		localData := make(map[string]*StationData)
		for _, line := range batch {
			station, temperature, err := ParseLine(line)
			if err != nil {
				slog.Error("ProcessLines", "ParseLine", err)
				continue
			}

			if sd, exists := localData[station]; exists {
				if temperature < sd.Min {
					sd.Min = temperature
				}
				if temperature > sd.Max {
					sd.Max = temperature
				}
				sd.Total += temperature
				sd.Count++
			} else {
				localData[station] = &StationData{
					Min:   temperature,
					Max:   temperature,
					Total: temperature,
					Count: 1,
				}
			}
		}

		for station, localSD := range localData {
			dataSD, _ := data.LoadOrStore(station, &StationData{
				Min:   localSD.Min,
				Max:   localSD.Max,
				Total: localSD.Total,
				Count: localSD.Count,
			})
			globalSD := dataSD.(*StationData)

			if localSD.Min < globalSD.Min {
				globalSD.Min = localSD.Min
			}
			if localSD.Max > globalSD.Max {
				globalSD.Max = localSD.Max
			}
			globalSD.Total += localSD.Total
			globalSD.Count += localSD.Count
		}
	}
}

// ReadFile reads the input file and sends lines to a channel for processing
func ReadFile(filename string, lines chan<- [][]byte, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(lines)

	file, err := os.Open(filename)
	if err != nil {
		slog.Error("ReadFile", "Open", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, buffSize)
	scanner := bufio.NewScanner(reader)
	buffer := make([][]byte, 0, batchSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		buffer = append(buffer, append([]byte(nil), line...))
		if len(buffer) >= batchSize {
			lines <- buffer
			buffer = make([][]byte, 0, batchSize)
		}
	}

	if len(buffer) > 0 {
		lines <- buffer
	}

	if err := scanner.Err(); err != nil {
		slog.Error("ReadFile", "scanner", err)
	}
}

// FormatOutput formats the aggregated data and prints it
func FormatOutput(stationData *sync.Map) {
	sb := strings.Builder{}
	sb.WriteString("{")
	first := true
	stationData.Range(func(station, data interface{}) bool {
		if first {
			first = false
		} else {
			sb.WriteString(", ")
		}
		sd := data.(*StationData)
		mean := sd.Total / float64(sd.Count)
		sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, sd.Min, mean, sd.Max))
		return true
	})
	sb.WriteString("}")

	fmt.Fprintln(os.Stdout, sb.String())
}
