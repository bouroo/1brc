package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var measurementsFile = "../../../../data/measurements.txt"
var workers = runtime.GOMAXPROCS(0) * 2
var batchSize = 1000

type Summary struct {
	Station       string
	Min, Max, Sum float64
	Count         uint
}

type KeyValueStore struct {
	store sync.Map
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{}
}

func (kvs *KeyValueStore) Set(key string, value Summary) {
	kvs.store.Store(key, value)
}

func (kvs *KeyValueStore) Get(key string) (Summary, error) {
	value, exists := kvs.store.Load(key)
	if !exists {
		return Summary{}, errors.New("key not found")
	}
	return value.(Summary), nil
}

func (kvs *KeyValueStore) PrintAll() {
	kvs.store.Range(func(key, value interface{}) bool {
		data := value.(Summary)
		fmt.Printf("%s, %f, %f, %f\n", key, data.Min, data.Sum/float64(data.Count), data.Max)
		return true
	})
}

func main() {
	started := time.Now()
	if err := processFile(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Elapsed time: %+v\n", time.Since(started))
}

func processFile() error {
	file, err := os.Open(measurementsFile)
	if err != nil {
		return err
	}
	defer file.Close()

	kvStore := NewKeyValueStore()

	scanner := bufio.NewScanner(file)
	ch := make(chan []Summary, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for summaries := range ch {
				for _, summary := range summaries {
					existingSummary, err := kvStore.Get(summary.Station)
					if err != nil {
						existingSummary = summary
					} else {
						existingSummary.Max = max(summary.Max, existingSummary.Max)
						existingSummary.Min = min(summary.Min, existingSummary.Min)
						existingSummary.Sum += summary.Sum
						existingSummary.Count += summary.Count
					}
					kvStore.Set(summary.Station, existingSummary)
				}
			}
		}()
	}

	var batch []Summary
	for scanner.Scan() {
		line := scanner.Text()
		summary, err := processLine(line)
		if err != nil {
			log.Println("Error processing line:", err)
			continue
		}
		batch = append(batch, summary)

		if len(batch) >= batchSize {
			ch <- batch
			batch = nil
		}
	}

	if len(batch) > 0 {
		ch <- batch
	}
	close(ch)
	wg.Wait()

	if err := scanner.Err(); err != nil {
		return err
	}

	kvStore.PrintAll()

	return nil
}

func processLine(line string) (Summary, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return Summary{}, fmt.Errorf("invalid line: %s", line)
	}

	station := parts[0]
	temperature, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return Summary{}, fmt.Errorf("error parsing temperature: %v", err)
	}

	return Summary{
		Station: station,
		Min:     temperature,
		Max:     temperature,
		Sum:     temperature,
		Count:   1,
	}, nil
}
