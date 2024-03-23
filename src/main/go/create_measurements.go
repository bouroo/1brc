package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

func checkArgs(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: create_measurements <positive integer number of records to create>")
		fmt.Println("        You can use underscore notation for large number of records.")
		fmt.Println("        For example: 1_000_000_000 for one billion")
		os.Exit(1)
	}
	_, err := strconv.Atoi(strings.ReplaceAll(args[1], "_", ""))
	if err != nil {
		fmt.Println("Usage: create_measurements <positive integer number of records to create>")
		fmt.Println("        You can use underscore notation for large number of records.")
		fmt.Println("        For example: 1_000_000_000 for one billion")
		os.Exit(1)
	}
}

func buildWeatherStationNameList() []string {
	stationNames := make([]string, 0)
	file, err := os.Open("../../../data/weather_stations.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		station := scanner.Text()
		if !strings.Contains(station, "#") {
			stationNames = append(stationNames, strings.Split(station, ";")[0])
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error scanning file:", err)
		os.Exit(1)
	}

	stationNamesMap := make(map[string]bool)
	uniqueNames := make([]string, 0)
	for _, name := range stationNames {
		if _, value := stationNamesMap[name]; !value {
			stationNamesMap[name] = true
			uniqueNames = append(uniqueNames, name)
		}
	}
	return uniqueNames
}

func convertBytes(num float64) string {
	suffixes := []string{"bytes", "KiB", "MiB", "GiB"}
	var x string
	for _, x = range suffixes {
		if num < 1024.0 {
			break
		}
		num /= 1024.0
	}
	return fmt.Sprintf("%.1f %s", num, x)
}

func formatElapsedTime(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%.3f seconds", seconds)
	} else if seconds < 3600 {
		minutes := int(seconds / 60)
		seconds = math.Mod(seconds, 60)
		return fmt.Sprintf("%d minutes %.0f seconds", minutes, seconds)
	} else {
		hours := int(seconds / 3600)
		remainder := math.Mod(seconds, 3600)
		minutes := int(remainder / 60)
		seconds = math.Mod(remainder, 60)
		return fmt.Sprintf("%d hours %d minutes %.0f seconds", hours, minutes, seconds)
	}
}

func estimateFileSize(weatherStationNames []string, numRowsToCreate int) string {
	var totalNameBytes float64
	for _, s := range weatherStationNames {
		totalNameBytes += float64(len([]byte(s)))
	}
	avgNameBytes := totalNameBytes / float64(len(weatherStationNames))
	avgTempBytes := 4.400200100050025 // Average bytes for temperature

	avgLineLength := avgNameBytes + avgTempBytes + 2 // add 2 for separator and newline
	humanFileSize := convertBytes(float64(numRowsToCreate) * avgLineLength)
	return "Estimated max file size is: " + humanFileSize + "."
}

func buildTestData(weatherStationNames []string, numRowsToCreate int) {
	startTime := time.Now()
	coldestTemp := -99.9
	hottestTemp := 99.9
	stationNames10kMax := make([]string, 0)
	for i := 0; i < 10000; i++ {
		stationNames10kMax = append(stationNames10kMax, weatherStationNames[rand.IntN(len(weatherStationNames))])
	}

	batchSize := 10000
	if numRowsToCreate < batchSize {
		batchSize = numRowsToCreate
	}
	chunks := numRowsToCreate / batchSize
	progressInterval := chunks / 100 // Update progress approximately every 1%

	fmt.Println("Building test data...")

	file, err := os.Create("../../../data/measurements.txt")
	if err != nil {
		log.Panicln("Error creating file:", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for chunk := 0; chunk < chunks; chunk++ {
		for i := 0; i < batchSize; i++ {
			row := fmt.Sprintf("%s;%.1f\n", stationNames10kMax[rand.IntN(len(stationNames10kMax))], rand.Float64()*(hottestTemp-coldestTemp)+coldestTemp)
			_, err := writer.WriteString(row)
			if err != nil {
				log.Panicln("Error writing to file:", err)
			}
		}

		// Print progress
		if  progressInterval > 0 && chunk%progressInterval == 0 {
			progress := (chunk * 100) / chunks
			bars := strings.Repeat("=", progress/2)
			fmt.Printf("\r[%-50s] %d%%", bars, progress)
		}
	}

	// Process the remaining rows (if numRowsToCreate is not perfectly divisible by batchSize)
	remainingRows := numRowsToCreate % batchSize
	for i := 0; i < remainingRows; i++ {
		row := fmt.Sprintf("%s;%.1f\n", stationNames10kMax[rand.IntN(len(stationNames10kMax))], rand.Float64()*(hottestTemp-coldestTemp)+coldestTemp)
		_, err := writer.WriteString(row)
		if err != nil {
			log.Panicln("Error writing to file:", err)
		}
	}

	elapsedTime := time.Since(startTime).Seconds()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Panicln("Error getting file info:", err)
	}
	fileSize := fileInfo.Size()
	humanFileSize := convertBytes(float64(fileSize))

	fmt.Println("\nTest data successfully written to 1brc/data/measurements.txt")
	fmt.Println("Actual file size:", humanFileSize)
	fmt.Println("Elapsed time:", formatElapsedTime(elapsedTime))
}

func main() {
	args := os.Args
	checkArgs(args)
	numRowsToCreate, err := strconv.Atoi(strings.ReplaceAll(args[1], "_", ""))
	if err != nil {
		log.Panicln("Error parsing integer:", err)
	}
	weatherStationNames := buildWeatherStationNameList()
	fmt.Println(estimateFileSize(weatherStationNames, numRowsToCreate))
	buildTestData(weatherStationNames, numRowsToCreate)
	fmt.Println("Test data build complete.")
}
