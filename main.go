package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// DEV_START
// Development stubs - these get automatically removed by the executor
var ScriptConfigJSON = `{
	"user_id": 12345,
	"batch_size": 15,
	"operation": "analyze",
	"debug": true
}`

var ScriptResult interface{}

// DEV_END

func main() {
	fmt.Println("🚀 Starting data processing script...")

	// LOCAL_TESTING_START
	// This block gets removed in production but helps during development
	if ScriptConfigJSON == "" {
		panic("ScriptConfigJSON not provided - this shouldn't happen in executor")
	}
	// LOCAL_TESTING_END

	fmt.Printf("Debug: Config length = %d\n", len(ScriptConfigJSON)) // DEBUG

	// Parse the injected configuration
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(ScriptConfigJSON), &config); err != nil {
		panic("Failed to parse config: " + err.Error())
	}

	// Extract configuration with defaults
	userID := getInt(config, "user_id", 0)
	batchSize := getInt(config, "batch_size", 10)
	operation := getString(config, "operation", "analyze")
	debug := getBool(config, "debug", false)

	fmt.Printf("📊 Processing for user %d with batch size %d\n", userID, batchSize)

	if debug { // This condition works in both dev and production
		fmt.Printf("Debug mode enabled\n") // DEBUG - this line gets removed
	}

	// Generate sample data
	rawData := generateSampleData()
	fmt.Printf("📥 Generated %d data points\n", len(rawData))

	// Process the data based on operation type
	var result map[string]interface{}
	switch operation {
	case "analyze":
		result = analyzeData(rawData, batchSize)
	case "transform":
		result = transformData(rawData, batchSize)
	case "aggregate":
		result = aggregateData(rawData, batchSize)
	default:
		result = analyzeData(rawData, batchSize)
	}

	// Add metadata to result
	result["user_id"] = userID
	result["operation"] = operation
	result["batch_size"] = batchSize
	result["processed_at"] = time.Now().Format(time.RFC3339)
	result["data_points"] = len(rawData)
	result["debug_mode"] = debug

	// Set the result for the executor to retrieve
	ScriptResult = result

	fmt.Printf("✅ Processing complete! Operation: %s, Results: %d items\n",
		operation, len(result))

	fmt.Printf("Debug: Final result keys = %d\n", len(result)) // DEBUG
}

// analyzeData performs statistical analysis on the data
func analyzeData(data []float64, batchSize int) map[string]interface{} {
	if len(data) == 0 {
		return map[string]interface{}{
			"error": "no data to analyze",
		}
	}

	// Sort data for percentile calculations
	sortedData := make([]float64, len(data))
	copy(sortedData, data)
	sort.Float64s(sortedData)

	// Calculate statistics
	sum := 0.0
	min := sortedData[0]
	max := sortedData[len(sortedData)-1]

	for _, value := range data {
		sum += value
	}

	mean := sum / float64(len(data))
	median := calculateMedian(sortedData)

	return map[string]interface{}{
		"type":          "analysis",
		"count":         len(data),
		"sum":           roundToTwo(sum),
		"mean":          roundToTwo(mean),
		"median":        roundToTwo(median),
		"min":           min,
		"max":           max,
		"range":         max - min,
		"percentile_25": sortedData[len(sortedData)/4],
		"percentile_75": sortedData[3*len(sortedData)/4],
	}
}

// transformData applies transformations to the data in batches
func transformData(data []float64, batchSize int) map[string]interface{} {
	var transformed []float64
	var batches []map[string]interface{}

	// Process in batches
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchResult := processBatch(batch, i/batchSize+1)
		batches = append(batches, batchResult)

		// Apply transformations: normalize to 0-100 scale
		for _, value := range batch {
			normalized := (value / 1000.0) * 100.0
			if normalized > 100 {
				normalized = 100
			}
			transformed = append(transformed, roundToTwo(normalized))
		}
	}

	return map[string]interface{}{
		"type":              "transformation",
		"original_count":    len(data),
		"transformed":       transformed,
		"batch_results":     batches,
		"batches_processed": len(batches),
	}
}

// aggregateData groups and summarizes the data
func aggregateData(data []float64, batchSize int) map[string]interface{} {
	ranges := map[string]int{
		"0-25":   0,
		"26-50":  0,
		"51-75":  0,
		"76-100": 0,
		"100+":   0,
	}

	for _, value := range data {
		switch {
		case value <= 25:
			ranges["0-25"]++
		case value <= 50:
			ranges["26-50"]++
		case value <= 75:
			ranges["51-75"]++
		case value <= 100:
			ranges["76-100"]++
		default:
			ranges["100+"]++
		}
	}

	total := len(data)
	percentages := make(map[string]float64)
	for key, count := range ranges {
		percentages[key] = roundToTwo((float64(count) / float64(total)) * 100.0)
	}

	return map[string]interface{}{
		"type":          "aggregation",
		"total_count":   total,
		"ranges":        ranges,
		"percentages":   percentages,
		"largest_group": findLargestGroup(ranges),
	}
}

// Helper functions
func processBatch(batch []float64, batchNum int) map[string]interface{} {
	sum := 0.0
	for _, value := range batch {
		sum += value
	}

	return map[string]interface{}{
		"batch_number": batchNum,
		"size":         len(batch),
		"sum":          roundToTwo(sum),
		"average":      roundToTwo(sum / float64(len(batch))),
	}
}

func calculateMedian(sortedData []float64) float64 {
	n := len(sortedData)
	if n%2 == 0 {
		return (sortedData[n/2-1] + sortedData[n/2]) / 2
	}
	return sortedData[n/2]
}

func findLargestGroup(ranges map[string]int) string {
	maxCount := 0
	maxKey := ""

	for key, count := range ranges {
		if count > maxCount {
			maxCount = count
			maxKey = key
		}
	}

	return maxKey
}

func roundToTwo(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}

func generateSampleData() []float64 {
	return []float64{
		23.5, 67.2, 45.8, 89.1, 12.3, 78.9, 34.6, 56.7, 91.2, 28.4,
		73.1, 41.9, 85.3, 19.7, 62.8, 37.5, 94.6, 52.3, 76.4, 29.8,
		68.7, 43.2, 87.9, 15.6, 59.4, 82.1, 38.7, 71.5, 26.3, 64.8,
		49.2, 93.7, 31.4, 75.9, 18.6, 57.3, 86.4, 42.8, 69.1, 35.7,
		81.2, 24.9, 66.5, 48.7, 92.3, 33.1, 74.6, 21.8, 58.9, 84.7,
	}
}

// Helper functions to safely extract values from config
func getInt(config map[string]interface{}, key string, defaultValue int) int {
	if value, exists := config[key]; exists {
		if floatVal, ok := value.(float64); ok {
			return int(floatVal)
		}
	}
	return defaultValue
}

func getString(config map[string]interface{}, key string, defaultValue string) string {
	if value, exists := config[key]; exists {
		if strVal, ok := value.(string); ok {
			return strVal
		}
	}
	return defaultValue
}

func getBool(config map[string]interface{}, key string, defaultValue bool) bool {
	if value, exists := config[key]; exists {
		if boolVal, ok := value.(bool); ok {
			return boolVal
		}
	}
	return defaultValue
}
