package plugin

import (
	"fmt"
	"math"
)

func aggregate(values []float64, aggregator string) (float64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("query returned no data points")
	}
	if _, ok := supportedAggregators[aggregator]; !ok {
		return 0, fmt.Errorf("invalid aggregator: %q", aggregator)
	}

	minValue := values[0]
	maxValue := values[0]
	sum := 0.0
	for _, value := range values {
		if !isFinite(value) {
			return 0, fmt.Errorf("query returned a non-finite value")
		}
		minValue = math.Min(minValue, value)
		maxValue = math.Max(maxValue, value)
		sum += value
		if !isFinite(sum) {
			return 0, fmt.Errorf("aggregated sum is non-finite")
		}
	}

	switch aggregator {
	case "max":
		return maxValue, nil
	case "min":
		return minValue, nil
	case "avg":
		return sum / float64(len(values)), nil
	case "sum":
		return sum, nil
	case "count":
		return float64(len(values)), nil
	case "latest":
		return values[len(values)-1], nil
	default:
		return 0, fmt.Errorf("invalid aggregator: %q", aggregator)
	}
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
