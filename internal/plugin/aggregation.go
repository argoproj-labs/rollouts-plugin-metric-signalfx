package plugin

import (
	"fmt"
	"math"
)

func aggregate(values []float64, aggregator string) (float64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("query returned no data points")
	}

	state, err := newAggregationState(aggregator)
	if err != nil {
		return 0, err
	}
	for _, value := range values {
		if err := state.add(value); err != nil {
			return 0, err
		}
	}
	return state.result()
}

type aggregationState struct {
	aggregator string
	count      int
	minValue   float64
	maxValue   float64
	mean       float64
	sum        float64
	latest     float64
}

func newAggregationState(aggregator string) (*aggregationState, error) {
	if _, ok := supportedAggregators[aggregator]; !ok {
		return nil, fmt.Errorf("invalid aggregator: %q", aggregator)
	}
	return &aggregationState{aggregator: aggregator}, nil
}

func (s *aggregationState) add(value float64) error {
	if !isFinite(value) {
		return fmt.Errorf("query returned a non-finite value")
	}
	if s.count == 0 {
		s.count = 1
		s.minValue = value
		s.maxValue = value
		s.mean = value
		s.sum = value
		s.latest = value
		return nil
	}

	switch s.aggregator {
	case "max":
		s.maxValue = math.Max(s.maxValue, value)
	case "min":
		s.minValue = math.Min(s.minValue, value)
	case "avg":
		nextCount := float64(s.count + 1)
		nextMean := (s.mean / nextCount * float64(s.count)) + value/nextCount
		if !isFinite(nextMean) {
			return fmt.Errorf("aggregated average is non-finite")
		}
		s.mean = nextMean
	case "sum":
		nextSum := s.sum + value
		if !isFinite(nextSum) {
			return fmt.Errorf("aggregated sum is non-finite")
		}
		s.sum = nextSum
	case "count":
	case "latest":
		s.latest = value
	default:
		return fmt.Errorf("invalid aggregator: %q", s.aggregator)
	}
	s.count++
	return nil
}

func (s *aggregationState) result() (float64, error) {
	if s.count == 0 {
		return 0, fmt.Errorf("query returned no data points")
	}

	switch s.aggregator {
	case "max":
		return s.maxValue, nil
	case "min":
		return s.minValue, nil
	case "avg":
		return s.mean, nil
	case "sum":
		return s.sum, nil
	case "count":
		return float64(s.count), nil
	case "latest":
		return s.latest, nil
	default:
		return 0, fmt.Errorf("invalid aggregator: %q", s.aggregator)
	}
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
