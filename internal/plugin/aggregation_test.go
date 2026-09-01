package plugin

import (
	"math"
	"testing"
)

func TestAggregate(t *testing.T) {
	tests := []struct {
		name       string
		aggregator string
		values     []float64
		want       float64
		wantErr    bool
	}{
		{name: "max", aggregator: "max", values: []float64{10, 20, 30}, want: 30},
		{name: "min", aggregator: "min", values: []float64{10, 20, 30}, want: 10},
		{name: "average", aggregator: "avg", values: []float64{10, 20, 30}, want: 20},
		{name: "sum", aggregator: "sum", values: []float64{10, 20, 30}, want: 60},
		{name: "count", aggregator: "count", values: []float64{10, 20, 30}, want: 3},
		{name: "latest", aggregator: "latest", values: []float64{10, 20, 30}, want: 30},
		{name: "empty", aggregator: "avg", wantErr: true},
		{name: "unsupported", aggregator: "median", values: []float64{10}, wantErr: true},
		{name: "NaN", aggregator: "avg", values: []float64{math.NaN()}, wantErr: true},
		{name: "positive infinity", aggregator: "max", values: []float64{math.Inf(1)}, wantErr: true},
		{name: "negative infinity", aggregator: "min", values: []float64{math.Inf(-1)}, wantErr: true},
		{name: "sum overflow", aggregator: "sum", values: []float64{math.MaxFloat64, math.MaxFloat64}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := aggregate(test.values, test.aggregator)
			if test.wantErr {
				if err == nil {
					t.Fatalf("aggregate(%v, %q) = %v, want error", test.values, test.aggregator, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("aggregate(%v, %q) = %v, want %v", test.values, test.aggregator, got, test.want)
			}
		})
	}
}
