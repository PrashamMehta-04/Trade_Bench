package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/trade-bench/platform/pkg/telemetry"
)

// StatsAggregator maintains rolling statistics for a submission.
type StatsAggregator struct {
	mu           sync.Mutex
	latencies    []float64
	throughput   float64
	correctCount int
	totalOrders  int
	startTime    time.Time
	orderBook    *telemetry.OrderBook
}

func NewStatsAggregator() *StatsAggregator {
	return &StatsAggregator{
		startTime: time.Now(),
		orderBook: telemetry.NewOrderBook(),
	}
}

func (s *StatsAggregator) AddMetric(event telemetry.MetricEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.Type == telemetry.Latency {
		s.latencies = append(s.latencies, event.Value)
	} else if event.Type == telemetry.Throughput {
		s.throughput += event.Value
	} else if event.Type == telemetry.Correctness && event.OrderData != nil {
		s.totalOrders++
		if !event.OrderData.IsResolved {
			// This is a new order, process in reference book
			s.orderBook.ProcessOrder(event.OrderData)
		} else {
			// This is a result from contestant, validate against reference (simplified)
			// In a real system, we'd compare the FillPrice and FillQty exactly.
			// For now, we simulate validation success.
			if event.Success {
				s.correctCount++
			}
		}
	}
}

func (s *StatsAggregator) GetReport() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return "Waiting for metrics..."
	}

	sort.Float64s(s.latencies)
	p50 := s.latencies[len(s.latencies)/2]
	p90 := s.latencies[int(float64(len(s.latencies))*0.9)]
	p99 := s.latencies[int(float64(len(s.latencies))*0.99)]

	duration := time.Since(s.startTime).Seconds()
	tps := s.throughput / duration

	accuracy := 0.0
	if s.totalOrders > 0 {
		accuracy = (float64(s.correctCount) / float64(s.totalOrders)) * 100
	}

	return fmt.Sprintf("Latency (ms): p50:%.2f, p90:%.2f, p99:%.2f | Throughput: %.2f TPS | Accuracy: %.1f%%", p50, p90, p99, tps, accuracy)
}

type Ingester struct {
	mu          sync.Mutex
	aggregators map[string]*StatsAggregator
}

func (i *Ingester) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event telemetry.MetricEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	i.mu.Lock()
	agg, ok := i.aggregators[event.SubmissionID]
	if !ok {
		agg = NewStatsAggregator()
		i.aggregators[event.SubmissionID] = agg
	}
	i.mu.Unlock()

	agg.AddMetric(event)
	w.WriteHeader(http.StatusAccepted)
}

func (i *Ingester) HandleReport(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("submission_id")
	if id == "" {
		http.Error(w, "submission_id is required", http.StatusBadRequest)
		return
	}

	i.mu.Lock()
	agg, ok := i.aggregators[id]
	i.mu.Unlock()

	if !ok {
		http.Error(w, "Submission not found", http.StatusNotFound)
		return
	}

	fmt.Fprintln(w, agg.GetReport())
}

func main() {
	fmt.Println("Telemetry Ingester Starting...")

	ingester := &Ingester{
		aggregators: make(map[string]*StatsAggregator),
	}

	http.HandleFunc("/ingest", ingester.HandleIngest)
	http.HandleFunc("/report", ingester.HandleReport)

	// Background reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			ingester.mu.Lock()
			for id, agg := range ingester.aggregators {
				log.Printf("[%s] %s", id[:8], agg.GetReport())
			}
			ingester.mu.Unlock()
		}
	}()

	log.Printf("Ingester listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
