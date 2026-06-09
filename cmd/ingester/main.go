package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/trade-bench/platform/pkg/telemetry"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// StatsAggregator maintains rolling statistics for a submission.
type StatsAggregator struct {
	mu            sync.Mutex
	SubmissionID  string
	latencies     []float64
	throughput    float64
	correctCount  int
	totalOrders   int
	startTime     time.Time
	orderBook     *telemetry.OrderBook
	expectedFills map[string][]telemetry.FillEvent // OrderID -> list of expected fills
}

func NewStatsAggregator(id string) *StatsAggregator {
	return &StatsAggregator{
		SubmissionID:  id,
		startTime:     time.Now(),
		orderBook:     telemetry.NewOrderBook(),
		expectedFills: make(map[string][]telemetry.FillEvent),
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
		if !event.OrderData.IsResolved {
			// New order submitted. SME calculates expected fills.
			newFills := s.orderBook.ProcessOrder(event.OrderData)
			for _, fill := range newFills {
				s.expectedFills[fill.OrderID] = append(s.expectedFills[fill.OrderID], fill)
			}
		} else {
			// Contestant reported a fill.
			s.totalOrders++
			
			expected, ok := s.expectedFills[event.OrderData.OrderID]
			if !ok {
				// No expected fill for this order - might be an error or a rejected order
				if !event.Success {
					s.correctCount++ // Correctly rejected
				}
				return
			}

			// Simple validation: check if the reported fill price and quantity match ANY expected fill
			// In a more robust system, we would consume the expected fills.
			matched := false
			for i, exp := range expected {
				if exp.Price == event.OrderData.FillPrice && exp.Quantity == event.OrderData.FillQty {
					matched = true
					// Remove this expected fill as it's been "consumed"
					s.expectedFills[event.OrderData.OrderID] = append(expected[:i], expected[i+1:]...)
					break
				}
			}

			if matched {
				s.correctCount++
			} else {
				log.Printf("[Validation] Discrepancy for order %s: expected %+v, got price=%.2f qty=%.2f", 
					event.OrderData.OrderID, expected, event.OrderData.FillPrice, event.OrderData.FillQty)
			}
		}
	}
}

type LeaderboardEntry struct {
	SubmissionID string  `json:"submission_id"`
	P50Latency   float64 `json:"p50"`
	P99Latency   float64 `json:"p99"`
	TPS          float64 `json:"tps"`
	Accuracy     float64 `json:"accuracy"`
	Score        float64 `json:"score"`
}

func (s *StatsAggregator) ToEntry() LeaderboardEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	p50, p99 := 0.0, 0.0
	if len(s.latencies) > 0 {
		sort.Float64s(s.latencies)
		p50 = s.latencies[len(s.latencies)/2]
		p99 = s.latencies[int(float64(len(s.latencies))*0.99)]
	}

	duration := time.Since(s.startTime).Seconds()
	if duration <= 0 {
		duration = 1
	}
	tps := s.throughput / duration

	accuracy := 0.0
	if s.totalOrders > 0 {
		accuracy = (float64(s.correctCount) / float64(s.totalOrders)) * 100
	}

	// Composite Score Algorithm:
	// Higher TPS and Accuracy increase the score.
	// Higher Latency (p99) significantly penalizes the score.
	score := 0.0
	if p99 > 0 {
		// Example: (TPS * (Accuracy^2)) / log10(p99 + 1)
		// This rewards high throughput and extreme accuracy, while penalizing latency non-linearly.
		score = (tps * (accuracy * accuracy / 10000.0)) / (p99 / 10.0)
	}

	return LeaderboardEntry{
		SubmissionID: s.SubmissionID,
		P50Latency:   p50,
		P99Latency:   p99,
		TPS:          tps,
		Accuracy:     accuracy,
		Score:        score,
	}
}

func (s *StatsAggregator) GetReport() string {
	entry := s.ToEntry()
	return fmt.Sprintf("Latency (ms): p50:%.2f, p99:%.2f | Throughput: %.2f TPS | Accuracy: %.1f%%", entry.P50Latency, entry.P99Latency, entry.TPS, entry.Accuracy)
}

type Ingester struct {
	mu          sync.Mutex
	aggregators map[string]*StatsAggregator
	redisClient *redis.Client
	dbPool      *pgxpool.Pool
}

func (i *Ingester) persistToTimescale(event telemetry.MetricEvent) {
	if i.dbPool == nil {
		return
	}

	orderID := ""
	if event.OrderData != nil {
		orderID = event.OrderData.OrderID
	}

	_, err := i.dbPool.Exec(context.Background(),
		"INSERT INTO metrics (time, submission_id, bot_id, metric_type, value, success, error_message, order_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		event.Timestamp, event.SubmissionID, event.BotID, string(event.Type), event.Value, event.Success, event.ErrorMessage, orderID)
	
	if err != nil {
		// Log error but don't crash; we want to keep ingesting other metrics
		if rand.Float32() < 0.01 {
			log.Printf("Failed to persist to TimescaleDB: %v", err)
		}
	}
}

func (i *Ingester) updateRedis(agg *StatsAggregator) {
	if i.redisClient == nil {
		return
	}
	entry := agg.ToEntry()
	data, _ := json.Marshal(entry)
	i.redisClient.HSet(context.Background(), "leaderboard", entry.SubmissionID, data)
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
		agg = NewStatsAggregator(event.SubmissionID)
		i.aggregators[event.SubmissionID] = agg
	}
	i.mu.Unlock()

	agg.AddMetric(event)
	i.updateRedis(agg)
	i.persistToTimescale(event)
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

func (i *Ingester) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		i.mu.Lock()
		entries := make([]LeaderboardEntry, 0, len(i.aggregators))
		for _, agg := range i.aggregators {
			entries = append(entries, agg.ToEntry())
		}
		i.mu.Unlock()

		if err := conn.WriteJSON(entries); err != nil {
			break
		}
	}
}

func startNATSConsumer(ingester *Ingester, natsURL string) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Failed to connect to NATS: %v", err)
		return
	}

	_, err = nc.Subscribe("telemetry.metrics", func(m *nats.Msg) {
		var event telemetry.MetricEvent
		if err := json.Unmarshal(m.Data, &event); err != nil {
			log.Printf("Failed to unmarshal NATS message: %v", err)
			return
		}

		ingester.mu.Lock()
		agg, ok := ingester.aggregators[event.SubmissionID]
		if !ok {
			agg = NewStatsAggregator(event.SubmissionID)
			ingester.aggregators[event.SubmissionID] = agg
		}
		ingester.mu.Unlock()

		agg.AddMetric(event)
		ingester.updateRedis(agg)
		ingester.persistToTimescale(event)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to NATS: %v", err)
	}

	log.Printf("Consuming metrics from NATS subject 'telemetry.metrics'")
}

func main() {
	fmt.Println("Telemetry Ingester Starting...")

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	dbURL := os.Getenv("TIMESCALE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/trade_bench"
	}
	dbPool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Printf("Warning: Failed to create TimescaleDB pool: %v", err)
	} else {
		// Verify connection
		if err := dbPool.Ping(context.Background()); err != nil {
			log.Printf("Warning: Failed to connect to TimescaleDB (Ping failed): %v", err)
			dbPool = nil
		} else {
			log.Printf("Successfully connected to TimescaleDB at %s", dbURL)
		}
	}

	ingester := &Ingester{
		aggregators: make(map[string]*StatsAggregator),
		redisClient: rdb,
		dbPool:      dbPool,
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	go startNATSConsumer(ingester, natsURL)

	http.HandleFunc("/ingest", ingester.HandleIngest)
	http.HandleFunc("/report", ingester.HandleReport)
	http.HandleFunc("/ws", ingester.HandleWS)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			ingester.mu.Lock()
			for _, agg := range ingester.aggregators {
				log.Printf("[%s] %s", agg.SubmissionID[:8], agg.GetReport())
			}
			ingester.mu.Unlock()
		}
	}()

	log.Printf("Ingester listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
