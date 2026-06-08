package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/trade-bench/platform/pkg/telemetry"
)

// Bot represents a single simulated trading bot.
type Bot struct {
	ID           string
	SubmissionID string
	TargetURL    string
}

// Run simulates the bot's trading activity.
func (b *Bot) Run(ctx context.Context, metricsChan chan<- telemetry.MetricEvent) {
	ticker := time.NewTicker(time.Duration(100+rand.Intn(900)) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Simulate an order entry
			start := time.Now()
			orderID := uuid.New().String()
			side := telemetry.Buy
			if rand.Intn(2) == 0 {
				side = telemetry.Sell
			}
			price := 100.0 + rand.Float64()*10
			qty := 1.0 + float64(rand.Intn(10))

			// 1. Report the order we are about to send
			metricsChan <- telemetry.MetricEvent{
				SubmissionID: b.SubmissionID,
				BotID:        b.ID,
				Type:         telemetry.Correctness,
				Timestamp:    time.Now(),
				OrderData: &telemetry.OrderEvent{
					OrderID:    orderID,
					Side:       side,
					Price:      price,
					Quantity:   qty,
					IsResolved: false,
				},
			}
			
			// Simulate a small delay for network/processing
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

			// 2. Simulate the contestant's response
			success := rand.Float32() > 0.05 // 95% success rate for simulation
			latency := float64(time.Since(start).Milliseconds())
			
			var errMsg string
			if !success {
				errMsg = "simulated order rejection"
			}

			// Report latency
			metricsChan <- telemetry.MetricEvent{
				SubmissionID: b.SubmissionID,
				BotID:        b.ID,
				Type:         telemetry.Latency,
				Value:        latency,
				Timestamp:    time.Now(),
				Success:      success,
				ErrorMessage: errMsg,
			}
			
			// Report the resolution (for correctness validation)
			metricsChan <- telemetry.MetricEvent{
				SubmissionID: b.SubmissionID,
				BotID:        b.ID,
				Type:         telemetry.Correctness,
				Timestamp:    time.Now(),
				Success:      success,
				OrderData: &telemetry.OrderEvent{
					OrderID:    orderID,
					IsResolved: true,
					FillPrice:  price, // Simplified: assume fill at requested price
					FillQty:    qty,
				},
			}
			
			metricsChan <- telemetry.MetricEvent{
				SubmissionID: b.SubmissionID,
				BotID:        b.ID,
				Type:         telemetry.Throughput,
				Value:        1.0,
				Timestamp:    time.Now(),
				Success:      true,
			}
		}
	}
}

func startReporter(ctx context.Context, metricsChan <-chan telemetry.MetricEvent, ingesterURL string) {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-metricsChan:
			data, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal event: %v", err)
				continue
			}

			resp, err := client.Post(ingesterURL, "application/json", bytes.NewBuffer(data))
			if err != nil {
				// log.Printf("Failed to report metric: %v", err) // Too noisy for high volume
				continue
			}
			resp.Body.Close()
		}
	}
}

func main() {
	fmt.Println("Load Generator (Bot Fleet) Starting...")

	submissionID := uuid.New().String()
	botCount := 50 // Reduced for local prototype testing
	ingesterURL := "http://localhost:8081/ingest"
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	metricsChan := make(chan telemetry.MetricEvent, 5000)
	var wg sync.WaitGroup

	// Start reporters
	reporterCount := 5
	for i := 0; i < reporterCount; i++ {
		go startReporter(ctx, metricsChan, ingesterURL)
	}

	// Start bots
	for i := 0; i < botCount; i++ {
		wg.Add(1)
		bot := &Bot{
			ID:           fmt.Sprintf("bot-%d", i),
			SubmissionID: submissionID,
			TargetURL:    "http://contestant-submission:8080",
		}
		go func() {
			defer wg.Done()
			bot.Run(ctx, metricsChan)
		}()
	}

	fmt.Printf("Spawned %d bots for submission %s\n", botCount, submissionID)
	wg.Wait()
	close(metricsChan)
	fmt.Println("Load Generator finished.")
}
