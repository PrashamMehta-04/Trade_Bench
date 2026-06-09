package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
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

			// 1. Prepare and report the order we are about to send
			event := telemetry.MetricEvent{
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
			metricsChan <- event

			// 2. Send actual order to the contestant submission
			orderPayload, _ := json.Marshal(event.OrderData)
			resp, err := http.Post(b.TargetURL+"/order", "application/json", bytes.NewBuffer(orderPayload))
			
			latency := float64(time.Since(start).Milliseconds())
			success := err == nil && resp != nil && resp.StatusCode == http.StatusOK
			
			var errMsg string
			var fillPrice, fillQty float64
			if err != nil {
				errMsg = err.Error()
			} else if resp.StatusCode != http.StatusOK {
				errMsg = fmt.Sprintf("HTTP %d", resp.StatusCode)
			} else {
				// Try to decode fill info from contestant response
				var resolution telemetry.OrderEvent
				if err := json.NewDecoder(resp.Body).Decode(&resolution); err == nil {
					fillPrice = resolution.FillPrice
					fillQty = resolution.FillQty
				}
				resp.Body.Close()
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
					FillPrice:  fillPrice,
					FillQty:    fillQty,
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

func startReporter(ctx context.Context, metricsChan <-chan telemetry.MetricEvent, natsURL string) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("Failed to connect to NATS: %v", err)
		return
	}
	defer nc.Close()

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

			if err := nc.Publish("telemetry.metrics", data); err != nil {
				log.Printf("Failed to publish to NATS: %v", err)
			} else {
				// Periodically log successful publishes to avoid spam
				if rand.Float32() < 0.01 {
					log.Printf("Successfully published a metric to NATS")
				}
			}
		}
	}
}

func main() {
	fmt.Println("Load Generator (Bot Fleet) Starting...")

	// Configuration via Environment Variables or Flags
	submissionID := os.Getenv("SUBMISSION_ID")
	if submissionID == "" {
		submissionID = uuid.New().String()
	}

	botCountStr := os.Getenv("BOT_COUNT")
	botCount := 50
	if botCountStr != "" {
		fmt.Sscanf(botCountStr, "%d", &botCount)
	}

	targetURL := os.Getenv("TARGET_URL")
	if targetURL == "" {
		targetURL = "http://contestant-submission:8080"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	durationStr := os.Getenv("DURATION")
	duration := 1 * time.Hour
	if durationStr != "" {
		d, err := time.ParseDuration(durationStr)
		if err == nil {
			duration = d
		}
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	metricsChan := make(chan telemetry.MetricEvent, 10000)
	var wg sync.WaitGroup

	// Start reporters
	reporterCount := 5
	for i := 0; i < reporterCount; i++ {
		go startReporter(ctx, metricsChan, natsURL)
	}

	// Start bots
	for i := 0; i < botCount; i++ {
		wg.Add(1)
		bot := &Bot{
			ID:           fmt.Sprintf("bot-%d", i),
			SubmissionID: submissionID,
			TargetURL:    targetURL,
		}
		go func() {
			defer wg.Done()
			bot.Run(ctx, metricsChan)
		}()
	}

	fmt.Printf("Spawned %d bots for submission %s targeting %s\n", botCount, submissionID, targetURL)
	wg.Wait()
	close(metricsChan)
	fmt.Println("Load Generator finished.")
}
