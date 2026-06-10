package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/trade-bench/platform/pkg/k8s"
	"github.com/trade-bench/platform/pkg/telemetry"
)

type SubmissionState string

const (
	Pending   SubmissionState = "PENDING"
	Running   SubmissionState = "RUNNING"
	Completed SubmissionState = "COMPLETED"
	Failed    SubmissionState = "FAILED"
)

type Submission struct {
	ID        string
	Name      string
	State     SubmissionState
	CreatedAt time.Time
}

type Orchestrator struct {
	mu          sync.Mutex
	submissions map[string]*Submission
	k8sManager  *k8s.Manager
	ingesterURL string
	natsURL     string
}

func NewOrchestrator(k8sMan *k8s.Manager, ingesterURL, natsURL string) *Orchestrator {
	return &Orchestrator{
		submissions: make(map[string]*Submission),
		k8sManager:  k8sMan,
		ingesterURL: ingesterURL,
		natsURL:     natsURL,
	}
}

func (o *Orchestrator) HandleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := r.URL.Query().Get("name")
	image := r.URL.Query().Get("image")
	if name == "" {
		name = "default-submission"
	}
	if image == "" {
		image = "nginx:alpine" // Default for testing
	}

	sub := &Submission{
		ID:        uuid.New().String(),
		Name:      name,
		State:     Pending,
		CreatedAt: time.Now(),
	}

	o.mu.Lock()
	o.submissions[sub.ID] = sub
	o.mu.Unlock()

	log.Printf("Received submission: %s (%s)", sub.Name, sub.ID)

	// Start the benchmark process
	go o.runBenchmark(sub.ID, image)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"submission_id\": \"%s\", \"message\": \"Benchmark started successfully\", \"status\": \"PENDING\"}\n", sub.ID)
}

func (o *Orchestrator) HandleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("submission_id")
	if id == "" {
		http.Error(w, "submission_id is required", http.StatusBadRequest)
		return
	}

	o.mu.Lock()
	sub, ok := o.submissions[id]
	o.mu.Unlock()

	if !ok {
		http.Error(w, "Submission not found", http.StatusNotFound)
		return
	}

	// Query Ingester for latest metrics
	resp, err := http.Get(fmt.Sprintf("%s/report?submission_id=%s", o.ingesterURL, id))
	metrics := "No metrics available yet."
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		metrics = string(body)
	}

	fmt.Fprintf(w, "ID: %s\nName: %s\nState: %s\nMetrics: %s\n", sub.ID, sub.Name, sub.State, metrics)
}

func (o *Orchestrator) runBenchmark(id, image string) {
	o.mu.Lock()
	sub, ok := o.submissions[id]
	if !ok {
		o.mu.Unlock()
		return
	}
	sub.State = Running
	o.mu.Unlock()

	log.Printf("Starting benchmark for %s...", id)

	if o.k8sManager != nil {
		ctx := context.Background()
		
		// 1. Setup isolation
		err := o.k8sManager.CreateNetworkPolicy(ctx, "default", id)
		if err != nil {
			log.Printf("Network Policy creation failed: %v", err)
		}

		// 2. Deploy the contestant's submission
		err = o.k8sManager.DeploySubmission(ctx, "default", id, image)
		if err != nil {
			log.Printf("K8s Submission Deployment failed: %v", err)
		}

		// 2. Wait for submission to be ready (simplified for prototype)
		time.Sleep(10 * time.Second)

		// 3. Deploy the Load Generator Fleet
		err = o.k8sManager.DeployLoadGenerator(ctx, "default", id, o.natsURL, 100)
		if err != nil {
			log.Printf("K8s Load Generator Deployment failed: %v", err)
		}
		
		// Real K8s run duration
		time.Sleep(60 * time.Second)
	} else {
		// LOCAL SIMULATION MODE (for Demo purposes when K8s is not available)
		log.Printf("[LOCAL MODE] Starting metric simulator for %s", id)
		o.runLocalSimulation(id)
	}

	o.mu.Lock()
	sub.State = Completed
	o.mu.Unlock()

	log.Printf("Benchmark completed for %s", id)
}

func (o *Orchestrator) runLocalSimulation(id string) {
	nc, err := nats.Connect(o.natsURL)
	if err != nil {
		log.Printf("Failed to connect to NATS for simulation: %v", err)
		return
	}
	defer nc.Close()

	stop := time.After(60 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Emit 5-10 random events per tick
			count := 5 + rand.Intn(5)
			for i := 0; i < count; i++ {
				// Random Latency (0.1ms to 5ms)
				lat := 0.1 + rand.Float64()*4.9
				o.publishMetric(nc, id, telemetry.Latency, lat)
				
				// Random Throughput
				o.publishMetric(nc, id, telemetry.Throughput, 1.0)
				
				// Random Correctness (95-100% accurate)
				acc := 1.0
				if rand.Float32() < 0.02 {
					acc = 0.0 // 2% chance of error
				}
				o.publishMetric(nc, id, telemetry.Correctness, acc)
			}
		}
	}
}

func (o *Orchestrator) publishMetric(nc *nats.Conn, subID string, mType telemetry.MetricType, value float64) {
	event := telemetry.MetricEvent{
		SubmissionID: subID,
		BotID:        "sim-bot-1",
		Type:         mType,
		Value:        value,
		Timestamp:    time.Now(),
		Success:      true,
	}
	// For correctness, we need a "resolved" order for the scoring logic
	if mType == telemetry.Correctness {
		event.OrderData = &telemetry.OrderEvent{
			OrderID:    uuid.New().String(),
			IsResolved: true,
			FillPrice:  100.0,
			FillQty:    1.0,
		}
		if value == 1.0 {
			event.Success = true
		} else {
			event.Success = false
		}
	}

	data, _ := json.Marshal(event)
	nc.Publish("telemetry.metrics", data)
}

func main() {
	fmt.Println("Orchestrator Service Starting...")

	k8sMan, err := k8s.NewManager()
	if err != nil {
		log.Printf("Warning: Kubernetes manager failed to initialize: %v (running in local mode)", err)
	}

	ingesterURL := os.Getenv("INGESTER_URL")
	if ingesterURL == "" {
		ingesterURL = "http://localhost:8081"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	orch := NewOrchestrator(k8sMan, ingesterURL, natsURL)
	http.HandleFunc("/submit", orch.HandleSubmit)
	http.HandleFunc("/status", orch.HandleStatus)

	// Serve the static dashboard
	fs := http.FileServer(http.Dir("tools/dashboard"))
	http.Handle("/dashboard/", http.StripPrefix("/dashboard/", fs))

	log.Printf("Orchestrator listening on :8080")
	log.Printf("Dashboard available at http://localhost:8080/dashboard/")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
