package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/trade-bench/platform/pkg/k8s"
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
}

func NewOrchestrator(k8sMan *k8s.Manager, ingesterURL string) *Orchestrator {
	return &Orchestrator{
		submissions: make(map[string]*Submission),
		k8sManager:  k8sMan,
		ingesterURL: ingesterURL,
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

	fmt.Fprintf(w, "Submission received: %s\n", sub.ID)
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
		err := o.k8sManager.DeploySubmission(ctx, "default", id, image)
		if err != nil {
			log.Printf("K8s Deployment failed: %v", err)
			// For prototype, we continue anyway
		}
	}

	// In a real scenario, we'd wait for the container to be ready, then trigger the Load Generator.
	time.Sleep(30 * time.Second) // Simulated benchmark duration

	o.mu.Lock()
	sub.State = Completed
	o.mu.Unlock()

	log.Printf("Benchmark completed for %s", id)
}

func main() {
	fmt.Println("Orchestrator Service Starting...")

	k8sMan, err := k8s.NewManager()
	if err != nil {
		log.Printf("Warning: Kubernetes manager failed to initialize: %v (running in local mode)", err)
	}

	orch := NewOrchestrator(k8sMan, "http://localhost:8081")
	http.HandleFunc("/submit", orch.HandleSubmit)
	http.HandleFunc("/status", orch.HandleStatus)

	log.Printf("Orchestrator listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
