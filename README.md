# Trade_Bench: Distributed Benchmarking Platform

Trade_Bench is a high-performance, distributed platform designed to evaluate and host trading infrastructure submissions. Built for the IICPC Summer Hackathon 2026, it emphasizes engineering excellence, system resilience, and scalability.

## Core Features
- **Submission Sandboxing:** Securely deploy contestant code in isolated Kubernetes pods with strict resource limits.
- **Distributed Bot Fleet:** Simulate thousands of concurrent market participants sending high-velocity orders.
- **Real-Time Telemetry:** Capture and aggregate latency (p50, p90, p99) and throughput metrics in real-time.
- **Architecture-First Design:** Decoupled microservices architecture using Go and Kubernetes.

## Project Structure
- `cmd/orchestrator`: Management service for submissions and benchmark lifecycles.
- `cmd/load-generator`: High-concurrency bot simulation engine.
- `cmd/ingester`: Low-latency telemetry aggregation and reporting service.
- `pkg/k8s`: Kubernetes orchestration layer for dynamic pod management.
- `pkg/telemetry`: Shared data models and metric definitions.
- `deploy/helm`: Infrastructure as Code (IaC) for automated deployment.

## Getting Started

### Prerequisites
- Go 1.22+
- Docker
- Kubernetes (for sandboxing features)

### Building
```bash
make build
```
Binaries will be generated in the `bin/` directory.

### Running Locally (Prototype Mode)
1. Start the Ingester: `./bin/ingester`
2. Start the Orchestrator: `./bin/orchestrator`
3. Submit a benchmark: `curl -X POST "localhost:8080/submit?name=my-matching-engine&image=my-image:latest"`
4. Monitor status: `curl "localhost:8080/status?submission_id=<id>"`

### Deployment
Deploy to Kubernetes using Helm:
```bash
helm install trade-bench ./deploy/helm/trade-bench
```

## Architecture
See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed breakdown of the system design and technical specifications.