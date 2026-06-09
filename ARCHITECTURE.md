# Architecture Blueprint: Distributed Benchmarking & Hosting Platform (Trade_Bench)

## 1. System Overview
The Trade_Bench platform is designed to evaluate contestant-submitted trading infrastructure (e.g., matching engines, orderbooks) under extreme load. The system provides a secure, sandboxed environment for code execution, a scalable bot fleet for traffic generation, and a low-latency telemetry pipeline for real-time performance assessment.

## 2. Core Components

### 2.1 Submission & Sandboxing Engine (Orchestrator)
*   **Role:** Manages the lifecycle of a contestant's submission.
*   **Implementation:** A Go-based service (`cmd/orchestrator`) that interfaces with Kubernetes.
*   **Isolation Strategy:** 
    *   Submissions are containerized and deployed as Kubernetes Pods.
    *   **Resource Constraints:** Strict CPU limits (CPU pinning via `cpuset-cpus`) and memory limits (cgroups) to ensure fair benchmarking and prevent resource exhaustion.
    *   **Networking:** Isolated via Kubernetes Network Policies to prevent external communication and side-channel attacks.

### 2.2 Distributed Load Generator (Bot Fleet)
*   **Role:** Simulates thousands of market participants sending high-velocity orders.
*   **Implementation:** A highly concurrent Go service (`cmd/load-generator`) designed to scale horizontally across multiple pods.
*   **Protocols Supported:** REST (for initial configuration), WebSocket/gRPC (for high-velocity order entry), and FIX (via a translation layer).
*   **Concurrency:** Utilizes Go's lightweight goroutines to manage thousands of active "bots" per pod.

### 2.3 Telemetry & Validation Ingester
*   **Role:** Captures every interaction between the Bot Fleet and the Contestant Submission.
*   **Implementation:** A low-latency tracking system (`cmd/ingester`) that aggregates metrics before storage.
*   **Validation Engine:** 
    *   **Shadow Matching Engine:** Maintains a price-time priority reference orderbook (`pkg/telemetry/validation.go`) to verify contestant execution.
    *   **Live Scoring:** Calculates real-time accuracy percentages alongside latency and throughput.
*   **Metrics Tracked:**
    *   **Latency:** p50, p90, and p99 order acknowledgment times.
    *   **Throughput:** Transactions Per Second (TPS).
    *   **Correctness:** Validation of price-time priority and fill accuracy.

## 3. Data Flow & Communication

1.  **Submission:** Contestant uploads source/binary -> **Orchestrator** containerizes and deploys it to a k8s sandbox.
2.  **Orchestration:** **Orchestrator** triggers the **Load Generator** to start the benchmarking run.
3.  **Load Generation:** **Bot Fleet** bombards the contestant's endpoint with orders.
4.  **Telemetry Streaming:** Each bot sends interaction logs (latency, status, order data) to the **Ingester** via **NATS** (subject: `telemetry.metrics`).
5.  **Aggregation & Storage:** **Ingester** consumes from NATS, computes rolling statistics, and maintains the reference orderbook.
6.  **Leaderboard:** **Ingester** broadcasts live metrics over **WebSockets** to the dashboard UI.


## 4. Technology Stack
*   **Language:** Go (Golang) for its superior concurrency model and performance.
*   **Communication:** gRPC for low-latency, type-safe inter-service communication.
*   **Orchestration:** Kubernetes (K8s) for container management and scaling.
*   **Databases:**
    *   **Redis:** For high-speed, real-time leaderboard metrics.
    *   **TimescaleDB:** For time-series telemetry data and deep performance analysis.
*   **IaC:** Terraform for infrastructure provisioning and Helm for Kubernetes application management.

## 5. Security & Fair Play
*   **Sandboxing:** Rootless containers with restricted syscalls (seccomp).
*   **Validation:** Automated correctness checks to ensure contestants aren't "gaming" the benchmark by skipping order validation logic.
