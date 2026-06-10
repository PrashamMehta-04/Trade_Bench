# Trade_Bench System Architecture

This document contains the primary architecture diagrams for the Trade_Bench platform, covering both high-level process flow and detailed engineering containers.

## 1. Engineering View (C4 Container Diagram)

The C4 diagram provides a detailed view of the microservices, their technology stacks, and the protocols used for inter-service communication.

```mermaid
C4Container
    title Container Diagram for Trade_Bench Platform

    Person(admin, "Admin/Contestant", "Submits trading engines and monitors performance benchmarks.")
    
    System_Boundary(c1, "Trade_Bench Platform") {
        
        Container(orch, "Orchestrator Service", "Go, Docker, K8s Client", "Manages submission lifecycles, builds images, and triggers K8s deployments.")
        
        Container(lg, "Load Generator Fleet", "Go, Goroutines", "Simulates thousands of bots. Sends high-velocity orders via gRPC/FIX.")
        
        ContainerDb(nats, "NATS JetStream", "Message Broker", "High-throughput event bus for real-time telemetry streaming.")

        Container(ingester, "Telemetry Ingester", "Go", "Aggregates metrics, calculates p-values, and validates execution against a Shadow Orderbook.")

        System_Boundary(sandbox, "Isolated Benchmark Sandbox") {
            Container(sub, "Contestant Submission", "Any (Containerized)", "The matching engine or strategy being evaluated. Resource-constrained.")
        }

        ContainerDb(redis, "Leaderboard Store", "Redis", "Stores real-time scores and active benchmark states.")
        
        ContainerDb(tsdb, "Historical Metrics", "TimescaleDB", "Persistent storage for deep performance analysis and audit trails.")
    }

    System_Ext(k8s, "Kubernetes API", "Orchestrates container lifecycle and enforces NetworkPolicies.")
    System_Ext(git, "Source Control", "GitHub / GitLab", "Hosts contestant source code or pre-built binaries.")

    %% Relationships
    Rel(admin, orch, "Submits job", "gRPC/REST")
    Rel(orch, git, "Pulls code", "HTTPS")
    Rel(orch, k8s, "Deploys pods", "K8s API")
    Rel(k8s, sub, "Enforces limits", "Cgroups/NetPolicy")
    
    Rel(orch, lg, "Triggers run", "gRPC")
    Rel(lg, sub, "Sends orders", "gRPC/FIX/REST")
    
    Rel(lg, nats, "Streams metrics", "NATS Protocol")
    Rel(nats, ingester, "Consumes events", "Sub/Push")
    
    Rel(ingester, redis, "Updates leaderboard", "RESP")
    Rel(ingester, tsdb, "Persists data", "SQL")
    Rel(admin, redis, "Views live results", "WebSockets")

    %% Styling
    UpdateElementStyle(orch, $bgColor="#08427b", $fontColor="#ffffff")
    UpdateElementStyle(lg, $bgColor="#08427b", $fontColor="#ffffff")
    UpdateElementStyle(ingester, $bgColor="#08427b", $fontColor="#ffffff")
    UpdateElementStyle(sub, $bgColor="#1168bd", $fontColor="#ffffff")
    UpdateElementStyle(nats, $bgColor="#666666", $fontColor="#ffffff")
```

---

## 2. Process View (Sequential Flowchart)

The flowchart illustrates the temporal sequence of events during a single benchmarking run.

```mermaid
flowchart TD
    %% External Entities
    User([User / Admin])
    Git[(GitHub / Repository)]

    %% Management Plane
    subgraph Control_Plane [Management & Control Plane]
        ORCH[Orchestrator Service]
        LG[Load Generator Fleet]
    end

    %% Execution Sandbox
    subgraph Sandbox [Isolated Benchmark Sandbox]
        direction TB
        subgraph NetPolicy [K8s Network Isolation]
            SUB[Contestant Submission Pod]
        end
        Limits[CPU Pinning / Memory Limits] -.-> SUB
    end

    %% Telemetry Pipeline
    subgraph Telemetry_Stack [Telemetry & Validation Pipeline]
        NATS((NATS JetStream))
        ING[Telemetry Ingester]
        Validation[[Shadow Orderbook Validation]]
    end

    %% Data Layer
    subgraph Persistence [Persistence & State Layer]
        Redis[(Redis - Live Leaderboard)]
        TSDB[(TimescaleDB - History)]
    end

    %% Process Flow
    User -->|1. Submit Job| ORCH
    ORCH -->|2. Fetch Source| Git
    ORCH -->|3. Deploy Sandbox| SUB
    ORCH -->|4. Trigger Load| LG
    
    LG -->|5. Order Traffic| SUB
    LG -->|6. Latency/TPS Events| NATS
    
    NATS -->|7. Consume Stream| ING
    ING -->|8. Verify Correctness| Validation
    ING -->|9. Update Stats| Redis
    ING -->|10. Archive Metrics| TSDB

    %% Visual Styling
    classDef control fill:#f5f5f5,stroke:#333,stroke-width:2px;
    classDef sandbox fill:#e1f5fe,stroke:#01579b,stroke-width:2px,stroke-dasharray: 5 5;
    classDef telemetry fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef storage fill:#f1f8e9,stroke:#33691e,stroke-width:2px;

    class ORCH,LG control;
    class SUB sandbox;
    class NATS,ING,Validation telemetry;
    class Redis,TSDB storage;
```

---

### Component Legend

| Component | Responsibility | Technology |
| :--- | :--- | :--- |
| **Orchestrator** | Job management and K8s orchestration. | Go, K8s SDK |
| **Load Generator** | High-velocity bot simulation. | Go, Goroutines |
| **NATS JetStream** | Real-time event streaming backbone. | NATS |
| **Ingester** | Metric aggregation and validation. | Go |
| **Shadow Orderbook** | Reference implementation for fill accuracy. | Go (pkg/telemetry) |
| **Redis** | High-speed leaderboard storage. | Redis (RESP) |
| **TimescaleDB** | Historical performance telemetry. | PostgreSQL / Timescale |
