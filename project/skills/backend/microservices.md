# Skill: Microservices Architecture

## Overview
Decomposing applications into independently deployable services with clear ownership boundaries.

## Key Patterns

### Service Decomposition
- **Domain-Driven Design** — bounded context per service
- **Strangler Fig** — migrate monolith incrementally; wrap legacy, route new traffic to microservice
- **Single Responsibility** — each service owns one business capability
- Target: 2-pizza team can own and operate each service

### Communication Patterns
| Style | Protocol | When to Use |
|---|---|---|
| Synchronous | REST / gRPC | Real-time response required |
| Asynchronous | Kafka / RabbitMQ | Decouple producers from consumers |
| Event-driven | Domain events | Eventual consistency acceptable |

### Service Mesh
- **Sidecar proxy** (Envoy/Istio) — handles mTLS, retries, circuit breaking outside app code
- **Service discovery** — services register and find each other via registry (Consul, Kubernetes DNS)
- **Load balancing** — client-side (Ribbon) or server-side (Nginx/Envoy)

### Data Management
- Each service owns its own database — no shared DB across services
- Eventual consistency across service boundaries via events
- Saga pattern for distributed transactions
- API composition at gateway for queries spanning multiple services

### Resilience Patterns
- **Circuit Breaker** — stop calling failing downstream service; fast-fail during outage
- **Bulkhead** — isolate resource pools per downstream service
- **Timeout** — always set timeouts on inter-service calls
- **Retry with jitter** — exponential backoff on transient failures

## Best Practices
- Define clear service contracts (OpenAPI, Protobuf) and version them
- Implement distributed tracing from day one (correlation IDs in all logs)
- Each service must be independently deployable without coordinating with others
- Centralise cross-cutting concerns in the API gateway (auth, rate limiting, logging)
- Automate service scaffolding with templates to enforce standards

## Common Pitfalls
- Distributed monolith — services tightly coupled via synchronous chains
- Shared database across services (breaks independent deployability)
- No service mesh — reinventing resilience in every service
- Chatty services — too many small synchronous calls in a request path

## Tools
- **Kubernetes** — container orchestration
- **Istio / Linkerd** — service mesh
- **Kong / AWS API Gateway** — API gateway
- **Jaeger / Tempo** — distributed tracing
- **Kafka** — event streaming
