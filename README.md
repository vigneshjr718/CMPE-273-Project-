# CMPE 273 — Enterprise Distributed Systems (Fall 2026)

## Course Project Ideas

**San José State University**  
**CMPE 273 — Section 01**  
**Instructor: Rakesh Ranjan**  
**Project Proposal: Three Ideas per Student**

This repository contains twelve candidate project ideas for CMPE 273. Each project focuses on enterprise distributed-system concepts, including RPC, asynchronous messaging, web services, data replication, partitioning, consistency, scalability, and fault tolerance.

Each student has proposed three project ideas.

---

# Student 1: Soumya Atluri

## 1. Distributed URL Shortener with Consistent Hashing

**Problem:** Build a URL-shortening service that remains fast and available as traffic and stored data increase.

**What It Demonstrates:**

- Consistent hashing for data partitioning
- Redis caching for frequently accessed links
- REST API design
- Horizontal scaling
- Read/write load testing

**Suggested Stack:** Java, Go, or Node.js; Redis; Cassandra or DynamoDB-style storage; Docker Compose.

**Stretch Goals:**

- Add a Bloom filter
- Simulate node failure and recovery
- Add API-key rate limiting
- Add link expiration and analytics

---

## 2. Real-Time Ride-Hailing Dispatch System

**Problem:** Match riders with nearby drivers while processing continuous location updates from thousands of simulated drivers.

**What It Demonstrates:**

- gRPC communication
- Geohashing or geo-sharding
- Kafka or RabbitMQ messaging
- Redis geospatial queries
- Eventual consistency

**Suggested Stack:** Java or Go gRPC services, Kafka, Redis, Docker Compose, and a map-based frontend.

**Stretch Goals:**

- Add surge pricing
- Simulate network partitions
- Measure matching latency
- Add driver and trip status tracking

---

## 3. Distributed Rate Limiter and API Gateway

**Problem:** Build a shared API gateway that protects backend services from overload while enforcing limits across multiple gateway instances.

**What It Demonstrates:**

- Token-bucket and sliding-window algorithms
- Redis-backed distributed counters
- Circuit breakers
- Retry and backoff strategies
- Reverse-proxy routing

**Suggested Stack:** Node.js, Go, or Java; Redis; Docker Compose; REST APIs; k6.

**Stretch Goals:**

- Add per-tenant quotas
- Build a monitoring dashboard
- Compare rate-limiting algorithms
- Add authentication and API-key management

---

# Student 2: Vignesh Jetty Ravi

## 4. Distributed Key-Value Store with Raft Consensus

**Problem:** Build a distributed key-value store that remains consistent when individual nodes fail.

**What It Demonstrates:**

- Raft leader election
- Log replication
- Quorum reads and writes
- Fault tolerance
- CAP-theorem tradeoffs
- Inter-node RPC communication

**Suggested Stack:** Go or Java, gRPC, Docker Compose, and HashiCorp Raft or a custom Raft implementation.

**Stretch Goals:**

- Add snapshotting and log compaction
- Visualize leader elections
- Simulate network partitions
- Add a client-facing REST API

---

## 5. E-Commerce Order Processing with the Saga Pattern

**Problem:** Coordinate inventory, payment, and shipping services without using a single distributed transaction.

**What It Demonstrates:**

- Choreography and orchestration-based Sagas
- Kafka or RabbitMQ messaging
- Compensating transactions
- Microservice decomposition
- Database-per-service architecture

**Suggested Stack:** Spring Boot, Node.js, or Go; Kafka or RabbitMQ; PostgreSQL; Docker Compose.

**Stretch Goals:**

- Compare Saga processing with two-phase commit
- Inject random service failures
- Add distributed tracing
- Implement idempotent event processing

---

## 6. Distributed Job Scheduler and Task Queue

**Problem:** Build a system where clients submit jobs and worker nodes execute them with failure detection and job reassignment.

**What It Demonstrates:**

- Leader election
- Distributed queues
- At-least-once or exactly-once processing
- Worker load balancing
- Heartbeat-based failure detection

**Suggested Stack:** Kafka or RabbitMQ, Java/Go/Python workers, ZooKeeper or etcd, Docker Compose.

**Stretch Goals:**

- Add job priorities
- Add retries with exponential backoff
- Build a job-monitoring dashboard
- Simulate worker crashes

---

# Student 3: [Enter Name]


---

# Common Themes Across All Projects

Each project explores multiple CMPE 273 concepts, including:

- RPC and object request broker-style communication
- REST and web-service APIs
- Asynchronous messaging
- Data partitioning and replication
- Distributed caching
- Leader election and consensus
- Fault tolerance and failure recovery
- CAP-theorem tradeoffs
- Load balancing and horizontal scaling
- Monitoring, logging, and performance analysis

All projects are scoped so that a working end-to-end implementation can be deployed using Docker Compose during the semester. Stretch goals provide opportunities to explore advanced distributed-system concepts, performance improvements, and failure scenarios.
