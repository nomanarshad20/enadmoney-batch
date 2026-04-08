# EnadMoney Batch Processing System
## Comprehensive Project Documentation

**Version:** 0.0.1-SNAPSHOT  
**Technology Stack:** Spring Boot 3.5.13 (Java 17), PostgreSQL, Maven  
**Group ID:** com.eandmoney  
**Documentation Generated:** 2026-04-08

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Core Concepts](#3-core-concepts)
4. [Key Features](#4-key-features)
5. [Project Structure](#5-project-structure)
6. [Technology Stack](#6-technology-stack)
7. [API Endpoints](#7-api-endpoints)
8. [Database Schema](#8-database-schema)
9. [Batch Processing Workflow](#9-batch-processing-workflow)
10. [Job Types](#10-job-types)
11. [Batch Statuses](#11-batch-statuses)
12. [Configuration](#12-configuration)
13. [Future Enhancements](#13-future-enhancements)

---

## 1. Project Overview

### Purpose

The EnadMoney Batch Processing System is a high-performance, scalable Spring Boot application designed to process large volumes of user data in batches. The system intelligently manages batch jobs with pagination, parallel processing, and automatic retry mechanisms to handle failures gracefully. It is specifically built for processing user lifecycle events such as marking inactive users and identifying dormant accounts.

### Business Goals

- Process large volumes of user data efficiently and reliably
- Minimize processing time through parallel execution
- Ensure data consistency and track job status throughout the lifecycle
- Handle failures gracefully with retry mechanisms
- Support graceful shutdown with job state persistence

### Key Capabilities

- **Batch Job Orchestration:** Configurable batch sizes and automatic dataset splitting
- **Parallel Processing:** Configurable thread pools for concurrent record processing
- **Pagination:** Memory-efficient range-based pagination (startId/endId)
- **Multi-Job Support:** Extensible architecture supporting multiple job types
- **Real-time Tracking:** Progress monitoring and job status visibility
- **Retry Mechanisms:** Automatic retry for failed batches
- **Graceful Shutdown:** Persistence of job state during pod termination
- **Worker Node Tracking:** Identifies which node processed which batch

---

## 2. System Architecture

### High-Level Architecture

The system follows a distributed batch processing architecture with clear separation of concerns:

| Component | Responsibility |
|-----------|-----------------|
| **Publisher** | Initiates batch jobs and splits large datasets into manageable chunks |
| **Orchestrator** | Routes jobs to appropriate processors based on job type (Factory Pattern) |
| **Processor** | Executes job logic with parallel processing of individual records |
| **Consumer** | REST endpoint to receive and process batch jobs |
| **Database** | PostgreSQL for persistent storage of job states and user data |
| **Queue** | External queue system (Eand) for job submission and distribution |

### Processing Flow

1. Client initiates batch job through REST API with job configuration
2. Publisher splits dataset into chunks based on batch configuration
3. Batch metadata is persisted to database with PENDING status
4. Batches are submitted to external queue (Eand Client)
5. Consumer receives batch request and routes to appropriate processor
6. Processor fetches records with pagination (startId/endId range)
7. Each page is submitted to thread pool for parallel processing
8. Records are processed concurrently with configurable thread pool size
9. Progress is tracked (processedCount, failedCount, etc.)
10. Batch status transitions: **PENDING → PROCESSING → COMPLETED/FAILED**
11. Failed records are tracked and completion timestamp is recorded

---

## 3. Core Concepts

### Jobs vs Batches

- **Job:** A logical unit of work (e.g., "mark all inactive users")
- **Batch:** A subset of a Job, processing a specific range of records
- **Record Range:** Each batch is defined by `startId` and `endId` for efficient pagination

### Batch Lifecycle

| Status | Description |
|--------|-------------|
| **PENDING** | Batch created and awaiting processing |
| **PROCESSING** | Batch is actively being processed |
| **COMPLETED** | Batch completed successfully |
| **FAILED** | Batch failed during processing |
| **ABORTED** | Batch was aborted (e.g., pod termination) |

### Pagination Strategy

Records are fetched using a range-based pagination strategy (startId/endId). This approach:
- Ensures efficient database queries without requiring full table scans
- Manages memory efficiently by processing data in chunks
- Allows each page to be processed in parallel by worker threads
- Simplifies retry logic by maintaining record ranges

### Parallel Processing

The system uses `CompletableFuture` with a configurable thread pool (`ExecutorService`) to:
- Process individual record pages in parallel
- Track completion via `CompletableFuture.allOf().join()`
- Handle exceptions gracefully with fallback handlers
- Manage thread lifecycle with proper shutdown

---

## 4. Key Features

### Scalable Batch Processing
Process millions of records efficiently with configurable parallelism and memory management.

### Fault Tolerance
- Automatic retry mechanisms for failed batches
- Detailed failure tracking per record
- Graceful error handling and recovery

### Progress Tracking
- Real-time monitoring of processed/failed records
- Timestamps for batch lifecycle (created, started, completed)
- Worker node identification for audit trails

### Worker Node Tracking
Identifies which node processed which batch for:
- Distributed processing scenarios
- Debugging and support
- Load balancing analysis

### Graceful Shutdown
- Marks in-progress jobs as FAILED on pod termination
- Allows thread pools to shut down gracefully
- Persists job state before shutdown

### Extensible Design
Easy to add new job types by extending `AbstractBatchJobProcessor`:
```
1. Add enum value to JobTypeEnum
2. Create processor class extending AbstractBatchJobProcessor
3. Implement abstract methods
4. Annotate with @Component
5. Factory auto-discovers and registers
```

### Factory Pattern
Dynamic routing to appropriate processor based on job type eliminates conditional logic and supports plugin architecture.

### Transaction Management
Spring Transaction support ensures data consistency across batch operations.

### Comprehensive Logging
Detailed logs at every stage using SLF4J for debugging and monitoring:
- Job initiation and completion
- Page processing progress
- Error conditions and exceptions
- Performance metrics

### Configurable Tuning
All critical parameters are configurable:
- Thread pool size
- Batch chunk size
- Pagination size
- Retry count
- Custom SQL queries

---

## 5. Project Structure

```
src/main/java/com/example/demo/
│
├── DemoApplication.java
│   └── Spring Boot application entry point
│
└── eand/
    ├── config/
    │   └── GracefulShutdownHandler.java
    │       Handles pod termination and marks jobs as FAILED
    │
    ├── controller/
    │   └── UserController.java
    │       REST API endpoints for batch job initiation and processing
    │       ├── GET /api/users/publisher (initiate)
    │       └── POST /api/users/consumer (receive & process)
    │
    ├── service/
    │   ├── JobBatchingPublisherService (Interface)
    │   ├── JobBatchingPublisherServiceImpl
    │   │   Splits dataset into batches and submits to queue
    │   ├── UserService (Interface)
    │   └── UserServiceImpl
    │       User processing business logic
    │
    ├── factory/
    │   ├── BatchJobProcessorFactory
    │   │   Factory pattern for processor selection
    │   └── BatchJobRoutingService
    │       Routes jobs to appropriate processor
    │
    ├── job/processor/
    │   ├── BatchJobProcessor (Interface)
    │   │   getJobType(), processBatchJob()
    │   ├── AbstractBatchJobProcessor<T>
    │   │   Generic implementation with pagination and parallelization
    │   ├── InactiveUserBatchProcessor
    │   │   Marks users as inactive
    │   └── DormantUserBatchProcessor
    │       Marks users as dormant
    │
    ├── entity/
    │   ├── UserEntity
    │   │   User data model with status tracking
    │   └── JobBatchProcessingEntity
    │       Batch job metadata and progress tracking
    │
    ├── dto/
    │   ├── JobBatchProcessingDto
    │   │   Transfer object for batch data
    │   └── BatchConfigDTO
    │       Configuration for batch initiation
    │
    ├── repo/
    │   ├── UserEntityRepo
    │   │   Spring Data JPA for user queries
    │   └── JobProcessingRepo
    │       Spring Data JPA for batch job persistence
    │
    ├── enums/
    │   ├── BatchStatusEnum
    │   │   PENDING, PROCESSING, COMPLETED, FAILED, ABORTED
    │   └── JobTypeEnum
    │       INACTIVE_USER, DORMANT_USER (extensible)
    │
    ├── client/
    │   └── EandClient
    │       REST client for external queue submission
    │
    ├── pc/
    │   └── ConsumerClient
    │       Consumer interface abstraction
    │
    └── utill/
        └── RetryUtil
            Retry logic utilities
```

---

## 6. Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Framework** | Spring Boot | 3.5.13 |
| **Language** | Java | 17 |
| **Build Tool** | Maven | 3.x |
| **Database** | PostgreSQL | Latest |
| **ORM** | Spring Data JPA + Hibernate | Latest |
| **Logging** | SLF4J + Logback | Latest |
| **Code Generation** | Project Lombok | Latest |

### Dependencies

```xml
<!-- Spring Boot Data JPA -->
org.springframework.boot:spring-boot-starter-data-jpa

<!-- Spring Boot Web -->
org.springframework.boot:spring-boot-starter-web

<!-- PostgreSQL Driver -->
org.postgresql:postgresql

<!-- Project Lombok -->
org.projectlombok:lombok

<!-- Spring Boot DevTools -->
org.springframework.boot:spring-boot-devtools (runtime)
```

---

## 7. API Endpoints

### Publisher Endpoint

**Endpoint:** `GET /api/users/publisher`

**Purpose:** Initiate batch job for inactive users

**Response:** List of batch configurations created and submitted

**Example:**
```
GET /api/users/publisher
Response: {"status": "SUCCESS", "batches": [...]}
```

### Consumer Endpoint

**Endpoint:** `POST /api/users/consumer`

**Purpose:** Receive a batch job from the queue and process it

**Request Body:** `JobBatchProcessingDto`

**Response:** `"SUCCESS"` when batch is accepted for processing

**Example:**
```json
POST /api/users/consumer
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "batchId": 1,
  "startId": 0,
  "endId": 1000,
  "jobType": "INACTIVE_USER",
  "paginationSize": 100,
  "executorPoolSize": 4,
  "batchChunkSize": 1000
}
Response: "SUCCESS"
```

### Request/Response Models

#### JobBatchProcessingDto Fields

| Field | Type | Description |
|-------|------|-------------|
| id | Long | Unique batch identifier |
| jobId | String | Unique job identifier (UUID) |
| batchId | Long | Sequential batch number within a job |
| startId | Long | Starting record ID for this batch |
| endId | Long | Ending record ID for this batch |
| totalRecords | Integer | Total records to process in this batch |
| processedRecords | Integer | Records successfully processed |
| failedRecords | Integer | Records that failed |
| status | String | Current batch status (PENDING, PROCESSING, COMPLETED, FAILED) |
| retryCount | Integer | Number of retries allowed |
| workerNode | String | Node that processed this batch |
| jobType | String | Type of job (INACTIVE_USER, DORMANT_USER) |
| paginationSize | Integer | Records per page |
| executorPoolSize | Integer | Thread pool size |
| batchChunkSize | Integer | Records per chunk |
| createdAt | LocalDateTime | Batch creation timestamp |
| startedAt | LocalDateTime | Processing start timestamp |
| completedAt | LocalDateTime | Processing completion timestamp |

---

## 8. Database Schema

### job_batch_processing Table

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | BIGINT | PK, AUTO_INCREMENT | Auto-generated batch identifier |
| job_id | VARCHAR(255) | - | Unique job identifier |
| batch_id | BIGINT | - | Sequential batch number |
| start_id | BIGINT | NOT NULL | Starting record ID |
| end_id | BIGINT | NOT NULL | Ending record ID |
| total_records | INTEGER | - | Total records in batch |
| processed_records | INTEGER | - | Successfully processed records |
| failed_records | INTEGER | - | Failed records |
| status | VARCHAR | NOT NULL | PENDING/PROCESSING/COMPLETED/FAILED/ABORTED |
| retry_count | INTEGER | NOT NULL | Number of retries |
| worker_node | VARCHAR(100) | - | Processing node identifier |
| parent_worker_node | VARCHAR(100) | - | Parent worker node |
| job_type | VARCHAR(100) | - | Job type identifier |
| created_at | TIMESTAMP | - | Batch creation time |
| started_at | TIMESTAMP | - | Processing start time |
| completed_at | TIMESTAMP | - | Processing completion time |
| pagination_size | INTEGER | NOT NULL | Records per page |
| executor_pool_size | INTEGER | NOT NULL | Thread pool size |
| batch_chunk_size | INTEGER | NOT NULL | Records per chunk |

### users Table

Stores user information with fields:
- `id` (BIGINT PK)
- `email` (VARCHAR)
- `username` (VARCHAR)
- `status` (VARCHAR) - active, inactive, dormant
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

---

## 9. Batch Processing Workflow

### Step-by-Step Workflow

1. **Initiation** → Client calls GET /api/users/publisher
2. **Dataset Split** → Publisher fetches all user IDs and splits them into batches
3. **Batch Creation** → Create JobBatchProcessingEntity with PENDING status for each batch
4. **Persistence** → Save all batch entities to database
5. **Queue Submission** → Submit each batch to Eand queue (external system)
6. **Job Reception** → Consumer endpoint receives batch from queue
7. **Routing** → BatchJobRoutingService uses Factory to select appropriate processor
8. **Status Update** → Mark batch as PROCESSING and record started_at timestamp
9. **Data Retrieval** → Processor fetches records with pagination (startId/endId range)
10. **Parallel Processing** → Submit each page to thread pool as separate CompletableFuture
11. **Record Processing** → Each thread processes its batch of records (e.g., mark as inactive)
12. **Progress Tracking** → Update processedCount and failedCount atomically
13. **Completion Wait** → Wait for all futures to complete (CompletableFuture.allOf().join())
14. **Final Status** → Mark batch as COMPLETED or FAILED based on success
15. **Shutdown Hook** → If pod terminates, GracefulShutdownHandler marks PROCESSING batches as FAILED

### Execution Timeline Example

```
Timeline:
T0: Publisher initiates job → 5 batches created (PENDING)
T1: Batches submitted to Eand queue
T2: Consumer receives batch #1 → Status: PROCESSING
T3: Fetch 1000 records in pages of 100
T4: 10 pages submitted to thread pool (4 threads)
T5: Pages processed in parallel
T6: All pages complete → Status: COMPLETED
T7: Consumer receives batch #2 → Status: PROCESSING
...
T-final: All 5 batches processed
```

---

## 10. Job Types

### INACTIVE_USER

- **Enum Value:** `JobTypeEnum.INACTIVE_USER`
- **Processor Class:** `InactiveUserBatchProcessor`
- **Purpose:** Identifies and marks user accounts that have been inactive for a specified period
- **Action:** Updates user status to `INACTIVE` in the database
- **Trigger:** GET /api/users/publisher endpoint

### DORMANT_USER

- **Enum Value:** `JobTypeEnum.DORMANT_USER`
- **Processor Class:** `DormantUserBatchProcessor`
- **Purpose:** Identifies user accounts that are dormant (no activity for extended period)
- **Action:** Updates user status to `DORMANT` in the database

### Adding New Job Types

To extend the system with a new job type:

1. **Add Enum Value**
   ```java
   public enum JobTypeEnum {
       INACTIVE_USER,
       DORMANT_USER,
       YOUR_NEW_JOB_TYPE  // Add here
   }
   ```

2. **Create Processor Class**
   ```java
   @Component
   public class YourNewBatchProcessor extends AbstractBatchJobProcessor<YourEntity> {
       
       @Override
       public JobTypeEnum getJobType() {
           return JobTypeEnum.YOUR_NEW_JOB_TYPE;
       }
       
       @Override
       protected List<YourEntity> getQueryPaginatedResponse(long startId, long endId, long pageSize) {
           // Implement pagination query
       }
       
       @Override
       protected void processRecords(List<YourEntity> records, AtomicInteger processedCount, AtomicInteger failedCount) {
           // Implement processing logic
       }
       
       @Override
       protected long getLastId(List<YourEntity> records) {
           // Return last ID from records
       }
       
       @Override
       protected int countRecords(JobBatchProcessingDto dto) {
           // Return count of records in range
       }
   }
   ```

3. **Factory Auto-Discovery**
   - The `BatchJobProcessorFactory` automatically discovers and registers the processor
   - No manual configuration needed

---

## 11. Batch Statuses

| Status | Meaning | Transitions |
|--------|---------|-------------|
| **PENDING** | Batch created, awaiting processing | → PROCESSING |
| **PROCESSING** | Batch actively being processed | → COMPLETED or FAILED or ABORTED |
| **COMPLETED** | Batch processing finished successfully | Terminal |
| **FAILED** | Batch encountered unrecoverable error | Terminal (retry creates new batch) |
| **ABORTED** | Batch stopped forcefully (e.g., pod termination) | Terminal |

### Status Transition Diagram

```
PENDING ──→ PROCESSING ──→ COMPLETED
                      ├──→ FAILED
                      └──→ ABORTED (on shutdown)
```

---

## 12. Configuration

### Configurable Parameters

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| batchChunkSize | Integer | - | Total records per batch (used during split) |
| paginationSize | Integer | 100 | Records per page (processor-side pagination) |
| executorPoolSize | Integer | 4 | Thread pool size for parallel processing |
| retryCount | Integer | 1 | Maximum retry attempts for failed batches |
| jobType | Enum | - | Type of job (INACTIVE_USER, DORMANT_USER) |
| startId | Long | 0 | Starting record ID for batch range |
| endId | Long | - | Ending record ID for batch range |
| jpaSqlCommand | String | - | Custom SQL query for fetching record IDs |
| workerNode | String | localhost | Identifier for the processing worker node |

### Spring Boot Configuration

**application.properties:**
```properties
# Server Configuration
server.port=8080
server.shutdown=graceful

# Spring Lifecycle
spring.lifecycle.timeout-per-shutdown-phase=30s

# Database Configuration
spring.datasource.url=jdbc:postgresql://localhost:5432/eandmoney
spring.datasource.username=postgres
spring.datasource.password=your_password
spring.datasource.hikari.maximum-pool-size=20

# JPA/Hibernate Configuration
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=false
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect

# Logging Configuration
logging.level.root=INFO
logging.level.com.example.demo=INFO
logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} - %msg%n
```

**application.yml (alternative):**
```yaml
server:
  port: 8080
  shutdown: graceful

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
  datasource:
    url: jdbc:postgresql://localhost:5432/eandmoney
    username: postgres
    password: your_password
    hikari:
      maximum-pool-size: 20
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect

logging:
  level:
    root: INFO
    com.example.demo: INFO
```

---

## 13. Future Enhancements

### Monitoring & Observability
- **Prometheus/Grafana Integration:** Export metrics for batch processing (throughput, latency, failure rates)
- **Distributed Tracing:** Integrate with Jaeger or Zipkin for request tracing
- **Health Checks:** Actuator endpoints for batch processor health status

### Advanced Retry Logic
- **Exponential Backoff:** Implement exponential backoff for retries
- **Dead-Letter Queue:** Capture permanently failed batches in DLQ
- **Retry Policies:** Configurable retry strategies per job type

### Job Scheduling
- **Scheduled Jobs:** Integration with Spring Scheduler for periodic batch execution
- **Cron Expressions:** Flexible scheduling (e.g., "run inactive user job every night")
- **Job Triggers:** Event-based job triggering

### Batch Prioritization
- **Priority Queue:** Support priority levels for different job types
- **Load Balancing:** Distribute jobs across worker nodes based on load

### Distributed Processing
- **Multi-Node Clustering:** Coordinate batch processing across multiple nodes
- **Load Balancing:** Automatic work distribution among nodes
- **Failover:** Handle node failures gracefully

### Real-time Dashboard
- **Progress API:** Real-time batch status and progress REST endpoints
- **Dashboard UI:** Web-based dashboard for monitoring batches
- **Alerts:** Real-time notifications for batch completion or failures

### Advanced Features
- **Webhook Notifications:** Callback URLs on batch completion
- **Batch Rollback:** Capability to rollback completed batches
- **Plugin Architecture:** Custom transformation plugins
- **Audit Logging:** Comprehensive audit trail of all batch operations
- **Batch Dependencies:** Support for dependent batches (wait for previous to complete)

---

## Appendix: Graceful Shutdown Handling

### Why Graceful Shutdown Matters

When a pod is terminated in Kubernetes:
1. SIGTERM signal is sent to the application
2. Kubernetes provides a grace period (typically 30-45 seconds)
3. If app doesn't stop, SIGKILL is sent (hard kill)

Without graceful shutdown:
- In-progress batches remain PROCESSING in database
- These batches become orphaned and require manual intervention
- Data consistency issues can occur

### Implementation

#### GracefulShutdownHandler

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class GracefulShutdownHandler {
    private final JobProcessingRepo jobProcessingRepo;

    @EventListener(ContextClosedEvent.class)
    public void handleContextClosed(ContextClosedEvent event) {
        log.warn("⚠️ SHUTDOWN INITIATED - Marking all in-progress jobs as FAILED");
        
        List<JobBatchProcessingEntity> processingJobs = 
            jobProcessingRepo.findByStatus(BatchStatusEnum.PROCESSING.name());
        
        processingJobs.forEach(job -> {
            job.setStatus(BatchStatusEnum.FAILED.name());
            job.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(job);
            log.warn("✗ Marked job as FAILED: jobId={}, batchId={}", 
                     job.getJobId(), job.getBatchId());
        });
    }
}
```

### Kubernetes Configuration

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eandmoney-batch
spec:
  template:
    spec:
      containers:
      - name: batch-app
        image: eandmoney-batch:latest
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 15"]
        terminationGracePeriodSeconds: 45
        env:
        - name: SERVER_SHUTDOWN
          value: "graceful"
```

### Shutdown Sequence

```
T0: Pod receives SIGTERM
T0-T15: preStop hook sleep (gives app time to prepare)
T15-T30: Graceful shutdown (30s timeout per Spring config)
  - Listen for ContextClosedEvent
  - Query PROCESSING batches
  - Mark as FAILED
  - Close connections
T30-T45: Buffer time
T45: SIGKILL if still running (hard kill)
```

---

## Conclusion

The EnadMoney Batch Processing System is a production-ready, scalable solution for processing large volumes of user data. Key strengths include:

✅ **Scalability:** Handle millions of records with configurable parallelism  
✅ **Reliability:** Graceful error handling and job state persistence  
✅ **Extensibility:** Easy to add new job types with factory pattern  
✅ **Observability:** Comprehensive logging and progress tracking  
✅ **Resilience:** Automatic retries and graceful shutdown  
✅ **Integration:** Seamless integration with Kubernetes and external queues  

The system is designed to be production-ready and can be extended to support more complex batch processing scenarios.

---

**Generated:** 2026-04-08  
**Documentation Version:** 1.0
