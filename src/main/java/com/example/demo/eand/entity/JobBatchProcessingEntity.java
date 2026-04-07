package com.example.demo.eand.entity;

import com.example.demo.eand.enums.BatchStatusEnum;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "job_batch_processing")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobBatchProcessingEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "job_id")
    private String jobId;

    @Column(name = "batch_id")
    private Long batchId;

    @Column(name = "start_id", nullable = false)
    private Long startId;

    @Column(name = "end_id", nullable = false)
    private Long endId;

    @Column(name = "total_records")
    private Integer totalRecords;

    @Column(name = "processed_records")
    private Integer processedRecords = 0;

    @Column(name = "failed_records")
    private Integer failedRecords = 0;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "parent_worker_node", length = 100)
    private String parentWorkerNode;

    @Column(name = "worker_node", length = 100)
    private String workerNode;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "batch_chunk_size", nullable = false)
    private Integer batchChunkSize;

    @Column(name = "pagination_size", nullable = false)
    private Integer paginationSize;

    @Column(name = "executor_pool_size", nullable = false)
    private Integer executorPoolSize;

    @Column(name = "job_type", length = 100)
    private String jobType;


    // ===============================
    // Lifecycle Hooks
    // ===============================
    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        if (this.status == null) {
            this.status = BatchStatusEnum.PENDING.name();
        }
        if (this.processedRecords == null) {
            this.processedRecords = 0;
        }
        if (this.failedRecords == null) {
            this.failedRecords = 0;
        }
        if (this.retryCount == null) {
            this.retryCount = 0;
        }
    }

}