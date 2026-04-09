package com.example.demo.eand.dto;

import com.example.demo.eand.enums.BatchJobTypeEnum;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobBatchProcessingDto {

    private Long id;
    private String jobId;
    private Long batchId;

    private Long startId;
    private Long endId;

    private Integer totalRecords;
    private Integer processedRecords;
    private Integer failedRecords;

    private BatchJobTypeEnum jobType;
    private String status;
    private Integer retryCount;

    private String workerNode;
    private String parentWorkerNode;

    private LocalDateTime createdAt;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;

    private Integer batchChunkSize;
    private Integer executorPoolSize;
    private Integer paginationSize;
}