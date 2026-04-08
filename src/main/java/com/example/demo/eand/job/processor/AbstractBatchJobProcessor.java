package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import com.example.demo.eand.utill.RetryUtil;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class AbstractBatchJobProcessor<T> implements BatchJobProcessor {

    private final JobProcessingRepo jobProcessingRepo;


    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Methods to override by child classes --------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    protected abstract List<T> getQueryPaginatedResponse(long startingId, long endId, long pageSize);

    protected abstract void processRecords(List<T> records, AtomicInteger processedCount, AtomicInteger failedCount);

    protected abstract long getLastId(List<T> records);

    protected abstract int countRecords(Long startId, Long endId, String jobId, Long batchId, String jobType);


    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Generic implementation for job processing --------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    @Override
    public void processBatchJob(JobBatchProcessingDto req) {
        logStart(req);
        JobBatchProcessingEntity batch = jobProcessingRepo.findByJobIdAndBatchIdAndJobType(req.getJobId(), req.getBatchId(), req.getJobType().name());
        JobBatchProcessingDto jobDto = jobBatchProcessingEntityToDTO(batch);

        markRunningBatchEntity(batch);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        // Execute with retry logic
        int maxRetries = jobDto.getRetryCount() != null && jobDto.getRetryCount() > 0 ? jobDto.getRetryCount() : 0;
        
        boolean success = RetryUtil.executeWithRetry(
                () -> processWithPagination(jobDto, batch, processedCount, failedCount),
                maxRetries, "Batch job processing - jobId=" + jobDto.getJobId() + ", batchId=" + jobDto.getBatchId()
        );

        try {
            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());

            if (success) {
                batch.setStatus(BatchStatusEnum.COMPLETED.name());
                batch.setCompletedAt(LocalDateTime.now());
                jobProcessingRepo.save(batch);
                log.info("BATCH JOBS : Batch completed successfully. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId());
            } else {
                batch.setStatus(BatchStatusEnum.FAILED.name());
                batch.setCompletedAt(LocalDateTime.now());
                jobProcessingRepo.save(batch);
                throw new RuntimeException("BATCH JOBS : Batch processing failed after " + (maxRetries + 1) + " attempts");
            }

        } catch (Exception ex) {
            log.error("BATCH JOBS : Failed to update batch status. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId(), ex);
            throw new RuntimeException("BATCH JOBS : Batch processing failed", ex);
        }

        logEnd(jobDto);
    }

    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Supporting methods -----------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    private void processWithPagination(JobBatchProcessingDto dto, JobBatchProcessingEntity batch, AtomicInteger processedCount, AtomicInteger failedCount) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(batch.getExecutorPoolSize());

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            long startingId = dto.getStartId();
            int currentPage = 1;

            while (true) {

                log.info("BATCH JOBS : Batch preparing | jobType={} jobId={} batchId={} startId={} endId={} page={}", dto.getJobType(), dto.getJobId(), dto.getBatchId(), dto.getStartId(), dto.getEndId(), currentPage);
                List<T> records = getQueryPaginatedResponse(startingId, dto.getEndId(), dto.getPaginationSize());

                if (records == null || records.isEmpty()) {
                    log.info("BATCH JOBS : No records found in range startId={}, endId={}", dto.getStartId(), dto.getEndId());
                    break;
                }

                long lastId = getLastId(records);
                int pageNo = currentPage++;
                int pageRetries = dto.getRetryCount() != null ? dto.getRetryCount() : 0;

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    log.info("BATCH JOBS : Processing page | jobType={} jobId={} batchId={} pageNo={}", dto.getJobType(), dto.getJobId(), dto.getBatchId(), pageNo);
                    
                    // Execute page processing with retry logic
                    boolean pageSuccess = RetryUtil.executeWithRetry(
                            () -> processRecords(records, processedCount, failedCount),
                            pageRetries,
                            "Page processing - pageNo=" + pageNo + ", recordCount=" + records.size()
                    );

                    if (!pageSuccess) {
                        failedCount.addAndGet(records.size());
                        throw new RuntimeException("Page processing failed after retries - pageNo=" + pageNo);
                    }

                }, executorService).exceptionally(ex -> {
                    log.error("BATCH JOBS : Page processing failed. pageNumber={}", pageNo, ex);
                    failedCount.addAndGet(records.size());
                    return null;
                });

                futures.add(future);

                // NOTE : database query should be optimized to fetch ordered records by start and end IDs. otherwise loop will be inefficient( never ending loop).
                if (lastId >= dto.getEndId()) {
                    break;
                }

                startingId = lastId;
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } finally {
            executorService.shutdown();
            // Wait for executor to terminate gracefully
            if (!executorService.awaitTermination(10, TimeUnit.MINUTES)) {
                log.warn("BATCH JOBS : Executor service did not terminate within timeout. Force shutting down...");
                executorService.shutdownNow();
            }
        }
    }

    protected JobBatchProcessingEntity markRunningBatchEntity(JobBatchProcessingEntity batch) {
        batch.setStatus(BatchStatusEnum.PROCESSING.name());
        batch.setStartedAt(LocalDateTime.now());
        batch.setTotalRecords(countRecords( batch.getStartId() , batch.getEndId() ,batch.getJobId() , batch.getBatchId() , batch.getJobType()));
        return jobProcessingRepo.save(batch);
    }

    protected void logStart(JobBatchProcessingDto jobDto) {
        log.info("BATCH JOBS : Starting jobType={}, jobId={}, batchId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }

    protected void logEnd(JobBatchProcessingDto jobDto) {
        log.info("BATCH JOBS : Completed jobType={}, jobId={}, batchId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }

    protected AbstractBatchJobProcessor(JobProcessingRepo jobProcessingRepo) {
        this.jobProcessingRepo = jobProcessingRepo;
    }

    private JobBatchProcessingDto jobBatchProcessingEntityToDTO(JobBatchProcessingEntity entity) {
        return JobBatchProcessingDto.builder()
                .id(entity.getId())
                .jobId(entity.getJobId())
                .batchId(entity.getBatchId())
                .startId(entity.getStartId())
                .endId(entity.getEndId())
                .totalRecords(entity.getTotalRecords())
                .processedRecords(entity.getProcessedRecords())
                .failedRecords(entity.getFailedRecords())
                .status(entity.getStatus())
                .retryCount(entity.getRetryCount())
                .workerNode(entity.getWorkerNode())
                .createdAt(entity.getCreatedAt())
                .startedAt(entity.getStartedAt())
                .completedAt(entity.getCompletedAt())
                .paginationSize(entity.getPaginationSize())
                .executorPoolSize(entity.getExecutorPoolSize())
                .build();
    }

}
