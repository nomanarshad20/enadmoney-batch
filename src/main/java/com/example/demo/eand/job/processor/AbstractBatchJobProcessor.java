package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    protected abstract int countRecords(JobBatchProcessingDto dto);


    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Generic implementation for job processing --------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------


    /**
     *  Job batch process only requires 3 parameters: jobId, batchId, job type to function properly.
     * @param jobBatchProcessingDto
     */
    @Override
    public void processBatchJob(JobBatchProcessingDto jobBatchProcessingDto) {
        log.info("BATCH JOBS : INITIATION : Processing batch job. jobId={}, batchId={}", jobBatchProcessingDto.getJobId(), jobBatchProcessingDto.getBatchId());

        JobBatchProcessingEntity batch = markRunningBatchEntity(jobBatchProcessingDto);
        final JobBatchProcessingDto jobDto = jobBatchProcessingEntityToDTO(batch);

        logStart(jobDto);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        try {
            processWithPagination(jobDto, batch, processedCount, failedCount);

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.COMPLETED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);

            log.info("BATCH JOBS : Batch completed successfully. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId());

        } catch (Exception ex) {
            log.error("BATCH JOBS : Batch failed. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId(), ex);

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.FAILED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);

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


    private void processWithPagination(JobBatchProcessingDto dto, JobBatchProcessingEntity batch, AtomicInteger processedCount, AtomicInteger failedCount) {
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

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    log.info("BATCH JOBS : Processing page | jobType={} jobId={} batchId={} pageNo={}", dto.getJobType(), dto.getJobId(), dto.getBatchId(), pageNo);
                    processRecords(records, processedCount, failedCount);
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
        }
    }

    protected JobBatchProcessingEntity markRunningBatchEntity(JobBatchProcessingDto dto) {
        JobBatchProcessingEntity batch = jobProcessingRepo.findByJobIdAndBatchIdAndJobType(
                dto.getJobId(),
                dto.getBatchId(),
                dto.getJobType().name());

        if (batch == null) {
            throw new RuntimeException(
                    "BATCH JOBS : Batch not found for jobId=" + dto.getJobId() + ", batchId=" + dto.getBatchId() + ", jobType=" + dto.getJobType());
        }

        batch.setStatus(BatchStatusEnum.PROCESSING.name());
        batch.setStartedAt(LocalDateTime.now());
        batch.setTotalRecords(countRecords(dto));
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
                .batchChunkSize(entity.getBatchChunkSize())
                .build();
    }

}
