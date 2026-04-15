package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.BatchJobProcessEntity;
import com.example.demo.eand.enums.BatchJobStatusEnum;
import com.example.demo.eand.repo.BatchJobProcessEntityRepo;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.Tracer.SpanInScope;
import org.springframework.cloud.sleuth.instrument.async.TraceableExecutorService;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class AbstractBatchJobConsumerProcessor<T> implements BatchJobProcessor {

    private final BatchJobProcessEntityRepo batchJobProcessEntityRepo;
    private final BeanFactory beanFactory;
    private final Tracer tracer;

    protected AbstractBatchJobConsumerProcessor(
            BatchJobProcessEntityRepo batchJobProccessEntityRepo,
            BeanFactory beanFactory,
            Tracer tracer) {
        this.batchJobProcessEntityRepo = batchJobProccessEntityRepo;
        this.beanFactory = beanFactory;
        this.tracer = tracer;
    }

    // ------------------------------------------------------------
    // Methods to be implemented by child classes
    // ------------------------------------------------------------

    protected abstract List<T> getQueryPaginatedResponse(long startingId, long endId, long pageSize ,int pageNumber);

    protected abstract void processRecords(List<T> records, AtomicInteger processedCount, AtomicInteger failedCount,
            String jobId, Long batchId);

    protected abstract long getLastId(List<T> records);

    // ------------------------------------------------------------
    // Generic implementation
    // ------------------------------------------------------------

    @Override
    public void processBatchJob(JobBatchProcessingDto jobBatchProcessingDto) {

        Span batchSpan = tracer.nextSpan().name("Batch-Consumer").start();

        try (SpanInScope ws = tracer.withSpan(batchSpan)) {
            MDC.put("traceId", traceId());
            MDC.put("spanId", spanId());

            log.info(
                    "BATCH JOB | Consumer | INITIATION | Processing batch job | jobId={} | batchId={} | traceId={} | spanId={}",
                    jobBatchProcessingDto.getJobId(), jobBatchProcessingDto.getBatchId(), traceId(), spanId());

            BatchJobProcessEntity batch = markRunningBatchEntity(jobBatchProcessingDto);
            final JobBatchProcessingDto jobDto = jobBatchProcessingEntityToDTO(batch);

            logStart(jobDto);

            AtomicInteger processedCount = new AtomicInteger(0);
            AtomicInteger failedCount = new AtomicInteger(0);
            AtomicInteger totalRecordsCount = new AtomicInteger(0);

            try {
                processWithPagination(jobDto, batch, processedCount, failedCount, totalRecordsCount);

                batch.setTotalRecords(totalRecordsCount.get());
                batch.setProcessedRecords(processedCount.get());
                batch.setFailedRecords(failedCount.get());
                batch.setStatus(BatchJobStatusEnum.COMPLETED.name());
                batch.setCompletedAt(LocalDateTime.now());
                batchJobProcessEntityRepo.save(batch);

                log.info(
                        "BATCH JOB | Consumer | COMPLETED | Batch completed successfully | jobId={} | batchId={} | totalRecords={} | processedRecords={} | failedRecords={} | traceId={} | spanId={}",
                        jobDto.getJobId(), jobDto.getBatchId(), totalRecordsCount.get(), processedCount.get(),
                        failedCount.get(), traceId(), spanId());

            } catch (Exception ex) {
                log.error(
                        "BATCH JOB | Consumer | FAILED | Batch failed | jobId={} | batchId={} | traceId={} | spanId={}",
                        jobDto.getJobId(), jobDto.getBatchId(), traceId(), spanId(), ex);

                batch.setProcessedRecords(processedCount.get());
                batch.setFailedRecords(failedCount.get());
                batch.setStatus(BatchJobStatusEnum.FAILED.name());
                batch.setCompletedAt(LocalDateTime.now());
                batchJobProcessEntityRepo.save(batch);

                throw new RuntimeException("BATCH JOB | Consumer | Batch processing failed", ex);
            }

            logEnd(jobDto);

        } finally {
            MDC.remove("traceId");
            MDC.remove("spanId");
            batchSpan.end();
        }
    }

    // ------------------------------------------------------------
    // Supporting methods
    // ------------------------------------------------------------

    private void processWithPagination(JobBatchProcessingDto dto, BatchJobProcessEntity batch, AtomicInteger processedCount, AtomicInteger failedCount, AtomicInteger totalRecordsCount) {

        ExecutorService delegate = Executors.newFixedThreadPool(batch.getExecutorPoolSize());
        ExecutorService executorService = new TraceableExecutorService(beanFactory, delegate);

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            long startingId = dto.getStartId();
            long endId = dto.getEndId();
            Integer pageSize = dto.getPaginationSize();
            
            int currentPage = 1;

            while (true) {
                log.info("BATCH JOB | Consumer | PREPARING | Batch preparing | jobType={} | jobId={} | batchId={} | startId={} | endId={} | page={} | traceId={} | spanId={}",
                        dto.getJobType(), dto.getJobId(), dto.getBatchId(), startingId, dto.getEndId(), currentPage, traceId(), spanId());

                List<T> records = getQueryPaginatedResponse(startingId, dto.getEndId(), dto.getPaginationSize() , currentPage);

                if (records == null || records.isEmpty()) {
                    log.info("BATCH JOB | Consumer | NO_RECORDS_FOUND | No records found in range | startId={} | endId={} | traceId={} | spanId={}",
                            startingId, dto.getEndId(), traceId(), spanId());
                    break;
                }

                totalRecordsCount.addAndGet(records.size());
                long lastId = getLastId(records);
                int pageNo = currentPage++;

                long finalStartingId = startingId;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {

                    Span pageSpan = tracer.nextSpan().name("batch-page-" + pageNo).start();

                    try (SpanInScope ws = tracer.withSpan(pageSpan)) {
                            MDC.put("traceId", traceId());
                            MDC.put("spanId", spanId());
                        log.info("BATCH JOB | Consumer | PROCESSING_PAGE | Processing page | jobType={} | jobId={} | batchId={} | pageNo={} | pageSize={} | startId={} | endId={} | traceId={} | spanId={}",
                                dto.getJobType(), dto.getJobId(), dto.getBatchId(), pageNo, records.size(), finalStartingId, lastId, traceId(), spanId());

                        // any logs inside child implementation will automatically
                        // use this current page span context
                        processRecords(records, processedCount, failedCount, dto.getJobId(), dto.getBatchId());

                        log.info("BATCH JOB | Consumer | PAGE_COMPLETED | Page completed | jobType={} | jobId={} | batchId={} | pageNo={} | processedRecords={} | failedRecords={} | traceId={} | spanId={}",
                                dto.getJobType(), dto.getJobId(), dto.getBatchId(), pageNo, processedCount.get(), failedCount.get(), traceId(), spanId());

                    } catch (Exception ex) {
                        log.error("BATCH JOB | Consumer | PAGE_FAILED | Page processing failed | jobType={} | jobId={} | batchId={} | pageNo={} | traceId={} | spanId={}",
                                dto.getJobType(), dto.getJobId(), dto.getBatchId(), pageNo, traceId(), spanId(), ex);

                        throw new RuntimeException(ex);
                    } finally {
                        MDC.remove("traceId");
                        MDC.remove("spanId");
                        pageSpan.end();
                    }

                }, executorService);

                futures.add(future);

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

    protected BatchJobProcessEntity markRunningBatchEntity(JobBatchProcessingDto dto) {
        log.info(
                "BATCH JOB | Consumer | MARK_RUNNING_STARTED | Mark running started | jobId={} | batchId={} | traceId={} | spanId={}",
                dto.getJobId(), dto.getBatchId(), traceId(), spanId());

        BatchJobProcessEntity batch = batchJobProcessEntityRepo.findByJobIdAndBatchIdAndJobType(
                dto.getJobId(),
                dto.getBatchId(),
                dto.getJobType().name());

        if (batch == null) {
            throw new RuntimeException("BATCH JOB | Consumer | Batch not found | jobId=" + dto.getJobId() + " | batchId=" + dto.getBatchId()  + " | jobType=" + dto.getJobType());
        }

        batch.setWorkerNode(workerNodeName());
        batch.setStatus(BatchJobStatusEnum.PROCESSING.name());
        batch.setStartedAt(LocalDateTime.now());

        BatchJobProcessEntity saved = batchJobProcessEntityRepo.save(batch);

        log.info("BATCH JOB | Consumer | MARKED PROCESSING | Mark running completed | jobId={} | batchId={} | status={} | traceId={} | spanId={}",
                saved.getJobId(), saved.getBatchId(), saved.getStatus(), traceId(), spanId());
        return saved;
    }

    protected String workerNodeName() {
        return "Worker_localhost_node";
    }

    protected void logStart(JobBatchProcessingDto jobDto) {
        log.info(
                "BATCH JOB | Consumer | STARTING | jobType={} | jobId={} | batchId={} | traceId={} | spanId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId(), traceId(), spanId());
    }

    protected void logEnd(JobBatchProcessingDto jobDto) {
        log.info(
                "BATCH JOB | Consumer | COMPLETED | jobType={} | jobId={} | batchId={} | traceId={} | spanId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId(), traceId(), spanId());
    }

    private String traceId() {
        return tracer.currentSpan() != null ? tracer.currentSpan().context().traceId() : "N/A";
    }

    private String spanId() {
        return tracer.currentSpan() != null ? tracer.currentSpan().context().spanId() : "N/A";
    }

    private JobBatchProcessingDto jobBatchProcessingEntityToDTO(BatchJobProcessEntity entity) {
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
                .parentWorkerNode(entity.getParentWorkerNode())
                .createdAt(entity.getCreatedAt())
                .startedAt(entity.getStartedAt())
                .completedAt(entity.getCompletedAt())
                .paginationSize(entity.getPaginationSize())
                .executorPoolSize(entity.getExecutorPoolSize())
                .batchChunkSize(entity.getBatchChunkSize())
                .build();
    }
}