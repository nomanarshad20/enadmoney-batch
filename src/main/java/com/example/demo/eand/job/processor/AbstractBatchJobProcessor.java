package com.example.demo.eand.job.processor;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.JobBatchProcessingEntity;
import com.example.demo.eand.entity.UserEntity;
import com.example.demo.eand.enums.BatchStatusEnum;
import com.example.demo.eand.enums.JobTypeEnum;
import com.example.demo.eand.repo.JobProcessingRepo;
import com.example.demo.eand.repo.UserEntityRepo;
import com.example.demo.eand.service.JobProcessingTemplateService;
import com.example.demo.eand.service.UserServiceImpl;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import com.example.demo.eand.repo.JobProcessingRepo;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public abstract class AbstractBatchJobProcessor<T> implements BatchJobProcessor {


    private final JobProcessingRepo jobProcessingRepo;



    //------------------------------------------------------------------------------------------------
    //------------------------------------------------------------------------------------------------
    //-------------------------- Methods to override by child classes --------------------------
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


    @Override
    public void processBatchJob(JobBatchProcessingDto jobDto) {
        logStart(jobDto);
        JobBatchProcessingEntity batch = markRunningBatchEntity(jobDto);

        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        try {
            processWithPagination(jobDto, batch, processedCount, failedCount);

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.COMPLETED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);

            log.info("Batch completed successfully. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId());

        } catch (Exception ex) {
            log.error("Batch failed. jobId={}, batchId={}", jobDto.getJobId(), jobDto.getBatchId(), ex);

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.FAILED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);

            throw new RuntimeException("Batch processing failed", ex);
        }

        logEnd(jobDto);
    }

    private void processWithPagination(JobBatchProcessingDto dto, JobBatchProcessingEntity batch, AtomicInteger processedCount, AtomicInteger failedCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(batch.getExecutorPoolSize());

        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            long startingId = dto.getStartId();
            int currentPage = 1;

            while (true) {
                List<T> records = getQueryPaginatedResponse(startingId, dto.getEndId(), dto.getPaginationSize());

                if (records == null || records.isEmpty()) {
                    log.info("No records found in range startId={}, endId={}", dto.getStartId(), dto.getEndId());
                    break;
                }

                long lastId = getLastId(records);
                int pageNo = currentPage++;

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    processRecords(records, processedCount, failedCount);
                }, executorService).exceptionally(ex -> {
                    log.error("Page processing failed. pageNumber={}", pageNo, ex);
                    failedCount.addAndGet(records.size());
                    return null;
                });

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

    protected JobBatchProcessingEntity markRunningBatchEntity(JobBatchProcessingDto dto) {
        JobBatchProcessingEntity batch = jobProcessingRepo.findByJobIdAndBatchIdAndJobType(
                dto.getJobId(),
                dto.getBatchId(),
                dto.getJobType().name()
        );

        if (batch == null) {
            throw new RuntimeException(
                    "Batch not found for jobId=" + dto.getJobId()
                            + ", batchId=" + dto.getBatchId()
                            + ", jobType=" + dto.getJobType()
            );
        }

        batch.setStatus(BatchStatusEnum.PROCESSING.name());
        batch.setStartedAt(LocalDateTime.now());
        batch.setTotalRecords(countRecords(dto));
        return jobProcessingRepo.save(batch);
    }

    protected void logStart(JobBatchProcessingDto jobDto) {
        log.info("Starting jobType={}, jobId={}, batchId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }

    protected void logEnd(JobBatchProcessingDto jobDto) {
        log.info("Completed jobType={}, jobId={}, batchId={}",
                jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }



}
