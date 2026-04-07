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

@Slf4j
@Component
@AllArgsConstructor
public class InactiveUserBatchProcessor implements BatchJobProcessor {

    private final UserServiceImpl userServiceImpl;
    private final JobProcessingRepo jobProcessingRepo;
    private final UserEntityRepo userRepository;

    //------------------------------------------------------------------------------


    @Override
    public JobTypeEnum getJobType() {
        return JobTypeEnum.INACTIVE_USER;
    }


    @Override
    public void processBatchJob(JobBatchProcessingDto jobDto) {
        logStart(jobDto);
        JobBatchProcessingEntity batch = markRunningBatchEntity(jobDto);
        initiate(jobDto, batch);
        logEnd(jobDto);
    }


    List<UserEntity> getQueryPaginatedResponse(long startingId, long endId, long pageSize) {
        return userRepository.findByIdBetweenOrderByIdAsc(startingId, endId);
    }
    //---------------------------------------------------------------------


    private JobBatchProcessingEntity markRunningBatchEntity(JobBatchProcessingDto dto) {
        JobBatchProcessingEntity batch = jobProcessingRepo.findByJobIdAndBatchIdAndJobType(dto.getJobId(), dto.getBatchId(), dto.getJobType().name());
        if (batch == null) {
            throw new RuntimeException("Batch not found for jobId=" + dto.getJobId() + ", batchId=" + dto.getBatchId() + ", jobType=" + dto.getJobType());
        }
        batch.setStatus(BatchStatusEnum.PROCESSING.name());
        batch.setStartedAt(LocalDateTime.now());
        long totalRecords = userRepository.countByIdBetween(dto.getStartId(), dto.getEndId()); // TODO : Query for total records or use
        batch.setTotalRecords((int) totalRecords);
        return jobProcessingRepo.save(batch);
    }


    void initiate(JobBatchProcessingDto dto, JobBatchProcessingEntity batch) {
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        try {
            processInactiveUsersWithPagination(batch, dto , processedCount  ,failedCount );

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.COMPLETED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);

            log.info("Batch completed successfully");

        } catch (Exception ex) {
            log.error("Batch failed. jobId={}, batchId={}", dto.getJobId(), dto.getBatchId(), ex);

            batch.setProcessedRecords(processedCount.get());
            batch.setFailedRecords(failedCount.get());
            batch.setStatus(BatchStatusEnum.FAILED.name());
            batch.setCompletedAt(LocalDateTime.now());
            jobProcessingRepo.save(batch);
            throw new RuntimeException("Batch processing failed", ex);
        }
    }


    private void processInactiveUsersWithPagination(JobBatchProcessingEntity batch, JobBatchProcessingDto dto , AtomicInteger processedCount,  AtomicInteger failedCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(batch.getExecutorPoolSize());
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            int currentPage = 1;
            long startingId = dto.getStartId();

            while (true) {
                List<UserEntity> list = getQueryPaginatedResponse(startingId, dto.getEndId(), dto.getPaginationSize());
                if (null == list || list.isEmpty()) {
                    log.info("No Records found in range startId={}, endId={}", dto.getStartId(), dto.getEndId());
                    break;
                }
                startingId = list.get(list.size() - 1).getId();
                int finalCurrentPage = currentPage++;
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    userServiceImpl.inactiveUserMark(list, processedCount, failedCount);
                }, executorService).exceptionally(ex -> {
                    log.error("Page processing failed. pageNumber={}", finalCurrentPage, ex);
                    return null;
                });
                futures.add(future);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            executorService.shutdown();
        }
    }


    protected void logStart(JobBatchProcessingDto jobDto) {
        log.info("Starting jobType={}, jobId={}, batchId={}", jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }

    protected void logEnd(JobBatchProcessingDto jobDto) {
        log.info("Completed jobType={}, jobId={}, batchId={}", jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }


}