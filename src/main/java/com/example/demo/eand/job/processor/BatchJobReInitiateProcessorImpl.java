package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.entity.BatchJobProcessEntity;
import com.example.demo.eand.enums.BatchJobStatusEnum;
import com.example.demo.eand.publisher.BatchJobPublisherService;
import com.example.demo.eand.repo.BatchJobProcessEntityRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchJobReInitiateProcessorImpl implements BatchJobReInitiateProcessor {

    private final BatchJobPublisherService batchJobPublisherService;
    private final BatchJobProcessEntityRepo batchJobProcessEntityRepo;

    /**
     * Re-initiates the failed job batching process for the given job.Consumer side trigger point
     * Call publisher service to re-initiate the generic job batching process
     *
     * @param jobDto
     */
    @Override
    public void reInitiateFailedJobBatchingProcess(JobBatchProcessingDto jobDto) {
        try {
            log.info("BATCH JOBS : ReInitiate : Checking if any job batching process for jobId={} has any retry limit reach.", jobDto.getJobId());

            // STEP 1 : Update the batch status which has reached the retry limit
            updateBatchStatusWhichHasRetryLimitReached(jobDto);

            // STEP 2 : Re-initiate the failed job batching process based on the jobId
            batchJobPublisherService.reInitiateOldJobBatchingProcess(BatchProcessingRequestDTO.builder()
                    .jobId(jobDto.getJobId())
                    .status(BatchJobStatusEnum.FAILED)
                    .build());
            log.info("BATCH JOBS : ReInitiate : Job batching process for jobId={} is re-initiated Consumer side successfully.", jobDto.getJobId());


        } catch (Exception e) {
            log.error("BATCH JOB : Publisher - Error in reInitiateOldJobBatchingProcess: {}", e.getMessage());
        }
    }

    private void updateBatchStatusWhichHasRetryLimitReached(JobBatchProcessingDto jobDto) {
        List<BatchJobProcessEntity> failedBatches = batchJobProcessEntityRepo.findByJobIdAndStatus(jobDto.getJobId(), BatchJobStatusEnum.FAILED.name());
        failedBatches.forEach(failed -> {
            long retryCount = batchJobProcessEntityRepo.countByJobIdAndBatchIdAndJobTypeAndStatusAndActiveIndIn(failed.getJobId(), failed.getBatchId(), failed.getJobType(), BatchJobStatusEnum.FAILED.name(), List.of(true, false));

            if (retryCount >= failed.getRetryCount()) {
                failed.setStatus(BatchJobStatusEnum.FAILED_RETRY_LIMIT_REACHED.name());
                batchJobProcessEntityRepo.save(failed);
                log.info("BATCH JOBS : ReInitiate : Job batching process for jobId={} batchId={} jobType={} jobRetry={} is marked as FAILED_RETRY_LIMIT_REACHED", failed.getJobId(), failed.getBatchId(), failed.getJobType(), failed.getRetryCount());
            }
        });
    }

}
