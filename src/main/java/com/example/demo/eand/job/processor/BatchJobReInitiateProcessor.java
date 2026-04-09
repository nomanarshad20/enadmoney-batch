package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.BatchProcessingRequestDTO;
import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.enums.BatchJobStatusEnum;
import com.example.demo.eand.publisher.BatchJobPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public abstract class BatchJobReInitiateProcessor {

    private final BatchJobPublisherService batchJobPublisherService;

    /**
     * Re-initiates the failed job batching process for the given job.Consumer side trigger point
     * @param jobDto
     */
    public void reInitiateFailedJobBatchingProcess(JobBatchProcessingDto jobDto){
        log.info("Re-initiated old job batching process");
        batchJobPublisherService.reInitiateOldJobBatchingProcess(BatchProcessingRequestDTO.builder()
                .jobId(jobDto.getJobId())
                .status(BatchJobStatusEnum.FAILED)
                .build());
    }

}
