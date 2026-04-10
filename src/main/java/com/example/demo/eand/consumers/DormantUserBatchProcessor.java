package com.example.demo.eand.consumers;


import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.enums.BatchJobTypeEnum;
import com.example.demo.eand.job.processor.BatchJobProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DormantUserBatchProcessor implements BatchJobProcessor {

    @Override
    public BatchJobTypeEnum getJobType() {
        return BatchJobTypeEnum.DORMANT_USER;
    }

    @Override
    public void processBatchJob(JobBatchProcessingDto jobDto) {
        logStart(jobDto);
        log.info("DormantUserBatchProcessor Executed : DONE");
        logEnd(jobDto);
    }


    protected void logStart(JobBatchProcessingDto jobDto) {
        log.info("Starting jobType={}, jobId={}, batchId={}", jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }

    protected void logEnd(JobBatchProcessingDto jobDto) {
        log.info("Completed jobType={}, jobId={}, batchId={}", jobDto.getJobType(), jobDto.getJobId(), jobDto.getBatchId());
    }


}
