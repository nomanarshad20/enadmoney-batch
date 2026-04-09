package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.enums.BatchJobTypeEnum;

public interface BatchJobProcessor {

    BatchJobTypeEnum getJobType();

    void processBatchJob(JobBatchProcessingDto jobDto);

    void reInitiateFailedJobBatchingProcess(JobBatchProcessingDto jobDto);
}
