package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;
import com.example.demo.eand.enums.JobTypeEnum;

public interface BatchJobProcessor {

    JobTypeEnum getJobType();

    void processBatchJob(JobBatchProcessingDto jobDto);
}
