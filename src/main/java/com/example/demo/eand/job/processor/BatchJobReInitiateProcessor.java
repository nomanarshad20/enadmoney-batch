package com.example.demo.eand.job.processor;

import com.example.demo.eand.dto.JobBatchProcessingDto;

public interface BatchJobReInitiateProcessor {

    void reInitiateFailedJobBatchingProcess(JobBatchProcessingDto jobDto);
}
